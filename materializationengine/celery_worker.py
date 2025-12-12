import datetime
import logging
import os
import signal
import sys
import threading
import time
import warnings
from typing import Any, Callable, Dict

import redis
from celery import signals
from celery.app.builtins import add_backend_cleanup_task
from celery.schedules import crontab
from celery.signals import after_setup_logger
from celery.utils.log import get_task_logger
from dateutil import relativedelta
from marshmallow import ValidationError

from materializationengine.celery_init import celery
from materializationengine.celery_slack import post_to_slack_on_task_failure
from materializationengine.errors import ConfigurationError
from materializationengine.schemas import CeleryBeatSchema
from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)


_task_execution_count = 0
_shutdown_requested = False
_last_task_time = None
_worker_start_time = None


def _request_worker_shutdown(delay_seconds: int, observed_count: int) -> None:
    """Delay and then terminate the worker process."""
    # Delay slightly so task result propagation finishes
    time.sleep(max(delay_seconds, 0))
    celery_logger.info(
        "Auto-shutdown: terminating worker PID %s after %s tasks",
        os.getpid(),
        observed_count,
    )
    try:
        os.kill(os.getpid(), signal.SIGTERM)
    except Exception as exc:  # pragma: no cover - best-effort shutdown
        celery_logger.error("Failed to terminate worker: %s", exc)


def _auto_shutdown_handler(sender=None, **kwargs):
    """Trigger worker shutdown after configurable task count when enabled."""
    if not celery.conf.get("worker_autoshutdown_enabled", False):
        return

    max_tasks = celery.conf.get("worker_autoshutdown_max_tasks", 1)
    if max_tasks <= 0:
        return

    global _task_execution_count, _shutdown_requested, _last_task_time
    if _shutdown_requested:
        return

    _task_execution_count += 1
    _last_task_time = time.time()  # Update last task time

    if _task_execution_count < max_tasks:
        return

    _shutdown_requested = True
    delay = celery.conf.get("worker_autoshutdown_delay_seconds", 2)
    celery_logger.info(
        "Auto-shutdown triggered after %s tasks; stopping consumer and terminating in %ss",
        _task_execution_count,
        delay,
    )
    
    # Immediately stop accepting new tasks by canceling the consumer
    # This prevents the worker from picking up new tasks during the shutdown delay
    try:
        # Get queue name from config or use default
        queue_name = celery.conf.get("task_default_queue") or celery.conf.get("task_routes", {}).get("*", {}).get("queue", "celery")
        # Get worker hostname from the task sender or use current worker's hostname
        worker_hostname = None
        if hasattr(sender, 'hostname'):
            worker_hostname = sender.hostname
        elif hasattr(celery, 'control'):
            # Try to get hostname from current worker
            try:
                from celery import current_app
                inspect = current_app.control.inspect()
                active_workers = inspect.active() if inspect else {}
                if active_workers:
                    worker_hostname = list(active_workers.keys())[0]
            except Exception:
                pass
        
        if worker_hostname and queue_name:
            celery_logger.info("Canceling consumer for queue '%s' on worker '%s'", queue_name, worker_hostname)
            celery.control.cancel_consumer(queue_name, destination=[worker_hostname])
        else:
            celery_logger.warning("Could not determine worker hostname or queue name for consumer cancellation")
    except Exception as exc:
        celery_logger.warning("Failed to cancel consumer during shutdown: %s", exc)
    
    shutdown_thread = threading.Thread(
        target=_request_worker_shutdown,
        args=(delay, _task_execution_count),
        daemon=True,
    )
    shutdown_thread.start()


def _monitor_idle_timeout():
    """Monitor worker idle time and shutdown if idle timeout exceeded."""
    idle_timeout_seconds = celery.conf.get("worker_idle_timeout_seconds", 0)
    if idle_timeout_seconds <= 0:
        return  # Idle timeout not enabled
    
    check_interval = min(30, idle_timeout_seconds / 4)  # Check every 30s or 1/4 of timeout, whichever is smaller
    
    global _last_task_time, _worker_start_time, _shutdown_requested
    
    while not _shutdown_requested:
        time.sleep(check_interval)
        
        if _shutdown_requested:
            break
            
        current_time = time.time()
        
        # If we've processed at least one task, use last task time
        # Otherwise, use worker start time
        if _last_task_time is not None:
            idle_duration = current_time - _last_task_time
            reference_time = _last_task_time
        elif _worker_start_time is not None:
            idle_duration = current_time - _worker_start_time
            reference_time = _worker_start_time
        else:
            continue  # Haven't started yet
        
        if idle_duration >= idle_timeout_seconds:
            celery_logger.info(
                "Idle timeout exceeded: worker has been idle for %.1f seconds (timeout: %d seconds). Shutting down.",
                idle_duration,
                idle_timeout_seconds,
            )
            _shutdown_requested = True
            # Cancel consumer to stop accepting new tasks
            try:
                queue_name = celery.conf.get("task_default_queue") or celery.conf.get("task_routes", {}).get("*", {}).get("queue", "celery")
                from celery import current_app
                inspect = current_app.control.inspect()
                active_workers = inspect.active() if inspect else {}
                if active_workers:
                    worker_hostname = list(active_workers.keys())[0]
                    celery_logger.info("Canceling consumer for queue '%s' on worker '%s'", queue_name, worker_hostname)
                    celery.control.cancel_consumer(queue_name, destination=[worker_hostname])
            except Exception as exc:
                celery_logger.warning("Failed to cancel consumer during idle timeout shutdown: %s", exc)
            
            # Shutdown after a short delay
            delay = celery.conf.get("worker_autoshutdown_delay_seconds", 2)
            shutdown_thread = threading.Thread(
                target=_request_worker_shutdown,
                args=(delay, 0),  # 0 tasks since we're shutting down due to idle timeout
                daemon=True,
            )
            shutdown_thread.start()
            break


def _worker_ready_handler(sender=None, **kwargs):
    """Handle worker ready signal - start idle timeout monitor if enabled."""
    global _worker_start_time, _shutdown_requested
    _worker_start_time = time.time()
    
    idle_timeout_seconds = celery.conf.get("worker_idle_timeout_seconds", 0)
    if idle_timeout_seconds > 0:
        celery_logger.info(
            "Worker idle timeout enabled: %d seconds. Worker will shutdown if idle for this duration.",
            idle_timeout_seconds,
        )
        monitor_thread = threading.Thread(
            target=_monitor_idle_timeout,
            daemon=True,
        )
        monitor_thread.start()


signals.task_postrun.connect(_auto_shutdown_handler, weak=False)
signals.worker_ready.connect(_worker_ready_handler, weak=False)


def create_celery(app=None):
    celery.conf.broker_url = app.config["CELERY_BROKER_URL"]
    celery.conf.result_backend = app.config["CELERY_RESULT_BACKEND"]
    if app.config.get("USE_SENTINEL", False):
        celery.conf.broker_transport_options = {
            "master_name": app.config["MASTER_NAME"]
        }
        celery.conf.result_backend_transport_options = {
            "master_name": app.config["MASTER_NAME"]
        }

    celery.conf.worker_autoshutdown_enabled = app.config.get(
        "CELERY_WORKER_AUTOSHUTDOWN_ENABLED", False
    )
    celery.conf.worker_autoshutdown_max_tasks = app.config.get(
        "CELERY_WORKER_AUTOSHUTDOWN_MAX_TASKS", 1
    )
    celery.conf.worker_autoshutdown_delay_seconds = app.config.get(
        "CELERY_WORKER_AUTOSHUTDOWN_DELAY_SECONDS", 2
    )
    celery.conf.worker_idle_timeout_seconds = app.config.get(
        "CELERY_WORKER_IDLE_TIMEOUT_SECONDS", 0
    )

    if celery.conf.worker_autoshutdown_enabled:
        celery_logger.info(
            "Worker auto-shutdown enabled: max_tasks=%s delay=%ss",
            celery.conf.worker_autoshutdown_max_tasks,
            celery.conf.worker_autoshutdown_delay_seconds,
        )
    
    if celery.conf.worker_idle_timeout_seconds > 0:
        celery_logger.info(
            "Worker idle timeout enabled: %s seconds",
            celery.conf.worker_idle_timeout_seconds,
        )
    # Configure Celery and related loggers
    log_level = app.config["LOGGING_LEVEL"]
    celery_logger.setLevel(log_level)
    
    # Configure all Celery internal loggers to suppress noisy messages
    celery_loggers = [
        'celery',
        'celery.worker',
        'celery.worker.consumer', 
        'celery.worker.strategy',
        'celery.worker.heartbeat',
        'celery.worker.job',
        'celery.beat',
        'celery.control',
        'celery.app.trace'
    ]
    
    for logger_name in celery_loggers:
        logging.getLogger(logger_name).setLevel(log_level)
    
    # Debug: Check if BEAT_SCHEDULES is in app.config
    beat_schedules = app.config.get("BEAT_SCHEDULES", [])
    celery_logger.debug(f"BEAT_SCHEDULES from app.config: {beat_schedules}")
    celery_logger.debug(f"BEAT_SCHEDULES type: {type(beat_schedules)}, length: {len(beat_schedules) if isinstance(beat_schedules, (list, dict)) else 'N/A'}")
    
    celery.conf.update(
        {
            "task_routes": ("materializationengine.task_router.TaskRouter"),
            "task_serializer": "json",
            "result_serializer": "json",
            "accept_content": ["json", "application/json"],
            "optimization": "fair",
            "task_send_sent_event": True,
            "task_track_started": True,
            "worker_send_task_events": True,
            "worker_prefetch_multiplier": 1,
            "result_expires": 86400,  # results expire in broker after 1 day
            "redis_socket_connect_timeout": 10,
            "broker_transport_options": {
                "visibility_timeout": 8000,
                "socket_timeout": 20,
                "socket_connect_timeout": 20,
            },  # timeout (s) for tasks to be sent back to broker queue
            "beat_schedules": beat_schedules,
        }
    )

    celery.conf.update(app.config)
    # Ensure beat_schedules is set correctly after update (in case app.config overwrote it)
    # Use BEAT_SCHEDULES from app.config if beat_schedules is empty or missing
    if not celery.conf.get("beat_schedules") and app.config.get("BEAT_SCHEDULES"):
        celery.conf.beat_schedules = app.config["BEAT_SCHEDULES"]
        celery_logger.debug(f"Restored beat_schedules from BEAT_SCHEDULES: {len(app.config['BEAT_SCHEDULES'])} schedules")
    
    # Debug: Verify beat_schedules is in celery.conf after update
    celery_logger.debug(f"beat_schedules in celery.conf after update: {celery.conf.get('beat_schedules', 'NOT FOUND')}")
    celery_logger.debug(f"BEAT_SCHEDULES in celery.conf after update: {celery.conf.get('BEAT_SCHEDULES', 'NOT FOUND')}")
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    if os.environ.get("SLACK_WEBHOOK"):
        celery.Task.on_failure = post_to_slack_on_task_failure
    
    # Manually trigger setup_periodic_tasks to ensure it runs with the correct configuration
    # The signal handler may have fired before beat_schedules was set, so we call it explicitly here
    try:
        setup_periodic_tasks(celery)
        celery_logger.info("Manually triggered setup_periodic_tasks after create_celery")
    except Exception as e:
        celery_logger.warning(f"Error manually triggering setup_periodic_tasks: {e}")
        # Don't fail if this doesn't work - the signal handler should still work
    
    return celery


@after_setup_logger.connect
def celery_loggers(logger, *args, **kwargs):
    """
    Add stdout handler for Celery logger output.
    """
    logger.addHandler(logging.StreamHandler(sys.stdout))
    

def days_till_next_month(date):
    """function to pick out the same weekday in the next month
    So if you pass the first wednesday of January, you get
    the first wednesday of February

    Args:
        date (datetime.datetime): a timepoint

    Returns:
        datetime.datetime: same day next month (in the sense of same # of weekday)
    """

    weekday = relativedelta.weekday(date.isoweekday() - 1)
    weeknum = (date.day - 1) // 7 + 1
    weeknum = weeknum if weeknum <= 4 else 4
    next_date = date + relativedelta.relativedelta(
        months=1, day=1, weekday=weekday(weeknum)
    )
    delta_days = next_date - date
    return delta_days.days


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # remove expired task results in redis broker
    sender.add_periodic_task(
        crontab(hour=0, minute=0, day_of_week="*", day_of_month="*", month_of_year="*"),
        add_backend_cleanup_task(celery),
        name="Clean up back end results",
    )

    # Try to get beat_schedules from celery.conf, fallback to BEAT_SCHEDULES if not found
    beat_schedules = celery.conf.get("beat_schedules")
    if not beat_schedules:
        # Fallback: try to get from BEAT_SCHEDULES (uppercase) in celery.conf
        beat_schedules = celery.conf.get("BEAT_SCHEDULES", [])
        if beat_schedules:
            celery_logger.debug(f"Found BEAT_SCHEDULES (uppercase), converting to beat_schedules")
            celery.conf.beat_schedules = beat_schedules
    
    celery_logger.debug(f"beat_schedules from celery.conf: {beat_schedules}")
    celery_logger.debug(f"beat_schedules type: {type(beat_schedules)}, length: {len(beat_schedules) if isinstance(beat_schedules, (list, dict)) else 'N/A'}")
    
    if not beat_schedules:
        celery_logger.debug("No periodic tasks configured yet (beat_schedules empty). Will retry after create_celery.")
        return
    
    # Check if tasks from beat_schedules are already registered to avoid duplicates
    existing_schedule = sender.conf.get("beat_schedule", {})
    if existing_schedule:
        # Check if any of our scheduled tasks are already registered
        schedule_names = [s.get("name") for s in beat_schedules if isinstance(s, dict)]
        already_registered = any(name in existing_schedule for name in schedule_names if name)
        if already_registered:
            celery_logger.debug("Periodic tasks already registered in beat_schedule, skipping duplicate registration.")
            return
    try:
        schedules = CeleryBeatSchema(many=True).load(beat_schedules)
    except ValidationError as validation_error:
        celery_logger.error(f"Configuration validation failed: {validation_error}")
        raise ConfigurationError("Invalid configuration") from validation_error

    min_databases = sender.conf.get("MIN_DATABASES")
    celery_logger.info(f"MIN_DATABASES: {min_databases}")
    for schedule in schedules:
        try:
            task = configure_task(schedule, min_databases)
            sender.add_periodic_task(
                create_crontab(schedule),
                task,
                name=schedule["name"],
            )
            celery_logger.info(f"Added task: {schedule['name']}")
        except ConfigurationError as e:
            celery_logger.error(
                f"Error configuring task '{schedule.get('name', 'Unknown')}': {str(e)}"
            )


def configure_task(schedule: Dict[str, Any], min_databases: int = None) -> Callable:
    task_name = schedule["task"]
    datastack_params = schedule.get("datastack_params", {})

    if is_old_materialization_configuration(task_name):
        return schedule_legacy_workflow(task_name)
    else:
        return schedule_workflow(task_name, datastack_params, min_databases)


def is_old_materialization_configuration(task_name: str) -> bool:
    old_task_names = [
        "run_daily_periodic_materialization",
        "run_weekly_periodic_materialization",
        "run_lts_periodic_materialization",
    ]
    return task_name in old_task_names


def schedule_legacy_workflow(task_name: str) -> Callable:
    from materializationengine.workflows.periodic_materialization import (
        run_periodic_materialization,
    )

    warnings.warn(
        f"Deprecated task name '{task_name}' detected. Please update your configuration to use 'run_periodic_materialization' instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    if task_name == "run_daily_periodic_materialization":
        days_to_expire = 2
    elif task_name == "run_weekly_periodic_materialization":
        days_to_expire = 7
    elif task_name == "run_lts_periodic_materialization":
        days_to_expire = days_till_next_month(
            datetime.datetime.now(datetime.timezone.utc)
        )
    else:
        raise ConfigurationError(f"Unknown old task name: {task_name}")

    return run_periodic_materialization.s(
        days_to_expire=days_to_expire,
        merge_tables=False,  # Default value for old configuration
    )


def schedule_workflow(
    task_name: str,
    datastack_params: Dict[str, Any],
    min_databases: int = None,
) -> Callable:
    from materializationengine.workflows.periodic_database_removal import (
        remove_expired_databases,
    )
    from materializationengine.workflows.periodic_materialization import (
        run_periodic_materialization,
    )
    from materializationengine.workflows.update_database_workflow import (
        run_periodic_database_update,
    )

    if task_name == "remove_expired_databases":
        default_threshold_for_task = 5
        if min_databases is not None:
            default_threshold_for_task = min_databases

        delete_threshold = datastack_params.get(
            "delete_threshold", default_threshold_for_task
        )

        return remove_expired_databases.s(
            delete_threshold=delete_threshold,
            datastack=datastack_params.get("datastack"),
        )

    elif task_name == "run_periodic_database_update":
        return run_periodic_database_update.s(
            datastack=datastack_params.get("datastack")
        )

    elif task_name == "run_periodic_materialization":
        return run_periodic_materialization.s(
            days_to_expire=datastack_params.get("days_to_expire", 2),
            merge_tables=datastack_params.get("merge_tables", False),
            datastack=datastack_params.get("datastack"),
        )

    else:
        raise ConfigurationError(f"Unknown task: {task_name}")


def create_crontab(schedule: Dict[str, Any]) -> crontab:
    """Create a crontab object from the schedule dictionary."""
    return crontab(
        minute=schedule.get("minute", "*"),
        hour=schedule.get("hour", "*"),
        day_of_week=schedule.get("day_of_week", "*"),
        day_of_month=schedule.get("day_of_month", "*"),
        month_of_year=schedule.get("month_of_year", "*"),
    )


def get_celery_worker_status():
    i = celery.control.inspect()
    availability = i.ping()
    stats = i.stats()
    registered_tasks = i.registered()
    active_tasks = i.active()
    scheduled_tasks = i.scheduled()
    result = {
        "availability": availability,
        "stats": stats,
        "registered_tasks": registered_tasks,
        "active_tasks": active_tasks,
        "scheduled_tasks": scheduled_tasks,
    }
    return result


def get_celery_queue_items(queue_name: str):
    with celery.connection_or_acquire() as conn:
        return conn.default_channel.queue_declare(
            queue=queue_name, passive=True
        ).message_count


def get_activate_tasks():
    inspector = celery.control.inspect()
    return inspector.active()


def inspect_locked_tasks(release_locks: bool = False):
    client = redis.StrictRedis(
        host=get_config_param("REDIS_HOST"),
        port=get_config_param("REDIS_PORT"),
        password=get_config_param("REDIS_PASSWORD"),
        db=0,
    )

    locked_tasks = list(client.scan_iter(match="LOCKED_WORKFLOW_TASK*"))
    lock_status_dict = {locked_task: {"locked": True} for locked_task in locked_tasks}

    if release_locks:
        for locked_task in lock_status_dict:
            client.delete(locked_task)
            lock_status_dict[locked_task] = {"locked": False}
    return lock_status_dict
