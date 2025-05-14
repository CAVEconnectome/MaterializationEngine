import datetime
import logging
import os
import sys
import warnings
from typing import Any, Callable, Dict

import redis
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
            "beat_schedules": app.config["BEAT_SCHEDULES"],
        }
    )

    celery.conf.update(app.config)
    TaskBase = celery.Task

    class ContextTask(TaskBase):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return TaskBase.__call__(self, *args, **kwargs)

    celery.Task = ContextTask
    if os.environ.get("SLACK_WEBHOOK"):
        celery.Task.on_failure = post_to_slack_on_task_failure
    return celery


@after_setup_logger.connect
def celery_loggers(logger, *args, **kwargs):
    """
    Display the Celery banner appears in the log output.
    https://www.distributedpython.com/2018/10/01/celery-docker-startup/
    """
    logger.info(f"Customize Celery logger, default handler: {logger.handlers[0]}")
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

    beat_schedules = celery.conf.get("beat_schedules", [])
    celery_logger.info(beat_schedules)
    if not beat_schedules:
        celery_logger.info("No periodic tasks configured.")
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
