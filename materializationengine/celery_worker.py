import logging
import os
import sys
import redis

from celery.app.builtins import add_backend_cleanup_task
from celery.schedules import crontab
from celery.signals import after_setup_logger
from celery.utils.log import get_task_logger

from materializationengine.celery_init import celery
from materializationengine.celery_slack import post_to_slack_on_task_failure
from materializationengine.errors import TaskNotFound
from materializationengine.schemas import CeleryBeatSchema
from materializationengine.utils import get_config_param
from dateutil import relativedelta
import datetime

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
    from materializationengine.workflows.periodic_database_removal import (
        remove_expired_databases,
    )
    from materializationengine.workflows.periodic_materialization import (
        run_periodic_materialization,
    )
    from materializationengine.workflows.update_database_workflow import (
        run_periodic_database_update,
    )

    task_map = {
        "run_periodic_materialization": run_periodic_materialization,
        "run_periodic_database_update": run_periodic_database_update,
        "remove_expired_databases": remove_expired_databases,
    }

    # remove expired task results in redis broker
    sender.add_periodic_task(
        crontab(hour=0, minute=0, day_of_week="*", day_of_month="*", month_of_year="*"),
        add_backend_cleanup_task(celery),
        name="Clean up back end results",
    )

    beat_schedules = celery.conf["beat_schedules"]
    celery_logger.info(beat_schedules)
    schedules = CeleryBeatSchema(many=True).dump(beat_schedules)
    for schedule in schedules:

        task_name = schedule["task"]

        if task_name not in task_map:
            raise TaskNotFound(task_name, task_map)

        task_function = task_map[task_name]
        datastack_params = schedule.get("datastack_params", {})

        if task_name == "remove_expired_databases":
            task = task_function.s(
                delete_threshold=datastack_params.get("delete_threshold", 5),
                datastack=datastack_params.get("datastack"),
            )
        elif task_name == "run_periodic_database_update":
            task = task_function.s(
                datastack=datastack_params.get("datastack"),
            )

        elif task_name == "run_periodic_materialization":
            # If `days_to_expire` is not provided, calculate it dynamically #TODO handle this better
            if datastack_params.get("days_to_expire") == 30:
                datastack_params["days_to_expire"] = days_till_next_month(
                    datetime.datetime.now(datetime.timezone.utc)
                )
            task = task_function.s(
                days_to_expire=datastack_params.get("days_to_expire"),
                merge_tables=datastack_params.get("merge_tables", False),
                datastack=datastack_params.get("datastack"),
            )

        sender.add_periodic_task(
            crontab(
                minute=schedule["minute"],
                hour=schedule["hour"],
                day_of_week=schedule["day_of_week"],
                day_of_month=schedule["day_of_month"],
                month_of_year=schedule["month_of_year"],
            ),
            task,
            name=schedule["name"],
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
