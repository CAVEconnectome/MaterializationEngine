import logging
import sys

from celery.app.builtins import add_backend_cleanup_task
from celery.schedules import crontab
from celery.signals import after_setup_logger
from celery.utils.log import get_task_logger

from materializationengine.celery_init import celery
from materializationengine.errors import TaskNotFound
from materializationengine.schemas import CeleryBeatSchema

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
            "task_serializer": "pickle",
            "result_serializer": "pickle",
            "accept_content": ["pickle"],
            "optimization": "fair",
            "task_send_sent_event": True,
            "worker_send_task_events": True,
            "worker_prefetch_multiplier": 1,
            "result_expires": 86400,  # results expire in broker after 1 day
            "visibility_timeout": 8000 # timeout (s) for tasks to be sent back to broker queue
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
    return celery


@after_setup_logger.connect
def celery_loggers(logger, *args, **kwargs):
    """
    Display the Celery banner appears in the log output.
    https://www.distributedpython.com/2018/10/01/celery-docker-startup/
    """
    logger.info(f"Customize Celery logger, default handler: {logger.handlers[0]}")
    logger.addHandler(logging.StreamHandler(sys.stdout))


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    from materializationengine.workflows.periodic_database_removal import \
        remove_expired_databases
    from materializationengine.workflows.periodic_materialization import \
        run_periodic_materialization

    periodic_tasks = {
        "run_daily_periodic_materialization": run_periodic_materialization.s(
            days_to_expire=2
        ),
        "run_weekly_periodic_materialization": run_periodic_materialization.s(
            days_to_expire=7
        ),
        "run_lts_periodic_materialization": run_periodic_materialization.s(
            days_to_expire=30
        ),
        "remove_expired_databases": remove_expired_databases.s(delete_threshold=5),
    }

    # remove expired task results in redis broker
    sender.add_periodic_task(
        crontab(hour=0, minute=0, day_of_week="*", day_of_month="*", month_of_year="*"),
        add_backend_cleanup_task(celery),
        name="Clean up back end results",
    )

    beat_schedules = celery.conf["BEAT_SCHEDULES"]
    celery_logger.info(beat_schedules)
    schedules = CeleryBeatSchema(many=True).dump(beat_schedules)
    for schedule in schedules:

        if schedule["task"] not in periodic_tasks:
            raise TaskNotFound(schedule["task"], periodic_tasks)

        task = periodic_tasks[schedule["task"]]
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
        'availability': availability,
        'stats': stats,
        'registered_tasks': registered_tasks,
        'active_tasks': active_tasks,
        'scheduled_tasks': scheduled_tasks
    }
    return result


def get_celery_queue_items(queue_name: str):
    with celery.connection_or_acquire() as conn:
        return conn.default_channel.queue_declare(
            queue=queue_name, passive=True).message_count
    