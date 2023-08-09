import os

from celery.utils.log import get_task_logger
from redis import ConnectionError, redis

from materializationengine.celery_init import celery
from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)

REDIS_CLIENT = redis.StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)

REDIS_STATUS_FILE = "/tmp/redis_status.txt"


@celery.task(bind=True)
def health_check_task(self):
    return "alive"


def set_redis_status(status):
    with os.open(REDIS_STATUS_FILE, "w") as f:
        f.write(status)


def get_redis_status():
    if not os.path.exists(REDIS_STATUS_FILE):
        return None
    with os.open(REDIS_STATUS_FILE, "r") as f:
        return f.read().strip()


def is_redis_connected():
    try:
        REDIS_CLIENT.ping()
        return True
    except ConnectionError:
        return False


def monitor_redis_connection():
    current_status = get_redis_status()

    if is_redis_connected():
        if current_status == "disconnected":
            set_redis_status("reconnected")
        else:
            set_redis_status("connected")
    else:
        set_redis_status("disconnected")


def check_worker_health():
    # Only run the celery health check if Redis was disconnected/reconnected
    if get_redis_status() == "reconnected":
        result = health_check_task.delay()
        try:
            # Give the task a fixed amount of time to execute
            is_alive = result.get(timeout=60)
            if is_alive != "alive":
                exit(1)
        except Exception as e:
            celery_logger.error(e)
            exit(1)  # Non-zero status indicates a failure

        # Reset the flag if everything is OK
        set_redis_status("connected")

    elif get_redis_status() == "disconnected":
        exit(1)


def run_health_checks():
    """Run health checks for the worker and Redis"""
    monitor_redis_connection()
    check_worker_health()


if __name__ == "__main__":
    run_health_checks()

    """We want to run this script as a liveness probe for Kubernetes.
    If the script exits with a non-zero status, Kubernetes will restart the pod.

    The script will exit with a non-zero status if:
    1. Redis is disconnected
    2. The worker is not responding

    Example Kubernetes configuration:
    
    livenessProbe:
    exec:
        command:
        - python
        - monitor.py
    initialDelaySeconds: 60  
    periodSeconds: 360
    
    """
