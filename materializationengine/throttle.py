import time
from typing import List

import redis
from celery.utils.log import get_task_logger

from materializationengine.celery_init import celery
from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)


def get_queue_length(queue_name: str = "celery"):
    """Get amount of tasks in specified redis queue

    Args:
        queue_name (str): Name of queue. Defaults to "celery".

    Raises:
        e: Redis error

    Returns:
        int: Amount of tasks held in a queue
    """

    try:
        r = redis.StrictRedis(
            host=get_config_param("REDIS_HOST"),
            port=get_config_param("REDIS_PORT"),
            password=get_config_param("REDIS_PASSWORD"),
            db=0,
        )

    except Exception as e:
        celery_logger.error(f"Redis connection error: {e}")
        raise e
    return r.llen(queue_name)


def get_redis_memory_usage():
    """Get redis memory usage

    Raises:
        e: Redis error

    Returns:
        int: Bytes of memory used in Redis
    """
    try:
        r = redis.StrictRedis(
            host=get_config_param("REDIS_HOST"),
            port=get_config_param("REDIS_PORT"),
            password=get_config_param("REDIS_PASSWORD"),
            db=0,
        )

    except Exception as e:
        celery_logger.error("Redis has an error: {e}")
        raise e
    return r.info()["used_memory"]


class CeleryThrottle:
    """Modified from https://stackoverflow.com/a/43429475"""

    def __init__(
        self,
        max_queue_length: int = 100,
        queues_to_throttle: List[str] = None,
        poll_interval: float = 3.0,
        memory_limit: int = 858993459,
    ):
        """Create a throttle to prevent too many tasks being sent
        to the broker. Will calculate wait time for task completion and
        sleep until the total task length is under the max_queue_length.

        Args:
            max_queue_length (int): max number of tasks to be enqueued at a time.
            queue_name (str): target queue for limiting
        TODO:
            Add additional logic to check redis memory usage and scale queue
            length.
        """
        if max_queue_length == 0:
            raise ValueError("max_queue_length must be great than 0")
        self.min_queue_length = max_queue_length // 2
        self.max_queue_length = max_queue_length
        self.poll_interval = poll_interval
        self.memory_limit = memory_limit
        self.queues_to_throttle = queues_to_throttle

    def wait_if_queue_full(self, queue_name: str):
        """Pause the calling function or let it proceed, depending on the
        enqueued task amount.

        Args:
            queue_name (str): Name of queue to check amount of enqueued tasks
        """

        if queue_name in self.queues_to_throttle:
            celery_logger.debug(f"throttle queue {queue_name}")
            while True:
                queue_length = get_queue_length(queue_name)
                if queue_length > self.max_queue_length:
                    time.sleep(self.poll_interval)
                else:
                    break

    def wait_if_memory_maxed(self):
        """Pause the calling function or let it proceed, depending on if max
        memory is not reached.
        """

        while True:
            memory_used = get_redis_memory_usage()
            if memory_used > self.memory_limit:
                time.sleep(self.poll_interval)
            else:
                self.memory_remaining = self.memory_limit - memory_used
                break

            
    def wait_if_needed(self, queue_name: str):
        """Combined throttling check for both queue length and memory usage."""
        # First check queue length
        if queue_name in self.queues_to_throttle:
            queue_length = get_queue_length(queue_name)
            if queue_length > self.max_queue_length:
                celery_logger.info(f"Queue {queue_name} full ({queue_length} tasks), throttling")
                time.sleep(self.poll_interval)
                return True  # Throttling was needed
                
        # Then check memory usage
        memory_used = get_redis_memory_usage()
        memory_percent = memory_used / self.memory_limit * 100
        
        if memory_used > self.memory_threshold:
            celery_logger.info(
                f"Redis memory usage high: {memory_used/1024/1024:.1f}MB "
                f"({memory_percent:.1f}% of limit), throttling"
            )
            time.sleep(self.poll_interval)
            return True  # Throttling was needed
            
        # No throttling needed
        self.memory_remaining = self.memory_limit - memory_used
        return False

throttle_celery = CeleryThrottle(
    max_queue_length=int(get_config_param("QUEUE_LENGTH_LIMIT")),
    queues_to_throttle=get_config_param("QUEUES_TO_THROTTLE"),
)
