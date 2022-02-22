import datetime
import time
from collections import deque


from celery.utils.log import get_task_logger

from materializationengine.celery_init import celery

celery_logger = get_task_logger(__name__)


def get_queue_length(queue_name="process"):
    with celery.connection_or_acquire() as conn:
        return conn.default_channel.queue_declare(queue=queue_name).message_count


class CeleryThrottle:
    """Modified from https://stackoverflow.com/a/43429475"""

    def __init__(self, max_queue_length: int = 100, queue_name: str = "celery"):
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
        self.queue_name = queue_name

        # Variables used to track the queue and wait-rate
        self.last_processed_count = 0
        self.count_to_do = self.max_queue_length
        self.last_measurement = None
        self.first_run = True

        # Use a fixed-length queue to hold last N rates
        self.rates = deque(maxlen=15)
        self.avg_rate = self._calculate_avg()

    def _calculate_avg(self):
        return float(sum(self.rates)) / (len(self.rates) or 1)

    def _add_latest_rate(self):
        """Calculate the rate that the queue is processing items."""
        start = datetime.datetime.now()
        elapsed_seconds = (start - self.last_measurement).total_seconds()
        self.rates.append(self.last_processed_count / elapsed_seconds)
        self.last_measurement = start
        self.last_processed_count = 0
        self.avg_rate = self._calculate_avg()

    def maybe_wait(self):
        """Pause the calling function or let it proceed, depending on the
        enqueued task amount.
        """
        self.last_processed_count += 1
        if self.count_to_do > 0:
            # Do not wait. Allow process to continue.
            if self.first_run:
                self.first_run = False
                self.last_measurement = datetime.datetime.now()
            self.count_to_do -= 1
            return

        self._add_latest_rate()
        task_count = get_queue_length(self.queue_name)
        if task_count > self.min_queue_length:

            surplus_task_count = task_count - self.min_queue_length
            wait_time = (surplus_task_count / self.avg_rate) * 1.05
            time.sleep(wait_time)

            if task_count < self.max_queue_length:
                self.count_to_do = self.max_queue_length - self.min_queue_length
        else:
            # Add more items.
            self.count_to_do = self.max_queue_length - task_count
        return
