import json
from hashlib import md5

import redis
from celery import Task
from celery.utils.log import get_task_logger
from kombu.utils.uuid import uuid

from materializationengine.celery_slack import post_to_slack_on_task_failure
from materializationengine.utils import get_config_param

REDIS_CLIENT = redis.StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    db=0,
)

celery_logger = get_task_logger(__name__)


def argument_signature(
    task_name: str, task_args=None, task_kwargs=None, key_prefix="LOCKEDTASK_"
):
    str_args = json.dumps(task_args or [], sort_keys=True)
    str_kwargs = json.dumps(task_kwargs or {}, sort_keys=True)
    task_hash = md5((task_name + str_args + str_kwargs).encode()).hexdigest()
    return key_prefix + task_hash


class LockedTask(Task):
    """A custom celery task that prevents tasks of the
    same type from running simultaneously. If an attempt to run a
    task of the same type is run it will be ignored until the currently
    running task is unlocked in redis.

    Inspired by https://github.com/steinitzu/celery-singleton

    Attributes
    ----------
    timeout: int
        Timeout for how long a task is locked in seconds.
    use_slack: bool (default False)
        Post an error message to slack if slack webhook is
        set as an env param.
    raise_on_duplicate: bool (default True)
        Raises a key error if the same task type is run while
        a current one is locked. If set to False, the running
        task's AsyncResult will be returned.

    Examples
    --------
    >>> @celery.task(bind=True, base=LockedTask, timeout=60*60, use_slack=True)
    ... def a_task_that_only_can_run_once(self):
    ...     do_stuff()
    ...
    ... a = a_task_that_only_can_run_once.s().apply_async()
    ... # runs ok
    ... b = a_task_that_only_can_run_once.s().apply_async()
    ... # raises duplicate task error, task is not queued

    """

    abstract = True
    timeout = None
    use_slack = False
    raise_on_duplicate = True

    def acquire_lock(self, lock_id: str, task_id: str) -> bool:
        return REDIS_CLIENT.set(lock_id, task_id, nx=True, ex=self.timeout)

    def get_existing_task_id(self, lock_id: str) -> bytes:
        return REDIS_CLIENT.get(lock_id)

    def unlock(self, lock_id: str) -> int:
        REDIS_CLIENT.delete(lock_id)

    def apply_async(
        self,
        args=None,
        kwargs=None,
        task_id=None,
        producer=None,
        link=None,
        link_error=None,
        shadow=None,
        **options,
    ):
        args = args or []
        kwargs = kwargs or {}
        task_id = task_id or uuid()
        lock_id = argument_signature(self.name, args, kwargs)

        run_args = dict(
            lock_id=lock_id,
            args=args,
            kwargs=kwargs,
            task_id=task_id,
            producer=producer,
            link=link,
            link_error=link_error,
            shadow=shadow,
            **options,
        )

        task = self.lock_and_run(**run_args)
        if task:
            return task

        existing_task_id = self.get_existing_task_id(lock_id)
        celery_logger.info(f"LOCKED_ID: {existing_task_id}")
        while not existing_task_id:
            task = self.lock_and_run(**run_args)
            if task:
                return task
            existing_task_id = self.get_existing_task_id(lock_id)
        return self.on_duplicate(self.name, lock_id, task_id)

    def lock_and_run(self, lock_id: str, task_id: str = None, *args, **kwargs):
        lock_acquired = self.acquire_lock(lock_id, task_id)
        if lock_acquired:
            try:
                return super(LockedTask, self).apply_async(
                    task_id=task_id, *args, **kwargs
                )
            except Exception:
                # release lock if failed
                self.unlock(lock_id)
                raise

    def release_lock(self, task_args: str = None, task_kwargs: dict = None):
        lock_id = argument_signature(self.name, task_args, task_kwargs)
        self.unlock(lock_id)

    def on_duplicate(self, task: str, lock_id: str, task_id: str):
        if self.raise_on_duplicate:
            raise KeyError(
                f"Attempted to queue a duplicate of task: {task} with task ID: {lock_id}:{task_id}"
            )
        return self.AsyncResult(task_id)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        self.release_lock(task_args=args, task_kwargs=kwargs)
        if self.use_slack:
            post_to_slack_on_task_failure(self, exc, task_id, args, kwargs, einfo)

    def on_success(self, retval, task_id, args, kwargs):
        self.release_lock(task_args=args, task_kwargs=kwargs)
