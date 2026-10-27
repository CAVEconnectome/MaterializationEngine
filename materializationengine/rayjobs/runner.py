"""Running a bulk upload's parallel work: on Ray workers, or in-process for tests.

The driver (entrypoints/bulk_upload.py) only talks to a Runner, so its resume and
failure handling is tested with InlineRunner and no Ray cluster. RayRunner sends the
same calls to rayjobs.upload_tasks.run_task on the workers.
"""

import collections
import logging
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Stopped(Exception):
    """The bounded run was told to stop (the upload was cancelled, or the lease lost)."""


# Per task: Ray options. Parts are idempotent (each overwrites its own output object),
# so application errors are retried there too. A spatial chunk's application errors
# come back to the driver instead, which counts them as attempts (FAILED_RETRYABLE,
# then FAILED_PERMANENT); Ray still retries a chunk whose worker died.
TASK_OPTIONS: Dict[str, dict] = {
    "count_csv_part": {"max_retries": 3, "retry_exceptions": True},
    "csv_part": {"max_retries": 3, "retry_exceptions": True},
    "parquet_part": {"max_retries": 3, "retry_exceptions": True},
    "spatial_chunk": {"max_retries": 3, "retry_exceptions": False},
}


class InlineRunner:
    """Runs each call when it is submitted, in this process."""

    def __init__(self, tasks: Optional[Dict[str, Callable]] = None):
        if tasks is None:
            from materializationengine.rayjobs import upload_tasks

            tasks = upload_tasks.TASKS
        self.tasks = tasks
        self.calls: List[Tuple[str, tuple]] = []
        self._results: Dict[int, Tuple[bool, Any]] = {}
        self._next = 0

    def submit(self, name: str, *args) -> int:
        self.calls.append((name, args))
        handle = self._next
        self._next += 1
        try:
            self._results[handle] = (True, self.tasks[name](*args))
        except Exception as e:  # returned from get(), as Ray would
            self._results[handle] = (False, e)
        return handle

    def wait(self, handles: List[int], timeout: float) -> List[int]:
        return list(handles)

    def get(self, handle: int) -> Any:
        ok, value = self._results.pop(handle)
        if not ok:
            raise value
        return value

    def cancel(self, handles: Iterable[int]) -> None:
        pass


class RayRunner:
    """Runs calls as Ray tasks (ray.init() must have been called)."""

    def __init__(self, num_cpus: float = 1, fn: Optional[Callable] = None):
        """fn(name, *args) runs on the workers; upload_tasks.run_task by default."""
        import ray

        if fn is None:
            from materializationengine.rayjobs import upload_tasks

            fn = upload_tasks.run_task
        self._ray = ray
        self._remote = ray.remote(num_cpus=num_cpus)(fn)

    def submit(self, name: str, *args):
        return self._remote.options(**TASK_OPTIONS.get(name, {})).remote(name, *args)

    def wait(self, handles: list, timeout: float) -> list:
        ready, _ = self._ray.wait(
            list(handles), num_returns=len(handles), timeout=timeout, fetch_local=False
        )
        return ready

    def get(self, handle) -> Any:
        try:
            return self._ray.get(handle)
        except self._ray.exceptions.RayTaskError as e:
            # the worker's own exception, so callers can tell what went wrong
            cause = getattr(e, "cause", None)
            if isinstance(cause, Exception):
                raise cause from e
            raise

    def cancel(self, handles: Iterable) -> None:
        for handle in handles:
            try:
                self._ray.cancel(handle)
            except Exception as e:
                logger.warning(f"Could not cancel a Ray task: {e}")


def run_bounded(
    runner,
    task: str,
    items: Iterable[Tuple[Hashable, tuple]],
    max_in_flight: int,
    on_done: Callable[[Hashable, Any], None],
    on_error: Optional[Callable[[Hashable, Exception], bool]] = None,
    should_stop: Optional[Callable[[], bool]] = None,
    on_tick: Optional[Callable[[], None]] = None,
    poll_seconds: float = 2.0,
) -> None:
    """Run task over items, (key, args) pairs, with at most max_in_flight at a time.

    on_done(key, result) is called as each finishes. A failure calls on_error(key, exc),
    which returns True to run that item again; without on_error a failure is raised.
    should_stop is checked between waits; when it says so the calls in flight are
    cancelled and Stopped is raised. on_tick runs after every wait (for flushing
    progress). Items are consumed lazily, so a long range costs nothing up front.
    """
    items = iter(items)
    retry = collections.deque()
    pending: Dict[Any, Tuple[Hashable, tuple]] = {}
    exhausted = False
    try:
        while True:
            if should_stop is not None and should_stop():
                raise Stopped()
            while len(pending) < max_in_flight:
                if retry:
                    key, args = retry.popleft()
                elif not exhausted:
                    try:
                        key, args = next(items)
                    except StopIteration:
                        exhausted = True
                        continue
                else:
                    break
                pending[runner.submit(task, *args)] = (key, args)
            if not pending:
                break
            for handle in runner.wait(list(pending), timeout=poll_seconds):
                key, args = pending.pop(handle)
                try:
                    result = runner.get(handle)
                except Exception as e:
                    if on_error is None:
                        raise
                    if on_error(key, e):
                        retry.append((key, args))
                    continue
                on_done(key, result)
            if on_tick is not None:
                on_tick()
    except BaseException:
        if pending:
            runner.cancel(list(pending))
        raise
    if on_tick is not None:
        on_tick()
