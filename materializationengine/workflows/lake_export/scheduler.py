"""Running tables' exports: their units of work through an executor, within budgets.

Range units each hold one Postgres connection, so at most max_streams run at once across
every table: the database is what a range-parallel export saturates (minnie's 8-vCPU
Cloud SQL at ~16 streams). Every unit counts against max_tasks, which bounds what the
driver keeps queued. Tables are served in the order given (biggest first), so a big
table's ranges start first and smaller tables fill the streams it leaves free. Publishing
runs in each table's own threads; finalizing runs in the scheduler's threads, so neither
holds up the loop. A table that fails stops its own units; the others carry on.

The executor is a seam: ThreadExecutor runs units in this process (tests, local runs);
the RayJob entrypoint passes a Ray executor, which retries a unit lost with its worker.
"""

import logging
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Dict, List, Optional, Protocol, Sequence

from materializationengine.workflows.lake_export.table import TERMINAL, TableExport, Unit

logger = logging.getLogger(__name__)


class Executor(Protocol):
    def submit(self, unit: Unit): ...

    def wait(self, handles: Sequence, timeout: float) -> List: ...

    def result(self, handle): ...

    def cancel(self, handles: Sequence) -> None: ...


class ThreadExecutor:
    """Units on threads of this process."""

    def __init__(self, workers: int = 8):
        self.pool = ThreadPoolExecutor(workers, thread_name_prefix="lake-export")

    def submit(self, unit: Unit) -> Future:
        return self.pool.submit(unit.fn, *unit.args)

    def wait(self, handles, timeout):
        done, _ = wait(handles, timeout=timeout, return_when=FIRST_COMPLETED)
        return list(done)

    def result(self, handle):
        return handle.result()

    def cancel(self, handles):
        for h in handles:
            h.cancel()

    def shutdown(self):
        self.pool.shutdown(wait=True, cancel_futures=True)


class ExportScheduler:
    def __init__(self, tables: Sequence[TableExport], executor: Executor, max_streams: int, max_tasks: int,
                 poll_seconds: float = 0.5, finalize_threads: int = 2):
        self.tables = list(tables)
        self.executor = executor
        self.max_streams = max(1, int(max_streams))
        self.max_tasks = max(self.max_streams, int(max_tasks))
        self.poll_seconds = poll_seconds
        self.finalize_threads = finalize_threads

    def run(self) -> Dict[str, Dict]:
        inflight: Dict = {}  # handle -> (table, unit)
        finalizing: Dict[Future, TableExport] = {}
        finalizers = ThreadPoolExecutor(self.finalize_threads, thread_name_prefix="lake-finalize")
        for table in self.tables:
            try:
                table.start()
            except Exception as e:  # e.g. a lake another export wrote
                table.fail(f"{type(e).__name__}: {e}")
        try:
            while True:
                for table in self.tables:
                    if table.poll() == "finalize":
                        finalizing[finalizers.submit(table.finalize)] = table
                for future in [f for f in finalizing if f.done()]:
                    table = finalizing.pop(future)
                    if future.exception() is not None:
                        e = future.exception()
                        table.fail(f"finalizing: {type(e).__name__}: {e}")

                self._fill(inflight)
                if not inflight and not finalizing and all(t.state in TERMINAL for t in self.tables):
                    break
                if inflight:
                    for handle in self.executor.wait(list(inflight), timeout=self.poll_seconds):
                        table, unit = inflight.pop(handle)
                        try:
                            result = self.executor.result(handle)
                        except Exception as e:
                            table.unit_failed(unit, e)
                        else:
                            table.unit_done(unit, result)
                        if table.state == "failed":
                            self._drop(table, inflight)
                else:
                    time.sleep(self.poll_seconds)
        finally:
            finalizers.shutdown(wait=True)
        return {t.table: t.summary() for t in self.tables}

    def _fill(self, inflight: Dict) -> None:
        streams = sum(1 for _, unit in inflight.values() if unit.kind == "range")
        for table in self.tables:
            while streams < self.max_streams and len(inflight) < self.max_tasks:
                unit = table.next_range()
                if unit is None:
                    break
                inflight[self.executor.submit(unit)] = (table, unit)
                streams += 1
        for table in self.tables:
            while len(inflight) < self.max_tasks:
                unit = table.next_optimize()
                if unit is None:
                    break
                inflight[self.executor.submit(unit)] = (table, unit)

    def _drop(self, table: TableExport, inflight: Dict) -> None:
        mine = [h for h, (t, _) in inflight.items() if t is table]
        self.executor.cancel(mine)
        for h in mine:
            inflight.pop(h, None)
