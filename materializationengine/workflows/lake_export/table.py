"""One table's export as a state machine the scheduler drives.

    planned -> streaming -> optimizing -> finalizing -> complete
                    \\------------\\------------\\------> failed

streaming: ranges are handed to the scheduler one at a time (next_range); each finished
range is published to every lake that lacks it, by one thread per lake, so each lake has
a single committer. optimizing: once every range is in every lake, each lake's partitions
are planned into optimize tasks by size. finalizing: a sweep of unreferenced files, the
row count of every lake checked against the frozen database's, the catalog, the planning
index and the catalog service.

A restarted job (KubeRay retrying the driver, same job_id) resumes: the plan is loaded,
ranges every lake already has are skipped, and a range only some lakes have is exported
again and published to the rest.
"""

import logging
import time
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from materializationengine import deltalake_catalog as cat
from materializationengine.workflows import deltalake_export as dx
from materializationengine.workflows.lake_export import optimize as opt
from materializationengine.workflows.lake_export import publish as pub
from materializationengine.workflows.lake_export.plan import TablePlan, delete_plan, published
from materializationengine.workflows.lake_export.ranges import RangeResult, export_range
from materializationengine.workflows.lake_export.settings import ExportSettings
from materializationengine.workflows.lake_export.sizing import plan_optimize
from materializationengine.workflows.lake_export.status import TableStatus

logger = logging.getLogger(__name__)

TERMINAL = ("complete", "failed")


# --- the work units, as plain functions a Ray task (or a thread) runs ----------------------


def run_range(plan: TablePlan, index: int) -> RangeResult:
    lo, hi = plan.ranges[index]
    return export_range(plan.connection_string, plan.table_source, plan.columns, plan.output_specs,
                        plan.geometry_columns, plan.output_uri_base, index, lo, hi, plan.flush_bytes,
                        row_filter=plan.row_filter, join=plan.export_source.join)


def run_optimize(lake_uri: str, spec: Dict, column: Optional[str], values: Optional[Tuple],
                 kwargs: Dict) -> Dict:
    return opt.optimize_partitions(lake_uri, dx.DeltaLakeOutputSpec(**spec), column,
                                   None if values is None else list(values), **kwargs)


@dataclass(frozen=True)
class Unit:
    """A unit of work for the scheduler: kind "range" (holds a Postgres stream) or
    "optimize"; fn(*args) runs on a worker."""

    table: str
    kind: str
    key: tuple
    fn: object
    args: tuple


class TableExport:
    def __init__(self, plan: TablePlan, settings: ExportSettings, status: TableStatus,
                 aligned_volume: Optional[str] = None):
        self.plan, self.settings, self.status = plan, settings, status
        self.aligned_volume = aligned_volume
        self.table = plan.table
        self.lakes = [s["name"] for s in plan.specs]
        self.state = "planned"
        self.error: Optional[str] = None
        self._writing = False  # the lakes are recorded as this export's
        self.rows = 0
        self.timings: Dict[str, float] = {"started": time.monotonic()}
        self._pending: deque = deque()
        self._needs: Dict[int, List[str]] = {}  # range -> lakes it still has to be published to
        self._running_ranges = 0
        self._publishers = {lake: ThreadPoolExecutor(1, thread_name_prefix=f"publish-{lake}") for lake in self.lakes}
        self._publishes: List[Future] = []
        self._optimize: deque = deque()
        self._running_optimize = 0
        self._optimized: Dict[str, int] = {}

    # --- start ------------------------------------------------------------------------------

    def start(self) -> None:
        """Find what is already published (refusing a lake another export wrote), record the
        lakes as writing, and queue the ranges still to export."""
        done = published(self.plan)
        resumed = sum(len(v) for v in done.values())
        for i in range(len(self.plan.ranges)):
            missing = [lake for lake in self.lakes if i not in done[lake]]
            if missing:
                self._needs[i] = missing
                self._pending.append(i)
        if self.aligned_volume:
            cat.safely(cat.record_writing, self.aligned_volume, self.plan.datastack, self.plan.version,
                       self.table, self.plan.catalog_lakes(), job_id=self.plan.job_id)
        self._writing = True
        self.state = "streaming"
        n = len(self.plan.ranges)
        self.status.phase(
            "streaming",
            f"Streaming {self.plan.row_count:,} rows as {n} range(s) into {len(self.lakes)} lake(s)"
            + (f"; resuming, {n - len(self._pending)} range(s) already published" if resumed else ""),
            total_rows=self.plan.row_count, ranges_total=n, ranges_done=n - len(self._pending),
            lakes_total=len(self.lakes))

    # --- units for the scheduler --------------------------------------------------------------

    def next_range(self) -> Optional[Unit]:
        if self.state != "streaming" or not self._pending:
            return None
        i = self._pending.popleft()
        self._running_ranges += 1
        return Unit(self.table, "range", (i,), run_range, (self.plan, i))

    def next_optimize(self) -> Optional[Unit]:
        if self.state != "optimizing" or not self._optimize:
            return None
        self._running_optimize += 1
        return self._optimize.popleft()

    def unit_done(self, unit: Unit, result) -> None:
        if self.state in TERMINAL:
            return
        if unit.kind == "range":
            self._running_ranges -= 1
            self._range_done(result)
        else:
            self._running_optimize -= 1
            lake = unit.key[0]
            self._optimized[lake] -= 1
            if self._optimized[lake] == 0:
                done = sum(1 for v in self._optimized.values() if v == 0)
                self.status.update(force=True, lakes_optimized=done)

    def unit_failed(self, unit: Unit, error: BaseException) -> None:
        cause = getattr(error, "cause", None) or error
        what = f"range {unit.key[0]}" if unit.kind == "range" else f"optimizing {unit.key[0]}"
        self.fail(f"{what}: {type(cause).__name__}: {cause}")

    # --- streaming and publishing ---------------------------------------------------------

    def _range_done(self, result: RangeResult) -> None:
        i = result.range_index
        self.rows += result.rows
        for lake in self._needs.get(i, []):
            spec = next(s for s in self.plan.specs if s["name"] == lake)
            partition_by = dx._spec_partition_by(dx.DeltaLakeOutputSpec(**spec))
            self._publishes.append(self._publishers[lake].submit(
                pub.publish, self.plan.lake_uri(lake), result.stage, partition_by, i, self.plan.job_id))
        done = len(self.plan.ranges) - len(self._pending) - self._running_ranges
        self.status.update(rows_processed=self.rows, ranges_done=done)

    def poll(self) -> Optional[str]:
        """Advance the state machine; returns "finalize" when the table is ready for it."""
        if self.state in TERMINAL:
            return None
        failed = next((f for f in self._publishes if f.done() and f.exception()), None)
        if failed is not None:
            self.fail(f"publishing: {type(failed.exception()).__name__}: {failed.exception()}")
            return None
        if self.state == "streaming" and not self._pending and not self._running_ranges \
                and all(f.done() for f in self._publishes):
            self.timings["streamed"] = time.monotonic()
            self._plan_optimize()
        if self.state == "optimizing" and not self._optimize and not self._running_optimize:
            self.timings["optimized"] = time.monotonic()
            self.state = "finalizing"
            return "finalize"
        return None

    # --- optimizing -----------------------------------------------------------------------------

    def _plan_optimize(self) -> None:
        from deltalake import DeltaTable

        if not any(DeltaTable.is_deltatable(self.plan.lake_uri(lake)) for lake in self.lakes):
            # nothing to optimize: the table has no rows (checked when finalizing)
            self.state = "optimizing"
            return
        kwargs = {"max_concurrent_tasks": 1, "target_size": self.settings.optimize_target_size,
                  "max_spill_size": self.settings.optimize_max_spill_size}
        units = []
        for spec in self.plan.specs:
            uri = self.plan.lake_uri(spec["name"])
            column = (dx._spec_partition_by(dx.DeltaLakeOutputSpec(**spec)) or [None])[0]
            tasks = plan_optimize(opt.partition_stats(uri, column), partitioned=column is not None,
                                  cfg=self.settings.sizing)
            self._optimized[spec["name"]] = len(tasks)
            units += [(t.bytes, Unit(self.table, "optimize", (spec["name"], t.values), run_optimize,
                                     (uri, spec, column, t.values, kwargs))) for t in tasks]
        # largest first across the table's lakes, so the long tasks start early
        self._optimize = deque(u for _, u in sorted(units, key=lambda x: -x[0]))
        self.state = "optimizing"
        self.status.phase("optimizing", f"Optimizing {len(self.lakes)} lake(s) as {len(units)} task(s)",
                          rows_processed=self.rows)

    # --- finalizing ------------------------------------------------------------------------------

    def finalize(self) -> None:
        """Sweep, verify, catalog, index, register; then complete (or fail)."""
        from deltalake import DeltaTable

        from materializationengine import catalog_service
        from materializationengine.blueprints.client import lake_index

        plan = self.plan
        lakes = [lake for lake in plan.catalog_lakes() if DeltaTable.is_deltatable(lake["uri"])]
        self.status.phase("finalizing", "Checking and indexing the lakes")
        if not lakes:
            if plan.row_count:
                raise RuntimeError(f"no lakes were written, but the table has {plan.row_count} rows")
            if self.aligned_volume:
                cat.safely(cat.forget, self.aligned_volume, plan.datastack, plan.version, self.table, self.lakes)
            self._finish("The table has no rows; no lakes written.")
            return
        with ThreadPoolExecutor(len(lakes)) as pool:
            list(pool.map(lambda lake: opt.sweep(lake["uri"]), lakes))
        # every lake must hold the frozen database's rows, whether or not the catalog is reachable
        counts = {lake["spec_name"]: cat.lake_facts(lake["uri"])["row_count"] for lake in lakes}
        if self.aligned_volume:
            cat.safely(cat.record_finished, self.aligned_volume, plan.datastack, plan.version, self.table,
                       lakes, plan.row_count)
        wrong = {name: n for name, n in counts.items() if n != plan.row_count}
        if wrong:
            raise RuntimeError(f"Delta Lake(s) {wrong} do not match the source's {plan.row_count} rows")

        def index(lake):
            try:
                lake_index.build_and_store(lake["uri"], lake_index.key_columns(lake))
            except Exception:
                logger.warning("no planning index for %s", lake["uri"], exc_info=True)

        with ThreadPoolExecutor(len(lakes)) as pool:
            list(pool.map(index, lakes))
        if self.aligned_volume and catalog_service.enabled():
            result = cat.safely(catalog_service.register_version, self.aligned_volume, plan.datastack,
                                plan.version, self.table)
            if result is not None:
                self.status.log(f"Catalog service: registered {len(result['registered'])} lake(s)"
                                + (f", failed {result['failed']}" if result["failed"] else ""))
        self._finish()

    def _finish(self, message: Optional[str] = None) -> None:
        delete_plan(self.plan)
        self._shutdown()
        self.state = "complete"
        self.timings["finished"] = time.monotonic()
        if message:
            self.status.log(message)
        self.status.complete()

    # --- failure ---------------------------------------------------------------------------------

    def fail(self, error: str) -> None:
        if self.state in TERMINAL:
            return
        self.state, self.error = "failed", error
        self.timings["finished"] = time.monotonic()
        self._shutdown(cancel=True)
        if not self._writing:
            # refused before writing anything (e.g. a lake another export wrote): the
            # catalog rows are that export's, and the plan serves no restart
            delete_plan(self.plan)
        elif self.aligned_volume:
            cat.safely(cat.record_failed, self.aligned_volume, self.plan.datastack, self.plan.version,
                       self.table, self.lakes, error)
        self.status.failed(error)

    def _shutdown(self, cancel: bool = False) -> None:
        for pool in self._publishers.values():
            pool.shutdown(wait=not cancel, cancel_futures=cancel)

    def summary(self) -> Dict:
        t = self.timings
        out = {"state": self.state, "rows": self.rows, "ranges": len(self.plan.ranges), "lakes": len(self.lakes),
               "error": self.error}
        if "streamed" in t:
            out["stream_s"] = round(t["streamed"] - t["started"], 1)
        if "optimized" in t:
            out["optimize_s"] = round(t["optimized"] - t["streamed"], 1)
        if "finished" in t:
            out["total_s"] = round(t["finished"] - t["started"], 1)
        return out
