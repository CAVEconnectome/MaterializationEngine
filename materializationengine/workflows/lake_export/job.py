"""Exporting tables of a version: plan (or resume) each table, then run them all.

Each table is read from an export source (lake_export.source; default its frozen
version's database), whose fields the table's plan records.

Needs the app (its config and database manager): the RayJob entrypoint calls this inside
an app context. Ray is not imported here; the caller passes the executor.
"""

import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from materializationengine.workflows.lake_export.plan import load_plan, plan_table, save_plan
from materializationengine.workflows.lake_export.scheduler import ExportScheduler, Executor
from materializationengine.workflows.lake_export.settings import ExportSettings
from materializationengine.workflows.lake_export.source import ExportSource, FrozenSource
from materializationengine.workflows.lake_export.status import TableStatus
from materializationengine.workflows.lake_export.table import TableExport

logger = logging.getLogger(__name__)


@dataclass
class TableRequest:
    table: str
    output_specs: Optional[List[Dict]] = None
    spec_overrides: Dict = field(default_factory=dict)


def prepare_table(datastack_info: Dict, version: int, request: TableRequest, job_id: str,
                  settings: ExportSettings, source: Optional[ExportSource] = None,
                  inputs: Optional[Callable] = None) -> TableExport:
    """The table's export, from its saved plan if this job planned it before (a restart),
    else from a new plan, saved before anything is written.

    inputs: (datastack_info, version, table) -> connection_string, engine,
    segmentation_table, row_count; default the source's (tests pass their own).
    """
    source = source or FrozenSource()
    datastack = datastack_info["datastack"]
    status = TableStatus(datastack, version, request.table, job_id)
    status.phase("planning", f"Planning the export of {request.table}")
    base = f"{settings.output_bucket}/{datastack}/v{version}/{request.table}"
    plan = load_plan(base, job_id)
    if plan is not None:
        status.log(f"Resuming job {job_id} from its saved plan")
    else:
        found = (inputs or source.inputs)(datastack_info, version, request.table)
        plan = plan_table(datastack=datastack, version=version, table=request.table, job_id=job_id,
                          connection_string=found["connection_string"], engine=found["engine"],
                          segmentation_table=found["segmentation_table"], row_count=found["row_count"],
                          settings=settings, output_specs=request.output_specs,
                          spec_overrides=request.spec_overrides or None, source=source)
        save_plan(plan)
        status.log(f"Planned {len(plan.ranges)} range(s) (~{plan.sizing['estimated_bytes'] / 2**30:.1f} GiB), "
                   f"{len(plan.specs)} lake(s): {', '.join(s['name'] for s in plan.specs)}")
    aligned_volume = (datastack_info.get("aligned_volume") or {}).get("name")
    return TableExport(plan, settings, status, aligned_volume=aligned_volume)


def run_tables(datastack_info: Dict, version: int, requests: Sequence[TableRequest],
               job_id_for: Callable[[str], str], settings: ExportSettings, executor: Executor,
               max_streams: Optional[int] = None, source: Optional[ExportSource] = None,
               inputs: Optional[Callable] = None,
               on_change: Optional[Callable[[Dict[str, Dict]], None]] = None) -> Dict[str, Dict]:
    """Plan every table, then export them together; a table's summary by name.

    A table that cannot be planned is reported failed; the others still run. on_change
    gets every table's summary whenever one's state changes.
    """
    exports, failed = [], {}
    for request in requests:
        job_id = job_id_for(request.table)
        try:
            exports.append(prepare_table(datastack_info, version, request, job_id, settings, source=source,
                                         inputs=inputs))
        except Exception as e:
            error = f"planning: {type(e).__name__}: {e}"
            logger.exception("could not plan %s", request.table)
            TableStatus(datastack_info["datastack"], version, request.table, job_id).failed(error)
            failed[request.table] = {"state": "failed", "error": error}
    scheduler = ExportScheduler(
        exports, executor, max_streams=max_streams or settings.max_streams, max_tasks=settings.max_tasks,
        on_change=None if on_change is None else (lambda summaries: on_change({**failed, **summaries})))
    return {**failed, **scheduler.run()}
