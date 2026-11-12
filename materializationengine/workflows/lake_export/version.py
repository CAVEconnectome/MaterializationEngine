"""Exporting a whole frozen version: every table, then the views stage.

    tables (every table's ranges and optimizes, one stream budget) -> views -> done

The views stage runs only once every table of the job is complete, and is the place the
lake-based view materialization plugs in, without either side editing the other's code:
DELTALAKE_EXPORT_VIEWS_STAGE names a "module:function" that is called with a
VersionContext and returns a StageResult (or a dict of its fields). It runs in the job's
driver, with Ray already initialised on the job's cluster. Its contract:

- it reads the version's table lakes from the catalog (ctx.lakes: deltalake_catalog rows
  with status complete -- the same rows lake_query and view_pilot.version_lakes read),
  never from SQL;
- it writes view lakes under <DELTALAKE_OUTPUT_BUCKET>/<datastack>/v<version>/<view>/...
  and records them in deltalake_catalog like tables, so queries, expiry and the catalog
  service treat them alike.

Without a configured stage, a job asked for views reports the stage as not_implemented;
a job not asked for views skips it.

The version's status record is the one get_version_status reads
(deltalake_version_export:<ds>:v<v>:<job_id>): per table a state of pending, running,
done or failed -- get_version_status folds a running table's own progress in -- plus its
phase, and the job's stages.
"""

import copy
import importlib
import logging
from dataclasses import asdict, dataclass, field
from typing import Callable, Dict, List, Optional, Sequence

from materializationengine.workflows.lake_export.job import TableRequest, run_tables
from materializationengine.workflows.lake_export.scheduler import Executor
from materializationengine.workflows.lake_export.settings import ExportSettings
from materializationengine.workflows.lake_export.source import ExportSource

logger = logging.getLogger(__name__)

VIEWS_STAGE_CONFIG = "DELTALAKE_EXPORT_VIEWS_STAGE"


@dataclass
class VersionContext:
    datastack_info: Dict
    datastack: str
    version: int
    aligned_volume: Optional[str]
    job_id: str
    settings: ExportSettings
    tables: Dict[str, Dict]  # every table's export summary (all complete)
    lakes: List[Dict]  # the version's complete lakes, as deltalake_catalog.list_exports returns them


@dataclass
class StageResult:
    state: str  # done | failed | skipped | not_implemented
    error: Optional[str] = None
    detail: Dict = field(default_factory=dict)


def _status_state(table_state: str) -> str:
    return {"complete": "done", "failed": "failed", "planned": "pending"}.get(table_state, "running")


def resolve_stage(path: Optional[str]) -> Optional[Callable]:
    """The function a "module:function" path names (None for an empty path)."""
    if not path:
        return None
    module, _, name = path.partition(":")
    if not name:
        raise ValueError(f"{VIEWS_STAGE_CONFIG} must be 'module:function', not {path!r}")
    return getattr(importlib.import_module(module), name)


def run_views_stage(ctx: VersionContext, stage: Optional[Callable]) -> StageResult:
    if stage is None:
        return StageResult("not_implemented", detail={"message": f"no views stage configured ({VIEWS_STAGE_CONFIG})"})
    try:
        result = stage(ctx)
    except Exception as e:
        logger.exception("views stage failed")
        return StageResult("failed", error=f"{type(e).__name__}: {e}")
    if isinstance(result, dict):
        result = StageResult(**result)
    return result or StageResult("done")


class VersionStatus:
    """The version's status record (best effort, like a table's)."""

    def __init__(self, datastack: str, version: int, job_id: str, write: Optional[Callable] = None):
        from materializationengine.workflows import deltalake_version_export as dve

        self.datastack, self.version, self.job_id = datastack, version, job_id
        self._write = write or dve.set_version_status

    def update(self, **fields) -> None:
        try:
            # a copy: the caller keeps changing its dicts (stages) after this update
            self._write(self.datastack, self.version, self.job_id, **copy.deepcopy(fields))
        except Exception:
            logger.warning("could not write the version status of %s", self.job_id, exc_info=True)

    def tables(self, summaries: Dict[str, Dict]) -> None:
        self.update(tables={name: {"state": _status_state(s["state"]), "phase": s["state"], "kind": "table",
                                   "rows": s.get("rows"), "ranges": s.get("ranges"), "error": s.get("error")}
                            for name, s in summaries.items()})


def run_version(datastack_info: Dict, version: int, job_id: str, requests: Sequence[TableRequest],
                settings: ExportSettings, executor: Executor, views: bool = False,
                views_stage: Optional[Callable] = None, skipped: Sequence[str] = (),
                max_streams: Optional[int] = None, source: Optional[ExportSource] = None,
                inputs: Optional[Callable] = None, status: Optional[VersionStatus] = None) -> Dict:
    """Export the tables, then (if asked, and every table is complete) the views stage."""
    from materializationengine import deltalake_catalog as cat
    from materializationengine.workflows import deltalake_version_export as dve

    datastack = datastack_info["datastack"]
    aligned_volume = (datastack_info.get("aligned_volume") or {}).get("name")
    status = status or VersionStatus(datastack, version, job_id)
    stages = {"tables": {"state": "running"}, "views": {"state": "pending" if views else "skipped"}}
    status.update(state="running", skipped=list(skipped), stages=stages,
                  tables={r.table: {"state": "pending", "kind": "table"} for r in requests})

    summaries = run_tables(datastack_info, version, requests, job_id_for=lambda t: dve.table_job_id(job_id, t),
                           settings=settings, executor=executor, max_streams=max_streams, source=source,
                           inputs=inputs, on_change=status.tables)
    status.tables(summaries)
    failed = sorted(t for t, s in summaries.items() if s["state"] != "complete")
    stages["tables"] = {"state": "failed" if failed else "done", "failed": failed}

    if not views:
        result = StageResult("skipped")
    elif failed:
        result = StageResult("skipped", error=f"{len(failed)} table(s) failed: {failed}")
    else:
        stages["views"] = {"state": "running"}
        status.update(stages=stages)
        lakes = cat.safely(cat.list_exports, aligned_volume, datastack, version) if aligned_volume else []
        ctx = VersionContext(datastack_info=datastack_info, datastack=datastack, version=int(version),
                             aligned_volume=aligned_volume, job_id=job_id, settings=settings,
                             tables=summaries, lakes=lakes or [])
        result = run_views_stage(ctx, views_stage)
    stages["views"] = asdict(result)
    state = "failed" if failed or result.state == "failed" else "done"
    status.update(state=state, stages=stages, failed_tables=failed)
    return {"state": state, "tables": summaries, "views": asdict(result)}
