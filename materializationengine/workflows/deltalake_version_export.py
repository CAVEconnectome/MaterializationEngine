"""Export every table of one frozen version to Delta Lake, as one job.

The per-table export (run_deltalake_export) is unchanged; this decides which tables, with
which settings, and how many at once, and keeps one status record for the version.

Why one job per version rather than one per table: on ltv7 (2026-11-09) exporting the 27
tables of v2253 one RayJob per table spent ~60-125 s per job just bringing a Ray cluster
up, most of the 35 minutes. One cluster for the whole version pays that once.

How many at once: a table export streams its rows out of the frozen database. One stream
is limited by the exporter (~110-125k rows/s on both ltv and minnie), not the database,
and used ~20% of minnie's disk read throughput, so a small number of concurrent streams
(DELTALAKE_MAX_CONCURRENT_TABLE_STREAMS, default 2) keeps the version moving without
starving production queries on the same instance. Biggest tables start first.

Per-table settings come from DELTALAKE_EXPORT_CONFIG (JSON):

    {"defaults": {"max_row_group_size": 10000},
     "tables": {"synapses_pni_2": {"n_partitions": 512},
                "some_table": {"output_specs": [...]},
                "scratch_table": {"skip": true}}}

"defaults" and a table's entry are overrides applied to every discovered output spec
(any DeltaLakeOutputSpec field); a table entry's "output_specs" replaces discovery for
that table; "skip" leaves it out.
"""

import json
import logging
from dataclasses import fields
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_MAX_CONCURRENT_TABLE_STREAMS = 2
_VERSION_STATUS_TTL = 7 * 86400
# PostGIS's catalog views live in every frozen database's public schema.
_SYSTEM_VIEWS = {"geometry_columns", "geography_columns", "raster_columns", "raster_overviews"}


def version_status_key(datastack: str, version: int, job_id: str) -> str:
    return f"deltalake_version_export:{datastack}:v{version}:{job_id}"


def table_job_id(job_id: str, table: str) -> str:
    """The job id a table's own progress record (get_deltalake_export_progress) uses."""
    return f"{job_id}.{table}"


def list_version_tables(datastack: str, version: int, include_views: bool = False) -> List[Dict]:
    """The version's annotation tables (MaterializedMetadata), biggest first, and optionally
    its views (rows unknown, after the tables)."""
    from sqlalchemy import text

    from materializationengine.database import db_manager
    from materializationengine.models import MaterializedMetadata

    database = f"{datastack}__mat{version}"
    with db_manager.session_scope(database) as session:
        rows = session.query(MaterializedMetadata.table_name, MaterializedMetadata.row_count).all()
        tables = [{"table": t, "rows": int(n or 0), "kind": "table"} for t, n in rows]
        if include_views:
            names = session.execute(text(
                "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm') ORDER BY c.relname"
            )).fetchall()
            tables += [{"table": r[0], "rows": None, "kind": "view"} for r in names if r[0] not in _SYSTEM_VIEWS]
    seen, out = set(), []
    for t in sorted(tables, key=lambda t: (t["kind"] != "table", -(t["rows"] or 0), t["table"])):
        if t["table"] not in seen:
            seen.add(t["table"])
            out.append(t)
    return out


def load_export_config(raw) -> Dict:
    """DELTALAKE_EXPORT_CONFIG as a dict ({} when unset); a JSON string or a dict."""
    if not raw:
        return {}
    config = json.loads(raw) if isinstance(raw, str) else dict(raw)
    if not isinstance(config, dict):
        raise ValueError("DELTALAKE_EXPORT_CONFIG must be a JSON object")
    unknown = set(config) - {"defaults", "tables"}
    if unknown:
        raise ValueError(f"DELTALAKE_EXPORT_CONFIG: unknown keys {sorted(unknown)} (expected defaults, tables)")
    _check_overrides("defaults", config.get("defaults") or {})
    for table, entry in (config.get("tables") or {}).items():
        if not isinstance(entry, dict):
            raise ValueError(f"DELTALAKE_EXPORT_CONFIG: tables.{table} must be an object")
        _check_overrides(f"tables.{table}", {k: v for k, v in entry.items() if k not in ("output_specs", "skip")})
    return config


def _spec_fields() -> set:
    from materializationengine.workflows.deltalake_export import DeltaLakeOutputSpec

    return {f.name for f in fields(DeltaLakeOutputSpec)} - {"name"}


def _check_overrides(where: str, overrides: Dict) -> None:
    unknown = set(overrides) - _spec_fields() - {"name_suffix"}
    if unknown:
        raise ValueError(f"DELTALAKE_EXPORT_CONFIG: {where} has unknown output spec fields {sorted(unknown)}")


def resolve_table_config(config: Dict, table: str) -> Tuple[bool, Optional[list], Dict]:
    """(skip, output_specs or None for discovery, overrides for every spec) for one table."""
    entry = dict((config.get("tables") or {}).get(table) or {})
    skip = bool(entry.pop("skip", False))
    output_specs = entry.pop("output_specs", None)
    overrides = {**(config.get("defaults") or {}), **entry}
    return skip, output_specs, overrides


def set_version_status(datastack: str, version: int, job_id: str, **fields_) -> Dict:
    from materializationengine.workflows.deltalake_export import _get_redis_client

    client = _get_redis_client()
    key = version_status_key(datastack, version, job_id)
    raw = client.get(key)
    status = json.loads(raw) if raw else {"job_id": job_id, "datastack": datastack, "version": version}
    status.update(fields_, last_updated=datetime.now(timezone.utc).isoformat())
    client.set(key, json.dumps(status, default=str), ex=_VERSION_STATUS_TTL)
    return status


def get_version_status(datastack: str, version: int, job_id: str) -> Optional[Dict]:
    from materializationengine.workflows.deltalake_export import (
        _get_redis_client,
        get_deltalake_export_progress,
    )

    raw = _get_redis_client().get(version_status_key(datastack, version, job_id))
    if not raw:
        return None
    status = json.loads(raw)
    for name, table in (status.get("tables") or {}).items():
        if table.get("state") == "running":
            progress = get_deltalake_export_progress(datastack, version, name, job_id=table_job_id(job_id, name))
            if progress:
                table["progress"] = {k: progress.get(k) for k in
                                     ("phase", "rows_processed", "total_rows", "percent_complete")}
    return status


def run_tables(tables: List[Dict], start: Callable, wait: Callable, max_streams: int,
               on_change: Callable[[Dict], None]) -> Dict[str, Dict]:
    """Export tables at most max_streams at a time, in the given order.

    start(table) returns a handle; wait(handles) blocks until one finishes and returns
    (handle, error or None). Each table ends "done" or "failed" (with the error); a
    failure does not stop the others. on_change gets the per-table states after every
    change. Kept free of Ray so the scheduling can be tested on its own.
    """
    max_streams = max(1, int(max_streams))
    states = {t["table"]: {"state": "pending", "rows": t.get("rows"), "kind": t.get("kind", "table")} for t in tables}
    pending = list(tables)
    running = {}
    on_change(states)
    while pending or running:
        while pending and len(running) < max_streams:
            t = pending.pop(0)
            try:
                running[start(t)] = t["table"]
                states[t["table"]].update(state="running", started=_now())
            except Exception as e:  # could not even be submitted
                states[t["table"]].update(state="failed", error=f"{type(e).__name__}: {e}", finished=_now())
            on_change(states)
        if not running:
            continue
        handle, error = wait(list(running))
        name = running.pop(handle)
        if error is None:
            states[name].update(state="done", finished=_now())
        else:
            states[name].update(state="failed", error=error, finished=_now())
            logger.error("Delta Lake export of %s failed: %s", name, error)
        on_change(states)
    return states


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
