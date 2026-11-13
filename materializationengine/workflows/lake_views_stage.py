"""The views stage of a version's lake export: build the datastack's views from the table lakes.

Plugged into lake_export/version.py by DELTALAKE_EXPORT_VIEWS_STAGE =
"materializationengine.workflows.lake_views_stage:build_views". It runs in the export job's
driver, with Ray up on the job's cluster, once every table of the version is complete:

    views to build (analysisviews, or DELTALAKE_EXPORT_VIEWS)
      -> per view: lake_views.lake_sql over the version's table lakes (ctx.lakes)
      -> Daft on Ray computes it once, into a copy sorted by its first *_root_id column
         (a "flat" copy for a view without one), and each further *_root_id copy is a
         re-sort of the first
      -> each copy recorded in deltalake_catalog, given its planning index, and the view
         registered with the catalog service, like a table's lakes

so lake_query serves a view's copies as it serves a table's, and expiry deletes them with
the version.

Where the SQL comes from: each view's pg_get_viewdef in the live aligned-volume database
(metadata only; the rows come from the lakes), so this keeps working once versions are
exported from the live database and the frozen ones are gone.

Robust on preemptible workers (measured on minnie, view pilot): Daft on Ray can't rebuild
the shuffle data of a worker lost mid-query (ObjectReconstructionFailedError), so a view
is built again, up to DELTALAKE_VIEW_ATTEMPTS times, once the cluster has its CPUs back.
That's safe because a copy commits only the files Daft reports writing (_to_delta): a lost
attempt's files stay out of the table, and are deleted once a later attempt commits.
A view whose copies are already complete for this job is skipped, so a restarted driver
resumes. A view reading a table with no lake is skipped and reported; one that fails to
build fails the stage, and so the job.

Settings (app config, else environment):
    DELTALAKE_EXPORT_VIEWS          JSON list of views to build (default: analysisviews)
    DELTALAKE_VIEW_ATTEMPTS         tries per view (default 3)
    DELTALAKE_VIEW_ROW_GROUP_ROWS   rows per row group of a view lake (default 10000)
"""

import json
import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple

import pyarrow as pa

from materializationengine.blueprints.client import lake_views as lv

logger = logging.getLogger(__name__)

FLAT = "flat"


# --- committing written Parquet as a Delta table, and surviving lost workers -------------


def _to_delta(uri: str, paths: Optional[List[str]] = None, threads: int = 32) -> float:
    """Make Parquet files written under uri a Delta table; its seconds.

    paths: the files to commit, as the writer reported them. Pass them whenever the writer
    can retry: a Daft task lost with its worker is run again and writes new files, and the
    lost task's finished files stay behind, so committing everything under uri counted
    some rows twice (minnie's connections_with_nuclei: 399M rows for 303M, after a worker
    was killed mid-build). Without paths, every file under uri (a writer that never
    retries, e.g. DuckDB's COPY).

    One listing for the files' sizes, every footer read in parallel, and one commit of
    their add actions (with the statistics Daft's own Delta writer records).
    convert_to_deltalake reads the footers one at a time: 12-17 s for minnie's
    connections_with_nuclei whether 4 or 16 workers wrote it."""
    from concurrent.futures import ThreadPoolExecutor

    import fsspec
    import pyarrow.parquet as pq
    from daft.io.delta_lake.delta_lake_write import make_deltalake_add_action
    from deltalake import Schema
    from deltalake.transaction import create_table_with_add_actions

    started = time.perf_counter()
    fs, root = fsspec.core.url_to_fs(uri)
    listed = {f["name"].rsplit("/", 1)[-1]: f for f in fs.ls(root, detail=True)
              if f["type"] == "file" and f["name"].endswith(".parquet")}
    if paths is None:
        files = list(listed.values())
    else:
        names = [p.rstrip("/").rsplit("/", 1)[-1] for p in paths]
        missing = [n for n in names if n not in listed]
        if missing:
            raise ValueError(f"the writer reported {len(missing)} file(s) not under {uri}, e.g. {missing[:3]}")
        files = [listed[n] for n in dict.fromkeys(names)]
    if not files:
        raise ValueError(f"no Parquet files under {uri}")

    def footer(f):
        with fs.open(f["name"], "rb", block_size=1 << 20) as fh:
            return pq.ParquetFile(fh).metadata

    with ThreadPoolExecutor(min(threads, len(files))) as pool:
        metadata = list(pool.map(footer, files))
    actions = [make_deltalake_add_action(f["name"].rsplit("/", 1)[-1], md, int(f["size"]), {})
               for f, md in zip(files, metadata)]
    schema = Schema.from_arrow(metadata[0].schema.to_arrow_schema())
    create_table_with_add_actions(uri, schema, actions, mode="error")
    return round(time.perf_counter() - started, 3)


def _remove_uncommitted(uri: str) -> int:
    """Delete the Parquet files under uri that its Delta table doesn't hold (a lost
    attempt's); how many."""
    import fsspec
    from deltalake import DeltaTable

    fs, root = fsspec.core.url_to_fs(uri)
    committed = {f.rstrip("/").rsplit("/", 1)[-1] for f in DeltaTable(uri).file_uris()}
    orphans = [f for f in fs.ls(root, detail=False)
               if f.endswith(".parquet") and f.rsplit("/", 1)[-1] not in committed]
    if orphans:
        fs.rm(orphans)
    return len(orphans)


def _cluster_cpus() -> Optional[float]:
    try:
        import ray

        return float(ray.cluster_resources().get("CPU", 0)) if ray.is_initialized() else None
    except Exception:
        return None


def _wait_for_cluster(cpus: Optional[float], wait: float, timeout: float = 600.0) -> None:
    """Before a retry: wait, then until the Ray cluster has its CPUs back (a preempted
    worker's replacement has joined), at most timeout seconds."""
    time.sleep(wait)
    deadline = time.monotonic() + timeout
    while cpus and (_cluster_cpus() or 0) < cpus and time.monotonic() < deadline:
        time.sleep(10)


# --- which views, from where ----------------------------------------------------------------


def view_definitions(engine) -> Tuple[Dict[str, str], Dict[str, List[Dict]]]:
    """Every view's definition (pg_get_viewdef) and columns ({"name", "type"}), by name."""
    rows = engine.execute("""
        SELECT c.relname, pg_get_viewdef(c.oid, true),
               (SELECT json_agg(json_build_object('name', a.attname, 'type', format_type(a.atttypid, a.atttypmod))
                                ORDER BY a.attnum)
                FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')
          AND c.relname NOT IN ('geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews')
    """).fetchall()
    defs, cols = {}, {}
    for name, definition, columns in rows:
        defs[name] = definition
        cols[name] = json.loads(columns) if isinstance(columns, str) else columns
    return defs, cols


def registered_views(engine, datastack: str) -> List[str]:
    """The datastack's views as users see them: its analysisviews rows."""
    import sqlalchemy as sa

    # dialect.has_table, not inspect(engine).has_table: SQLAlchemy is 1.3 here (the lock
    # file), whose Inspector has no has_table
    if not engine.dialect.has_table(engine, "analysisviews"):
        return []
    rows = engine.execute(sa.text("SELECT table_name FROM analysisviews WHERE datastack_name = :ds ORDER BY id"),
                          ds=datastack).fetchall()
    return [r[0] for r in rows]


def _views_to_build(datastack: str, registered: List[str]) -> List[str]:
    from materializationengine.utils import get_config_param

    raw = get_config_param("DELTALAKE_EXPORT_VIEWS", None)
    if raw in (None, ""):
        return list(dict.fromkeys(registered))
    views = json.loads(raw) if isinstance(raw, str) else list(raw)
    if not isinstance(views, list) or not all(isinstance(v, str) for v in views):
        raise ValueError("DELTALAKE_EXPORT_VIEWS must be a JSON list of view names")
    return list(dict.fromkeys(views))


def table_lakes(rows: Iterable[Dict], job_id: Optional[str] = None) -> Dict[str, Dict]:
    """One complete lake per table to read a view from: its smallest copy (every copy of a
    table holds every row), preferring the lakes job_id exported (its table jobs are
    "<job_id>.<table>"), so a view built beside an earlier export reads this export's tables."""
    def ours(row):
        return bool(job_id) and str(row.get("job_id") or "").startswith(f"{job_id}.")

    best: Dict[str, Dict] = {}
    for row in rows:
        if row.get("status", "complete") != "complete":
            continue
        current = best.get(row["table_name"])
        if current is None or (ours(row), -(row.get("bytes") or 0)) > (ours(current), -(current.get("bytes") or 0)):
            best[row["table_name"]] = row
    return best


def _lake_columns(uri: str) -> List[str]:
    from deltalake import DeltaTable

    return [f.name for f in DeltaTable(uri).schema().fields]


def copy_keys(columns: List[str]) -> List[Optional[str]]:
    """The copies a view gets: one sorted by each *_root_id column, or one flat copy."""
    keys = [c for c in columns if c.endswith("root_id")]
    return keys or [None]


# --- building one view ---------------------------------------------------------------------


def _row_group_bytes(schema: pa.Schema, rows: int) -> int:
    """Daft sizes row groups in bytes: rows times the row's width (strings guessed at 16)."""
    width = 0
    for f in schema:
        t = f.type
        width += (t.bit_width // 8) if (pa.types.is_integer(t) or pa.types.is_floating(t)) else \
            1 if pa.types.is_boolean(t) else 16
    return max(64 * 1024, rows * max(width, 1))


def _write_copy(df, uri: str, key: Optional[str]) -> Dict:
    """Write df (sorted by key) as Snappy Parquet from every worker, and commit exactly the
    files Daft reports."""
    started = time.perf_counter()
    if key:
        df = df.sort(key)
    written = df.write_parquet(uri, compression="snappy").to_pydict()["path"]
    commit = _to_delta(uri, paths=written)
    return {"seconds": round(time.perf_counter() - started, 3), "commit_seconds": commit, "files": len(written)}


def build_view(plan: lv.LakeView, lakes: Dict[str, str], copies: List[Tuple[str, Optional[str], str]],
               row_group_rows: int = 10000) -> List[Dict]:
    """Compute the view once with Daft into its first copy, then re-sort that copy into the
    others. copies: (spec_name, sort key or None, uri) in build order. Returns per copy
    seconds, commit seconds and files."""
    import daft
    from daft.session import Session

    sess = Session()
    for table, uri in lakes.items():
        sess.create_temp_table(table, daft.read_deltalake(uri))
    df = sess.sql(plan.sql)
    daft.set_execution_config(parquet_target_row_group_size=_row_group_bytes(df.schema().to_pyarrow_schema(),
                                                                            row_group_rows))
    out = []
    first_uri = None
    for spec_name, key, uri in copies:
        source = df if first_uri is None else daft.read_deltalake(first_uri)
        out.append({"spec_name": spec_name, **_write_copy(source, uri, key)})
        first_uri = first_uri or uri
    return out


def _complete_specs(lakes: List[Dict], view: str, job_id: str) -> set:
    return {r["spec_name"] for r in lakes if r["table_name"] == view and r.get("status", "complete") == "complete"
            and r.get("job_id") == job_id}


def _view_job_id(job_id: str, view: str) -> str:
    return f"{job_id}.{view}"


class ViewSkipped(Exception):
    """The view can't be built from this version's lakes (it reads a table that has none)."""


def plan_view(ctx, view: str, definitions: Dict[str, str], columns: Dict[str, List[Dict]],
              lakes: Dict[str, Dict]) -> lv.LakeView:
    lake_cols = {t: _lake_columns(row["uri"]) for t, row in lakes.items()}
    pcg = ctx.datastack_info["segmentation_source"].split("/")[-1]
    try:
        return lv.lake_sql(view, definitions, lake_cols, pcg, lv.point_names(columns, lake_cols))
    except lv.ViewNotSupported as e:
        if "no lake" in str(e):
            raise ViewSkipped(str(e)) from None
        raise


def _copies(ctx, view: str, plan: lv.LakeView) -> List[Tuple[str, Optional[str], str]]:
    """(spec name, sort key, uri) per copy. The job's name_suffix is appended to each spec
    name, as the export does for a table's lakes, so a view built beside an earlier export
    of the version writes beside its lakes."""
    base = f"{ctx.settings.output_bucket}/{ctx.datastack}/v{ctx.version}/{view}"
    suffix = getattr(ctx, "name_suffix", None) or ""
    return [(f"{key or FLAT}{suffix}", key, f"{base}/{key or FLAT}{suffix}") for key in copy_keys(plan.columns)]


def _check_targets(ctx, view: str, copies, job_id: str) -> None:
    """Refuse to replace a lake another export wrote: delete it first, as for tables."""
    from deltalake import DeltaTable

    from materializationengine import deltalake_catalog as cat

    rows = {r["spec_name"]: r for r in cat.list_exports(ctx.aligned_volume, ctx.datastack, ctx.version,
                                                         table_name=view, statuses=None)}
    for spec_name, _, uri in copies:
        if DeltaTable.is_deltatable(uri) and (rows.get(spec_name) or {}).get("job_id") != job_id:
            raise RuntimeError(f"view {view}: {uri} already holds a lake another export wrote; "
                               "delete it before building the view again")


def export_view(ctx, view: str, plan: lv.LakeView, lakes: Dict[str, Dict], attempts: int = 3,
                row_group_rows: int = 10000, retry_wait: float = 30.0) -> Dict:
    """Build one view's copies into lakes and record them; its summary (raises on failure)."""
    from materializationengine import catalog_service
    from materializationengine import deltalake_catalog as cat
    from materializationengine.blueprints.client import lake_index

    copies = _copies(ctx, view, plan)
    job_id = _view_job_id(ctx.job_id, view)
    _check_targets(ctx, view, copies, job_id)
    catalog_lakes = [{"spec_name": s, "uri": u, "partition_by": k,
                      "spec": {"name": s, "partition_by": k, "sorted_by": k, "view": True, "engine": "daft",
                               "row_group_rows": row_group_rows, "reads": sorted(plan.lakes)}}
                     for s, k, u in copies]
    av, ds, v = ctx.aligned_volume, ctx.datastack, ctx.version
    cat.record_writing(av, ds, v, view, catalog_lakes, job_id=job_id)

    failed, cpus, started = [], _cluster_cpus(), time.perf_counter()
    built = None
    for attempt in range(1, max(1, attempts) + 1):
        try:
            _clear_tables([u for _, _, u in copies])  # this job's, by _check_targets
            built = build_view(plan, {t: lakes[t]["uri"] for t in plan.lakes}, copies, row_group_rows)
            break
        except Exception as e:
            logger.exception("view %s: attempt %d of %d failed", view, attempt, attempts)
            failed.append({"attempt": attempt, "error": f"{type(e).__name__}: {e}"[:2000]})
            if attempt < attempts:
                _wait_for_cluster(cpus, retry_wait)
    if built is None:
        cat.safely(cat.record_failed, av, ds, v, view, [s for s, _, _ in copies], failed[-1]["error"])
        raise RuntimeError(f"view {view}: {failed[-1]['error']}")

    rows = cat.lake_facts(copies[0][2])["row_count"]
    bad = cat.record_finished(av, ds, v, view, catalog_lakes, expected_rows=rows)
    if bad:
        raise RuntimeError(f"view {view}: copies {bad} disagree with the first copy's {rows} rows")
    orphans = sum(cat.safely(_remove_uncommitted, u) or 0 for _, _, u in copies)
    for lake in catalog_lakes:
        try:
            lake_index.build_and_store(lake["uri"], lake_index.key_columns(lake))
        except Exception:
            logger.warning("view %s: no planning index for %s", view, lake["uri"], exc_info=True)
    if catalog_service.enabled():
        cat.safely(catalog_service.register_version, av, ds, v, view)
    return {"state": "complete", "rows": rows, "copies": built, "reads": sorted(plan.lakes),
            "seconds": round(time.perf_counter() - started, 3), "failed_attempts": failed,
            "orphans_removed": orphans}


def _clear_tables(uris: List[str]) -> None:
    """Before an attempt: a lake an earlier attempt committed (it failed on a later copy) is
    removed, so every copy of a view comes from one attempt."""
    from deltalake import DeltaTable

    from materializationengine.workflows.deltalake_expiry import remove_lake_files

    for uri in uris:
        if DeltaTable.is_deltatable(uri):
            remove_lake_files(uri)


# --- the stage -------------------------------------------------------------------------------


def build_views(ctx) -> Dict:
    """DELTALAKE_EXPORT_VIEWS_STAGE: build the version's views from its table lakes."""
    import daft

    from materializationengine.database import db_manager
    from materializationengine.utils import get_config_param

    if not ctx.aligned_volume:
        raise ValueError("no aligned volume: view lakes can't be recorded in the catalog")
    engine = db_manager.get_engine(ctx.aligned_volume)
    definitions, columns = view_definitions(engine)
    views = _views_to_build(ctx.datastack, registered_views(engine, ctx.datastack))
    lakes = table_lakes((r for r in ctx.lakes if r["table_name"] not in definitions), job_id=ctx.job_id)
    attempts = int(get_config_param("DELTALAKE_VIEW_ATTEMPTS", 3))
    row_group_rows = int(get_config_param("DELTALAKE_VIEW_ROW_GROUP_ROWS", 10000))
    try:
        import ray

        if ray.is_initialized():  # the export job's cluster, which its driver has joined
            daft.set_runner_ray()
    except Exception:
        logger.info("Daft's runner is already set", exc_info=True)

    built, skipped, failed = {}, {}, {}
    for view in views:
        if view not in definitions:
            skipped[view] = "no such view in the live database"
            continue
        try:
            plan = plan_view(ctx, view, definitions, columns, lakes)
            done = _complete_specs(ctx.lakes, view, _view_job_id(ctx.job_id, view))
            if done and done == {s for s, _, _ in _copies(ctx, view, plan)}:
                skipped[view] = "already complete for this job"
                continue
            built[view] = export_view(ctx, view, plan, lakes, attempts, row_group_rows)
            logger.info("view %s: %s", view, json.dumps(built[view], default=str))
        except ViewSkipped as e:
            skipped[view] = str(e)
        except Exception as e:
            logger.exception("view %s failed", view)
            failed[view] = f"{type(e).__name__}: {e}"[:2000]
    detail = {"built": built, "skipped": skipped, "failed": failed}
    if failed:
        return {"state": "failed", "error": f"{len(failed)} view(s) failed: {sorted(failed)}", "detail": detail}
    return {"state": "done", "detail": detail}
