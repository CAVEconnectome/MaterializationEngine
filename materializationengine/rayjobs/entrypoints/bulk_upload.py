"""A bulk upload as one RayJob: the driver runs every stage, workers the parallel parts.

Usage::

    python -m materializationengine.rayjobs.entrypoints.bulk_upload --job-id <job id>

The API creates the job in the upload state database (upload_state.store.create_job)
with everything this needs in its params, then submits the RayJob. All state lives in
that database, none in redis, so a driver that dies (a lost head pod; KubeRay then
runs the entrypoint again on a new cluster) resumes where it stopped:

=================  ===========================================  =========================
stage              work                                         on resume
=================  ===========================================  =========================
process_source     CSV byte ranges / parquet parts on workers,  only parts not yet done
                   then composed into one CSV                   run again
upload_to_db       create the staging table, Cloud SQL import,  waits on (or adopts) the
                   annotation indexes                           recorded import
spatial_init       materialization info at the frozen time      the stored parameters
                   stamp, segmentation table, chunking          are used
spatial_chunks     one worker task per spatial chunk            completed chunks are
                                                                never run again
spatial_finalize   mark the lookup complete                     idempotent
cluster            CLUSTER both staging tables by id, unless    waits for an orphaned
                   one is in id order already                   CLUSTER first
transfer           Cloud SQL import into production (from the   waits on, or skips, the
                   processed CSV; the segmentation table        recorded exports and
                   exported from staging first)                 imports
cleanup            drop staging, delete the processed and       idempotent
                   exported CSVs,
                   mark the job done
=================  ===========================================  =========================

The stages are the celery tasks' own bodies (process_csv, upload_to_database,
cluster_staging_tables, transfer_to_production), run with a StoreSink so their status
updates, cancellation checks and import state go to the database. Only the driver
writes the job's state; workers return results.

Failures: a stage that fails MAX_STAGE_ATTEMPTS times, or with an error that will
not go away (PermanentUploadError, ValueError), fails the job for good: status error,
retryable false, staging discarded. Anything else exits 1 without a terminal status,
so KubeRay's retry (backoffLimit) resumes. A cancel stops before the transfer starts,
discards staging and exits 0; once the transfer has started it is left to finish.
"""

import argparse
import datetime
import json
import logging
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("ray.bulk_upload")

HEARTBEAT_SECONDS = 20
# How long a new driver waits for the lease of one that has just died to go stale.
LEASE_WAIT_SECONDS = 150
MAX_STAGE_ATTEMPTS = 3
# Attempts at one spatial chunk before it is FAILED_PERMANENT (as the celery path).
MAX_CHUNK_ATTEMPTS = 10
# This many different chunks failing, with the same error and none succeeding, is not
# bad luck with chunks but a broken setup (a missing token, a wrong Bigtable instance):
# the stage stops instead of trying every chunk MAX_CHUNK_ATTEMPTS times.
SYSTEMIC_FAILURE_CHUNKS = 20
FLUSH_SECONDS = 2.0
DEFAULT_MAX_IN_FLIGHT = 32

# Jobs that build in the production database (params["build_in_production"], every
# job started now): the tables are built there under temporary names and published by
# renaming them (blueprints/upload/production_build.py).
PRODUCTION_STAGE_ORDER = [
    "process_source",
    "load_annotations",
    "spatial_init",
    "spatial_chunks",
    "finish_tables",
    "publish",
    "cleanup",
]
# Jobs started before that, which build in staging and copy into production.
STAGE_ORDER = [
    "process_source",
    "upload_to_db",
    "spatial_init",
    "spatial_chunks",
    "spatial_finalize",
    "cluster",
    "transfer",
    "cleanup",
]
# Cancelling is no longer possible from here on: the stage changes production.
UNCANCELLABLE_FROM = {"production": "publish", "staging": "transfer"}


def builds_in_production(params: dict) -> bool:
    return bool(params.get("build_in_production"))


def stage_order(params: dict) -> List[str]:
    return PRODUCTION_STAGE_ORDER if builds_in_production(params) else STAGE_ORDER


class PermanentUploadError(RuntimeError):
    """A failure that a retry would only repeat."""


class SystemicChunkFailure(RuntimeError):
    """Every spatial chunk tried so far failed the same way."""


class LeaseLost(RuntimeError):
    """Another driver took over this job; this one must stop without writing."""


class Cancelled(RuntimeError):
    pass


def _jsonable(value):
    return json.loads(json.dumps(value, default=str))


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass
class Context:
    engine: Any  # the upload state database
    job_id: str
    epoch: int
    params: Dict[str, Any]
    runner: Any
    max_in_flight: int = DEFAULT_MAX_IN_FLIGHT
    results: Dict[str, Any] = field(default_factory=dict)
    cancel: threading.Event = field(default_factory=threading.Event)
    lost: threading.Event = field(default_factory=threading.Event)
    cancellable: bool = True
    fault: Optional[str] = None
    gate: Optional["ProductionGate"] = None

    @property
    def table_name(self) -> str:
        return self.params["file_metadata"]["metadata"]["table_name"]

    def wait_turn(self) -> None:
        """Before starting work in the production database: give way to a frozen
        materialization that is waiting to copy it."""
        if self.gate is not None:
            self.gate.wait_turn()

    def should_stop(self) -> bool:
        return self.lost.is_set() or (self.cancellable and self.cancel.is_set())

    def check(self) -> None:
        if self.lost.is_set():
            raise LeaseLost(f"lost the lease on {self.job_id}")
        if self.cancellable and self.cancel.is_set():
            raise Cancelled(f"upload {self.job_id} cancelled")

    def maybe_fault(self, point: str) -> None:
        """Testing only (UPLOAD_FAULT_INJECT, honoured when the config enables it):
        die the way a lost head pod does, at a named point."""
        if self.fault and self.fault == point:
            logger.warning(f"fault injection: exiting at {point}")
            os._exit(17)


# --- heartbeat ----------------------------------------------------------------------


class Heartbeat(threading.Thread):
    """Renews the lease and watches for a cancel request."""

    def __init__(self, ctx: Context, interval: float = HEARTBEAT_SECONDS):
        super().__init__(daemon=True, name="upload-heartbeat")
        self.ctx = ctx
        self.interval = interval
        self._stop = threading.Event()
        self._last_ok = time.monotonic()

    def run(self):
        from materializationengine.upload_state import store

        while not self._stop.wait(self.interval):
            try:
                owner, cancel = store.heartbeat(self.ctx.engine, self.ctx.job_id, self.ctx.epoch)
            except Exception as e:
                logger.warning(f"heartbeat failed: {e}")
                if time.monotonic() - self._last_ok > store.LEASE_STALE_SECONDS:
                    # another driver may have taken the lease by now
                    self.ctx.lost.set()
                continue
            self._last_ok = time.monotonic()
            if not owner:
                self.ctx.lost.set()
            if cancel:
                self.ctx.cancel.set()

    def stop(self):
        self._stop.set()


class ProductionGate:
    """The driver's side of upload_state/clone_gate.py, for a job building in the
    production (aligned volume) database.

    The job holds the database while it may have work running there. When a frozen
    materialization asks to copy it, the driver starts nothing new there, lets what is
    running finish (run_bounded drains the chunks in flight), closes its connections
    and stops holding it; once the request is released it holds it again and goes on.
    """

    def __init__(self, ctx: Context, aligned_volume: str, poll_seconds: float = 10):
        self.ctx = ctx
        self.aligned_volume = aligned_volume
        self.poll_seconds = poll_seconds

    def _set(self, holding: bool) -> None:
        from materializationengine.upload_state import store

        store.set_holding(self.ctx.engine, self.ctx.job_id, self.aligned_volume, holding)

    def start(self) -> None:
        self._set(True)

    def stop(self) -> None:
        try:
            self._set(False)
        except Exception as e:
            logger.warning(f"could not release {self.aligned_volume}: {e}")

    def requested(self) -> bool:
        from materializationengine.upload_state import store

        try:
            return store.clone_requested(self.ctx.engine, self.aligned_volume)
        except Exception as e:
            logger.warning(f"could not check for a frozen materialization: {e}")
            return False

    def wait_turn(self) -> None:
        """Call with no work of this job running in the production database."""
        if not self.requested():
            return
        from materializationengine.database import db_manager, dynamic_annotation_cache

        logger.info(f"a frozen materialization of {self.aligned_volume} is waiting; releasing it")
        while True:
            db_manager.cleanup()
            dynamic_annotation_cache.invalidate_cache()
            self._set(False)
            while self.requested():
                self.ctx.check()
                time.sleep(self.poll_seconds)
            self._set(True)
            # a request made between the last check and holding again saw no holder
            # and may be copying now: wait for it too
            if not self.requested():
                break
        logger.info(f"{self.aligned_volume} copied; carrying on")


# --- the part runner: process_csv's parts on workers ----------------------------------


def make_part_runner(ctx: Context) -> Callable:
    """A _run_csv_part_tasks replacement: runs the part signatures on the runner, records
    each finished processing part, and returns the recorded result for parts already
    done by an earlier attempt."""
    from materializationengine.blueprints.upload.tasks import update_job_status
    from materializationengine.rayjobs.runner import run_bounded
    from materializationengine.rayjobs.upload_tasks import CELERY_PART_TASKS
    from materializationengine.upload_state import store

    def runner(signatures: list, job_id: str, phase: str) -> List[Any]:
        names = {CELERY_PART_TASKS[s.task] for s in signatures}
        if len(names) != 1:
            raise ValueError(f"mixed part tasks {names}")
        task = names.pop()
        calls = []
        for s in signatures:
            if task == "count_csv_part":
                calls.append(tuple(s.args))
            else:
                calls.append((dict(s.kwargs),))
        results: List[Any] = [None] * len(calls)

        recorded = task != "count_csv_part"  # counting is quick: just redo it
        todo = list(range(len(calls)))
        if recorded:
            kind = "csv" if task == "csv_part" else "parquet"
            store.plan_parts(
                ctx.engine,
                ctx.job_id,
                [
                    {
                        "part_index": i,
                        "kind": kind,
                        "spec": _jsonable(args[0]),
                        "output_blob": args[0]["destination_blob_name"],
                        "id_counter_start": args[0].get("id_counter_start"),
                    }
                    for i, args in enumerate(calls)
                ],
            )
            todo = []
            for part in store.get_parts(ctx.engine, ctx.job_id):
                i = part["part_index"]
                if i >= len(calls):
                    continue
                if part["spec"] != _jsonable(calls[i][0]):
                    raise PermanentUploadError(
                        f"part {i} of {ctx.job_id} was planned differently by an earlier attempt"
                    )
                if part["status"] == "done":
                    results[i] = {
                        "rows_read": part["rows_read"] or 0,
                        "rows_written": part["rows_written"] or 0,
                        "dropped_rows": part["dropped_rows"] or 0,
                        **({"split_error": part["split_error"]} if part["split_error"] else {}),
                    }
                else:
                    todo.append(i)
            if len(todo) < len(calls):
                logger.info(f"{phase}: {len(calls) - len(todo)} of {len(calls)} parts already done")

        done_count = [len(calls) - len(todo)]
        last_flush = [0.0]

        def on_done(i, result):
            results[i] = result
            done_count[0] += 1
            if recorded:
                store.update_parts(
                    ctx.engine,
                    ctx.job_id,
                    [{
                        "part_index": i,
                        "status": "done",
                        "rows_read": result.get("rows_read"),
                        "rows_written": result.get("rows_written"),
                        "dropped_rows": result.get("dropped_rows"),
                        "split_error": result.get("split_error"),
                    }],
                )

        def on_tick(force=False):
            if not force and time.monotonic() - last_flush[0] < FLUSH_SECONDS:
                return
            last_flush[0] = time.monotonic()
            rows = sum(r.get("rows_read", 0) for r in results if isinstance(r, dict))
            update_job_status(
                job_id,
                {
                    "status": "processing",
                    "phase": phase,
                    "progress": round(100.0 * done_count[0] / max(len(calls), 1), 2),
                    "processed_rows": rows,
                    "current_chunk_num": done_count[0],
                    "total_chunks": len(calls),
                },
            )

        run_bounded(
            ctx.runner,
            task,
            ((i, calls[i]) for i in todo),
            ctx.max_in_flight,
            on_done,
            should_stop=ctx.should_stop,
            on_tick=on_tick,
        )
        on_tick(force=True)
        return results

    return runner


# --- stages -------------------------------------------------------------------------


def _staging_database() -> str:
    from materializationengine.utils import get_config_param

    name = get_config_param("STAGING_DATABASE_NAME")
    if not name:
        raise PermanentUploadError("STAGING_DATABASE_NAME is not configured")
    return name


def stage_process_source(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.tasks import process_csv
    from materializationengine.upload_state.status_sink import use_part_runner

    p = ctx.params
    meta = p["file_metadata"]["metadata"]
    with use_part_runner(make_part_runner(ctx)):
        return process_csv.run(
            file_path=p["file_path"],
            schema_type=meta["schema_type"],
            column_mapping=p["file_metadata"]["column_mapping"],
            job_id_for_status=ctx.job_id,
            reference_table=meta.get("reference_table"),
            ignored_columns=p["file_metadata"].get("ignored_columns"),
            timestamp=p["created_ts"],
        )


def stage_upload_to_db(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.tasks import upload_to_database

    p = ctx.params
    return upload_to_database.run(
        ctx.results["process_source"],
        sql_instance_name=p["sql_instance_name"],
        file_metadata=p["file_metadata"],
        datastack_info=p["datastack_info"],
        job_id=ctx.job_id,
    )


def stage_spatial_init(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.tasks import update_job_status
    from materializationengine.database import db_manager
    from materializationengine.index_manager import index_cache
    from materializationengine.upload_state import store
    from materializationengine.utils import get_config_param
    from materializationengine.workflows.chunking import (
        DEFAULT_TARGET_ROWS_PER_CHUNK,
        ChunkingStrategy,
    )
    from materializationengine.workflows.spatial_lookup import (
        _total_point_estimate,
        create_missing_segmentation_table,
        get_materialization_info,
    )

    existing = store.get_spatial(ctx.engine, ctx.job_id)
    if existing:
        return {"total_chunks": existing["total_chunks"]}

    p = ctx.params
    datastack_info = p["datastack_info"]
    # frozen when the job was created: a resumed lookup asks for root ids as of the
    # same moment as the chunks it already did
    time_stamp = datetime.datetime.fromisoformat(p["materialization_time_stamp"])
    if builds_in_production(p):
        from materializationengine.blueprints.upload import production_build as pb

        # the temporary tables have no metadata rows until they are published
        database, engine = _production(ctx)
        table = p["build_tables"]["annotation"]
        meta = p["table_meta"]
        ctx.wait_turn()
        mat_metadata = get_materialization_info(
            datastack_info=datastack_info,
            materialization_time_stamp=time_stamp,
            table_name=table,
            skip_row_count=True,
            database=database,
            annotation_metadata=meta,
        )
    else:
        database = _staging_database()
        engine = db_manager.get_engine(database)
        table = ctx.table_name
        mat_metadata = get_materialization_info(
            datastack_info=datastack_info,
            materialization_time_stamp=time_stamp,
            table_name=table,
            skip_row_count=True,
            database=database,
        )
    if not mat_metadata:
        raise PermanentUploadError(f"No materialization metadata for {table} in {database}")
    info = mat_metadata[0]
    if builds_in_production(p):
        seg_model = pb.segmentation_model(table, meta, _pcg_table_name(ctx))
        with engine.connect() as conn:
            exists = pb._table_exists(conn, info["segmentation_table_name"])
        if not exists:
            # the chunk upserts need the primary key; the rest is built after them
            pb.create_table(engine, seg_model, ctx.job_id, keep_primary_key=True)
    else:
        create_missing_segmentation_table(info)
        index_cache.drop_table_indices(info["segmentation_table_name"], engine, drop_primary_key=False)
    chunking = ChunkingStrategy(
        engine=engine,
        table_name=info["annotation_table_name"],
        database=database,
        base_chunk_size=int(p.get("chunk_scale_factor", 2)) * 1024,
        target_rows_per_chunk=int(
            get_config_param("SPATIAL_LOOKUP_TARGET_ROWS_PER_CHUNK", DEFAULT_TARGET_ROWS_PER_CHUNK)
        ),
        schema=info["schema"],
    )
    if not chunking.determine_bounds():
        # select_strategy would go on with one zero-volume chunk that finds nothing
        raise PermanentUploadError(f"Could not find the spatial bounds of {table} in {database}")
    chunking.select_strategy()
    row = store.init_spatial(
        ctx.engine,
        ctx.job_id,
        database_name=database,
        workflow_name=ctx.table_name,
        datastack_name=datastack_info.get("datastack"),
        status="processing_chunks",
        total_chunks=chunking.total_chunks,
        chunking_strategy=chunking.strategy_name,
        used_chunk_size=chunking.actual_chunk_size,
        chunking_parameters=_jsonable(chunking.to_dict()),
        min_enclosing_bbox=(
            [chunking.min_coords.tolist(), chunking.max_coords.tolist()]
            if chunking.min_coords is not None and chunking.max_coords is not None
            else None
        ),
        total_row_estimate=chunking.estimated_rows,
        total_point_estimate=_total_point_estimate(chunking.estimated_rows, info),
        mat_metadata=_jsonable(info),
    )
    update_job_status(
        ctx.job_id,
        {
            "status": "processing",
            "phase": "Spatial Lookup: processing_chunks",
            "progress": 0,
            "active_workflow_part": "spatial_lookup",
            "spatial_lookup_config": {
                "table_name": ctx.table_name,
                "database_name": database,
                "datastack_name": datastack_info.get("datastack", ""),
            },
        },
    )
    return {"total_chunks": row["total_chunks"]}


def stage_spatial_chunks(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.checkpoint_manager import (
        CHUNK_STATUS_COMPLETED,
        CHUNK_STATUS_FAILED_PERMANENT,
        CHUNK_STATUS_FAILED_RETRYABLE,
        _progress_estimate,
    )
    from materializationengine.blueprints.upload.tasks import update_job_status
    from materializationengine.rayjobs.runner import run_bounded
    from materializationengine.upload_state import store
    from materializationengine.workflows.chunking import reconstruct_chunk_bounds
    from materializationengine.workflows.spatial_lookup import DEFAULT_SUPERVOXEL_BATCH_SIZE

    spatial = store.get_spatial(ctx.engine, ctx.job_id)
    total = spatial["total_chunks"] or 0
    database = spatial["database_name"]
    info = spatial["mat_metadata"]
    params = spatial["chunking_parameters"]
    sub_batch = int(ctx.params.get("supervoxel_batch_size") or DEFAULT_SUPERVOXEL_BATCH_SIZE)

    completed, permanent, attempts = store.chunk_state(ctx.engine, ctx.job_id)
    # the chunk rows are the authority; the counters on the spatial row follow them
    counts = store.chunk_totals(ctx.engine, ctx.job_id)
    if completed:
        logger.info(f"spatial lookup: {len(completed)} of {total} chunks already done")

    buffer: List[dict] = []
    last_flush = [0.0]

    def items():
        for i in range(total):
            if i in completed or i in permanent:
                continue
            min_corner, max_corner = reconstruct_chunk_bounds(i, params)
            yield i, (database, info, min_corner, max_corner, sub_batch)

    def on_done(i, result):
        attempts[i] = attempts.get(i, 0) + 1
        counts["completed_chunks"] += 1
        counts["rows_processed"] += int(result.get("points", 0))
        buffer.append({
            "chunk_index": i,
            "status": CHUNK_STATUS_COMPLETED,
            "attempt_count": attempts[i],
            "rows_processed": result.get("rows"),
            "points": result.get("points"),
        })
        ctx.maybe_fault(f"spatial_chunk:{counts['completed_chunks']}")

    succeeded_this_run = [0]
    failed_this_run: Dict[int, str] = {}

    def on_done_counted(i, result):
        succeeded_this_run[0] += 1
        on_done(i, result)

    def on_error(i, exc) -> bool:
        attempts[i] = attempts.get(i, 0) + 1
        final = attempts[i] >= MAX_CHUNK_ATTEMPTS
        error_type = getattr(exc, "error_type", None) or type(exc).__name__
        message = getattr(exc, "message", None) or str(exc)
        logger.warning(f"chunk {i} failed (attempt {attempts[i]}): {error_type}: {message}")
        if final:
            counts["failed_chunks"] += 1
        buffer.append({
            "chunk_index": i,
            "status": CHUNK_STATUS_FAILED_PERMANENT if final else CHUNK_STATUS_FAILED_RETRYABLE,
            "attempt_count": attempts[i],
            "error_message": message,
            "error_type": error_type,
        })
        failed_this_run[i] = error_type
        if (
            not succeeded_this_run[0]
            and len(failed_this_run) >= SYSTEMIC_FAILURE_CHUNKS
            and len(set(failed_this_run.values())) == 1
        ):
            raise SystemicChunkFailure(
                f"the first {len(failed_this_run)} spatial chunks all failed and none "
                f"succeeded: {error_type}: {message}"
            )
        return not final

    def flush(force=False):
        if not force and time.monotonic() - last_flush[0] < FLUSH_SECONDS:
            return
        last_flush[0] = time.monotonic()
        if ctx.lost.is_set():
            return
        if buffer:
            store.record_chunk_results(ctx.engine, ctx.job_id, list(buffer))
            buffer.clear()
        row = store.get_spatial(ctx.engine, ctx.job_id)
        estimate = _progress_estimate(store.as_workflow_data(row), counts["rows_processed"], _now_iso())
        store.update_spatial(
            ctx.engine,
            ctx.job_id,
            completed_chunks=counts["completed_chunks"],
            failed_chunks=counts["failed_chunks"],
            rows_processed=counts["rows_processed"],
            **estimate,
        )
        progress = 100.0 * counts["completed_chunks"] / total if total else 100.0
        update_job_status(
            ctx.job_id,
            {
                "status": "processing",
                "phase": (
                    f"Spatial Lookup: processing_chunks "
                    f"({counts['completed_chunks']}/{total} chunks)"
                ),
                "progress": round(progress, 2),
                "active_workflow_part": "spatial_lookup",
                "total_rows": total,
                "processed_rows": counts["completed_chunks"],
            },
        )

    try:
        run_bounded(
            ctx.runner,
            "spatial_chunk",
            items(),
            ctx.max_in_flight,
            on_done_counted,
            on_error=on_error,
            should_stop=ctx.should_stop,
            on_tick=flush,
            should_pause=ctx.gate.requested if ctx.gate else None,
            on_paused=ctx.wait_turn,
        )
    finally:
        # keep what finished, whatever stopped the loop
        try:
            flush(force=True)
        except Exception as e:
            logger.warning(f"could not record the last chunk results: {e}")

    if counts["failed_chunks"]:
        store.update_spatial(
            ctx.engine, ctx.job_id, status="failed",
            last_error=f"{counts['failed_chunks']} chunk(s) failed permanently",
        )
        raise PermanentUploadError(
            f"{counts['failed_chunks']} spatial chunk(s) failed {MAX_CHUNK_ATTEMPTS} times; "
            f"see upload_spatial_chunks for {ctx.job_id}"
        )
    return dict(counts)


def stage_spatial_finalize(ctx: Context) -> dict:
    """The lookup is complete. The staging segmentation table keeps only its primary key
    (as on the celery path, whose index rebuild never ran): the transfer builds the
    indexes in production, so building them in staging too would be wasted."""
    from materializationengine.blueprints.upload.checkpoint_manager import CHUNK_STATUS_COMPLETED
    from materializationengine.blueprints.upload.tasks import update_job_status
    from materializationengine.upload_state import store

    store.update_spatial(ctx.engine, ctx.job_id, status=CHUNK_STATUS_COMPLETED)
    update_job_status(
        ctx.job_id,
        {
            "status": "processing",
            "phase": "Spatial Lookup Complete, Preparing Transfer",
            "progress": 0,
            "active_workflow_part": "transfer",
        },
    )
    return {"status": CHUNK_STATUS_COMPLETED}


def _monitor_result(ctx: Context) -> dict:
    """What cluster_staging_tables and transfer_to_production take from the celery
    chain's monitor task."""
    return {
        "datastack_info": ctx.params["datastack_info"],
        "table_name": ctx.table_name,
        "materialization_time_stamp": ctx.params["materialization_time_stamp"],
        "spatial_workflow_final_status": "COMPLETED",
        "job_id_for_status": ctx.job_id,
    }


def stage_cluster(ctx: Context) -> dict:
    from dynamicannotationdb.key_utils import build_segmentation_table_name

    from materializationengine.blueprints.upload.tasks import (
        cluster_staging_tables,
        wait_for_backend_work,
    )
    from materializationengine.database import db_manager

    engine = db_manager.get_engine(_staging_database())
    pcg_table_name = ctx.params["datastack_info"]["segmentation_source"].split("/")[-1]
    for table in (ctx.table_name, build_segmentation_table_name(ctx.table_name, pcg_table_name)):
        # a dead driver's CLUSTER runs on to the end in its backend
        wait_for_backend_work(engine, table, ("CLUSTER", "CREATE INDEX"))
    cluster_staging_tables.run(_monitor_result(ctx))
    return {"clustered": True}


def stage_transfer(ctx: Context) -> dict:
    """Through GCS, with Cloud SQL exports and imports: the annotation table from the
    processed CSV it was imported from, the segmentation table exported from staging."""
    from materializationengine.blueprints.upload.tasks import transfer_to_production

    monitor_result = _monitor_result(ctx)
    output_path = (ctx.results.get("process_source") or {}).get("output_path")
    if output_path:
        monitor_result["annotation_csv_uri"] = f"gs://{output_path}"
    return transfer_to_production.run(
        monitor_result, transfer_segmentation=True, method="cloudsql"
    )


def stage_cleanup(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.gcs_processor import GCSCsvProcessor
    from materializationengine.blueprints.upload.tasks import _discard_staging, update_job_status

    dropped = _discard_staging(ctx.table_name, ctx.job_id, "completed")
    output_path = (ctx.results.get("process_source") or {}).get("output_path") or ""
    exported = [
        uri[len("gs://"):]
        for uri in (ctx.results.get("transfer") or {}).get("exported_csvs") or []
    ]
    # the processed CSV, and the CSVs the transfer exported staging tables to
    for path in ([output_path] if output_path else []) + exported:
        bucket, _, blob = path.partition("/")
        if not (bucket and blob):
            continue
        try:
            GCSCsvProcessor(bucket).delete_blobs([blob])
        except Exception as e:
            logger.warning(f"could not delete gs://{path}: {e}")
    update_job_status(ctx.job_id, {"status": "done", "phase": "Transfer Complete", "progress": 100})
    return {"staging": dropped.get("status")}


# --- stages of a job building in the production database --------------------------


def _production(ctx: Context):
    """The aligned volume database the job builds in, and its engine."""
    from materializationengine.database import db_manager

    database = ctx.params["datastack_info"]["aligned_volume"]["name"]
    return database, db_manager.get_engine(database)


def _pcg_table_name(ctx: Context) -> str:
    return ctx.params["datastack_info"]["segmentation_source"].split("/")[-1]


def _run_index_commands(ctx: Context, engine, table: str, commands: List[str], label: str) -> None:
    """Build indexes one at a time, giving way to a frozen materialization between them.
    A dead attempt's CREATE INDEX runs on in its backend, so wait for it first; the
    IF NOT EXISTS then skips it."""
    from materializationengine.blueprints.upload.tasks import (
        update_job_status,
        upload_index_maintenance_work_mem,
        wait_for_backend_work,
    )
    from sqlalchemy import text

    for i, command in enumerate(commands):
        ctx.check()
        ctx.wait_turn()
        wait_for_backend_work(engine, table, ("CREATE INDEX", "CLUSTER", "ALTER TABLE"))
        update_job_status(
            ctx.job_id,
            {"status": "processing", "phase": f"{label}: Adding Index {i + 1}/{len(commands)}"},
        )
        logger.info(f"{table}: {command}")
        with engine.begin() as conn:
            mem = upload_index_maintenance_work_mem()
            if mem:
                conn.execute(text(f"SET LOCAL maintenance_work_mem TO '{mem}'"))
            conn.execute(text(command))


def _cluster_unless_in_id_order(ctx: Context, engine, table: str, label: str) -> Tuple[bool, Optional[float]]:
    """CLUSTER table on its primary key unless it is already in id order (the planner's
    id correlation, after ANALYZE, at or above UPLOAD_CLUSTER_SKIP_CORRELATION).
    Returns (clustered, correlation)."""
    from materializationengine.blueprints.upload.tasks import (
        DEFAULT_CLUSTER_SKIP_CORRELATION,
        cluster_table_by_id,
        id_order_correlation,
        update_job_status,
        wait_for_backend_work,
    )
    from materializationengine.utils import get_config_param

    skip_above = float(
        get_config_param("UPLOAD_CLUSTER_SKIP_CORRELATION") or DEFAULT_CLUSTER_SKIP_CORRELATION
    )
    ctx.wait_turn()
    # a dead attempt's CLUSTER runs on in its backend; once it is done the table is in order
    wait_for_backend_work(engine, table, ("CLUSTER", "CREATE INDEX"))
    correlation = id_order_correlation(table, engine)
    if correlation is not None and correlation >= skip_above:
        logger.info(f"{table}: already in id order (correlation {correlation:.4f})")
        return False, correlation
    update_job_status(ctx.job_id, {"status": "processing", "phase": f"Clustering {label} by id"})
    ctx.wait_turn()
    logger.info(f"{table}: CLUSTER by id (correlation {correlation})")
    return cluster_table_by_id(table, engine), correlation


def stage_load_annotations(ctx: Context) -> dict:
    """The temporary annotation table, the processed CSV imported into it, its indexes.

    The CSV is in id order when the upload numbers the rows, but in the source file's
    order when the file has its own ids (the minnie synapse files' are unsorted: the
    1M ltv7 test table came out with id correlation 0.0001). Such a table is clustered
    on its primary key before the other indexes are built, so the CLUSTER rewrites one
    index instead of six (the staging CLUSTER of all six took 6h13m on 337M rows).
    """
    from sqlalchemy import text

    from materializationengine.blueprints.upload import production_build as pb
    from materializationengine.blueprints.upload.tasks import (
        DEFAULT_SQL_IMPORT_TIMEOUT_SECONDS,
        _run_cloudsql_operation,
        _table_columns,
        import_csv,
        update_job_status,
    )
    from materializationengine.utils import get_config_param

    names = ctx.params["build_tables"]
    meta = ctx.params["table_meta"]
    temp = names["annotation"]
    database, engine = _production(ctx)
    quote = engine.dialect.identifier_preparer.quote
    model = pb.annotation_model(temp, meta)

    ctx.wait_turn()
    taken = pb.names_in_use(engine, names, job_id=ctx.job_id)
    if taken:
        raise PermanentUploadError(f"{taken} already exist in {database}")
    with engine.connect() as conn:
        exists = pb._table_exists(conn, temp)
    if not exists:
        # without its indexes and primary key for the bulk load; they are built after
        pb.create_table(engine, model, ctx.job_id, keep_primary_key=False)

    output_path = ctx.results["process_source"]["output_path"]
    uri = f"gs://{output_path}"
    sql_instance_name = ctx.params["sql_instance_name"]
    timeout = float(get_config_param("SQL_IMPORT_TIMEOUT_SECONDS") or DEFAULT_SQL_IMPORT_TIMEOUT_SECONDS)

    def truncate():
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {quote(temp)}"))

    update_job_status(
        ctx.job_id,
        {"status": "processing", "phase": "Uploading to Database: Importing CSV into Cloud SQL", "progress": 30},
    )
    ctx.wait_turn()
    columns = _table_columns(temp, engine)
    _run_cloudsql_operation(
        ctx.job_id,
        "import",
        "IMPORT",
        uri,
        database,
        temp,
        start=lambda on_started: import_csv(
            sql_instance_name, uri, database=database, table=temp, user="postgres",
            timeout=timeout, on_started=on_started, columns=columns,
        ),
        prepare=truncate,
    )
    primary_key = [c for c in pb.index_commands(engine, temp, model) if "primary key" in c.lower()]
    _run_index_commands(ctx, engine, temp, primary_key, "Uploading to Database")
    clustered, correlation = _cluster_unless_in_id_order(ctx, engine, temp, "Annotation Table")
    commands = pb.index_commands(engine, temp, model)
    _run_index_commands(ctx, engine, temp, commands, "Uploading to Database")
    ctx.wait_turn()
    with engine.begin() as conn:
        conn.execute(text(f"ANALYZE {quote(temp)}"))
    update_job_status(
        ctx.job_id,
        {"status": "processing", "phase": "Database Upload Complete, awaiting next step", "progress": 100},
    )
    return {
        "table": temp,
        "indexes": len(primary_key) + len(commands),
        "clustered": clustered,
        "correlation": correlation,
    }


def stage_finish_tables(ctx: Context) -> dict:
    """The segmentation table into id order (the spatial lookup wrote it in chunk
    order) and its indexes; statistics for both tables."""
    from sqlalchemy import text

    from materializationengine.blueprints.upload import production_build as pb
    from materializationengine.blueprints.upload.tasks import mark_clustered_on_primary_key

    names = ctx.params["build_tables"]
    meta = ctx.params["table_meta"]
    database, engine = _production(ctx)
    quote = engine.dialect.identifier_preparer.quote
    seg = names["segmentation"]

    clustered, correlation = _cluster_unless_in_id_order(ctx, engine, seg, "Segmentation Table")
    seg_model = pb.segmentation_model(names["annotation"], meta, _pcg_table_name(ctx))
    commands = pb.index_commands(engine, seg, seg_model)
    _run_index_commands(ctx, engine, seg, commands, "Segmentation Table")
    ctx.wait_turn()
    with engine.begin() as conn:
        for table in (names["annotation"], seg):
            conn.execute(text(f"ANALYZE {quote(table)}"))
    for table in (names["annotation"], seg):
        mark_clustered_on_primary_key(table, engine)
    return {"clustered": clustered, "correlation": correlation, "indexes": len(commands)}


def stage_publish(ctx: Context) -> dict:
    from materializationengine.blueprints.upload import production_build as pb
    from materializationengine.blueprints.upload.tasks import update_job_status
    from materializationengine.database import dynamic_annotation_cache

    names = ctx.params["build_tables"]
    database, engine = _production(ctx)
    update_job_status(
        ctx.job_id, {"status": "processing", "phase": "Publishing to Production", "progress": 0}
    )
    ctx.wait_turn()
    outcome = pb.publish(
        engine,
        ctx.job_id,
        names,
        ctx.params["table_meta"],
        _pcg_table_name(ctx),
        created=_naive_utc(ctx.params["created_ts"]),
        last_updated=_naive_utc(ctx.params["materialization_time_stamp"]),
    )
    dynamic_annotation_cache.invalidate_cache()
    return {"publish": outcome, "table": names["final_annotation"]}


def _naive_utc(value: str) -> datetime.datetime:
    """A stored timestamp as the naive UTC datetime the metadata columns hold."""
    ts = datetime.datetime.fromisoformat(value)
    if ts.tzinfo is not None:
        ts = ts.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return ts


def stage_cleanup_production(ctx: Context) -> dict:
    from materializationengine.blueprints.upload.gcs_processor import GCSCsvProcessor
    from materializationengine.blueprints.upload.tasks import update_job_status

    output_path = (ctx.results.get("process_source") or {}).get("output_path") or ""
    bucket, _, blob = output_path.partition("/")
    if bucket and blob:
        try:
            GCSCsvProcessor(bucket).delete_blobs([blob])
        except Exception as e:
            logger.warning(f"could not delete gs://{output_path}: {e}")
    update_job_status(ctx.job_id, {"status": "done", "phase": "Transfer Complete", "progress": 100})
    return {"deleted": output_path}


STAGES: Dict[str, Callable[[Context], dict]] = {
    "process_source": stage_process_source,
    "upload_to_db": stage_upload_to_db,
    "spatial_init": stage_spatial_init,
    "spatial_chunks": stage_spatial_chunks,
    "spatial_finalize": stage_spatial_finalize,
    "cluster": stage_cluster,
    "transfer": stage_transfer,
    "cleanup": stage_cleanup,
}
assert list(STAGES) == STAGE_ORDER
PRODUCTION_STAGES: Dict[str, Callable[[Context], dict]] = {
    "process_source": stage_process_source,
    "load_annotations": stage_load_annotations,
    "spatial_init": stage_spatial_init,
    "spatial_chunks": stage_spatial_chunks,
    "finish_tables": stage_finish_tables,
    "publish": stage_publish,
    "cleanup": stage_cleanup_production,
}
assert list(PRODUCTION_STAGES) == PRODUCTION_STAGE_ORDER


# --- the driver ---------------------------------------------------------------------


def _is_permanent(exc: Exception, attempts: int) -> bool:
    from materializationengine.blueprints.upload.production_build import PublishConflict

    return (
        isinstance(exc, (PermanentUploadError, ValueError, PublishConflict))
        or attempts >= MAX_STAGE_ATTEMPTS
    )


def _keeps_built_tables(exc: Exception) -> bool:
    """A name conflict at publish leaves the finished tables for a person to publish."""
    from materializationengine.blueprints.upload.production_build import PublishConflict

    return isinstance(exc, PublishConflict)


def _discard(ctx: Context, reason: str) -> None:
    from materializationengine.blueprints.upload.tasks import _discard_staging, _note_on_job
    from materializationengine.upload_state.status_sink import StoreSink, use_sink

    try:
        with use_sink(StoreSink(ctx.engine, ctx.job_id, epoch=ctx.epoch)):
            if builds_in_production(ctx.params):
                from materializationengine.blueprints.upload import production_build as pb

                _, engine = _production(ctx)
                ctx.wait_turn()
                dropped = pb.drop_build_tables(engine, ctx.job_id, ctx.params["build_tables"])
                logger.info(f"dropped {dropped} ({reason} upload)")
                _note_on_job(ctx.job_id, staging_cleanup="done")
            else:
                _discard_staging(ctx.table_name, ctx.job_id, reason)
    except Exception as e:
        logger.warning(f"could not discard the built tables of {ctx.job_id}: {e}")


def acquire_lease(engine, job_id: str, host: str, wait_seconds: float = LEASE_WAIT_SECONDS) -> Optional[int]:
    from materializationengine.upload_state import store

    deadline = time.monotonic() + wait_seconds
    while True:
        epoch = store.acquire_lease(engine, job_id, host)
        if epoch is not None or time.monotonic() > deadline:
            return epoch
        logger.info(f"another driver holds {job_id}; waiting for its lease to lapse")
        time.sleep(10)


def run_job(
    engine,
    job_id: str,
    make_runner: Callable[[], Any],
    host: Optional[str] = None,
    max_in_flight: int = DEFAULT_MAX_IN_FLIGHT,
    fault: Optional[str] = None,
    lease_wait_seconds: float = LEASE_WAIT_SECONDS,
    heartbeat_seconds: float = HEARTBEAT_SECONDS,
) -> int:
    """Run (or resume) one upload; returns the process exit code."""
    from materializationengine.blueprints.upload.tasks import UploadCancelled
    from materializationengine.rayjobs.runner import Stopped
    from materializationengine.upload_state import store
    from materializationengine.upload_state.status_sink import StoreSink, use_sink

    job = store.get_job(engine, job_id)
    if job is None:
        logger.error(f"no upload job {job_id}")
        return 1
    stages = store.get_stages(engine, job_id)
    if job["status"] == "cancelled" or stages.get("cleanup", {}).get("status") == "done":
        logger.info(f"upload {job_id} is already {job['status']}; nothing to do")
        return 0
    if job["status"] == "error" and not job["retryable"]:
        logger.info(f"upload {job_id} failed for good earlier; not retrying")
        return 1

    epoch = acquire_lease(engine, job_id, host or socket.gethostname(), lease_wait_seconds)
    if epoch is None:
        logger.error(f"could not take the lease on {job_id}: another driver is still alive")
        return 1
    logger.info(f"driving upload {job_id} (epoch {epoch})")

    ctx = Context(
        engine=engine, job_id=job_id, epoch=epoch, params=job["params"],
        runner=make_runner(), max_in_flight=max_in_flight, fault=fault,
    )
    heartbeat = Heartbeat(ctx, heartbeat_seconds)
    heartbeat.start()
    order = stage_order(job["params"])
    stages_by_name = PRODUCTION_STAGES if builds_in_production(job["params"]) else STAGES
    uncancellable_from = UNCANCELLABLE_FROM[
        "production" if builds_in_production(job["params"]) else "staging"
    ]
    if builds_in_production(job["params"]):
        ctx.gate = ProductionGate(ctx, job["params"]["datastack_info"]["aligned_volume"]["name"])
        ctx.gate.start()
    stage = None
    try:
        if store.is_cancel_requested(engine, job_id):
            ctx.cancel.set()
        for stage in order:
            if stage == uncancellable_from:
                ctx.cancellable = False
            done = stages.get(stage)
            if done and done["status"] == "done":
                ctx.results[stage] = done["result"]
                continue
            ctx.check()
            # atomically with respect to a cancel request: once the transfer has been
            # entered a cancel is refused, and the transfer is not entered after one
            if not store.enter_stage(engine, job_id, stage, epoch, refuse_if_cancelled=not ctx.cancellable):
                if not store.heartbeat(engine, job_id, epoch)[0]:
                    raise LeaseLost(f"lost the lease on {job_id} before {stage}")
                ctx.cancellable = True
                ctx.cancel.set()
                raise Cancelled(f"upload {job_id} cancelled before {stage}")
            store.start_stage(engine, job_id, stage, epoch)
            logger.info(f"{job_id}: stage {stage}")
            with use_sink(StoreSink(engine, job_id, epoch=epoch, stage=stage)):
                result = stages_by_name[stage](ctx)
            ctx.check()
            if not store.finish_stage(engine, job_id, stage, epoch, _jsonable(result)):
                raise LeaseLost(f"lost the lease on {job_id} during {stage}")
            ctx.results[stage] = _jsonable(result)
            ctx.maybe_fault(f"after:{stage}")
        logger.info(f"upload {job_id} complete")
        return 0
    except LeaseLost as e:
        logger.warning(f"{e}; stopping without writing anything more")
        return 1
    except (Cancelled, UploadCancelled, Stopped) as e:
        if ctx.lost.is_set():
            logger.warning(f"lost the lease on {job_id}; stopping")
            return 1
        logger.info(f"upload {job_id} cancelled during {stage}: {e}")
        if stage:
            store.fail_stage(engine, job_id, stage, "cancelled")
        _discard(ctx, "cancelled")
        with use_sink(StoreSink(engine, job_id, epoch=epoch)):
            from materializationengine.blueprints.upload.tasks import update_job_status

            update_job_status(job_id, {"status": "cancelled", "phase": "Cancelled by user", "error": None})
        return 0
    except Exception as e:
        attempts = (store.get_stages(engine, job_id).get(stage) or {}).get("attempts", 1)
        permanent = _is_permanent(e, attempts)
        logger.exception(
            f"upload {job_id} failed in {stage} (attempt {attempts}, "
            f"{'permanent' if permanent else 'will retry'})"
        )
        if ctx.lost.is_set():
            return 1
        store.fail_stage(engine, job_id, stage, f"{type(e).__name__}: {e}")
        if permanent:
            store.set_job_fields(engine, job_id, retryable=False)
            store.update_job(
                engine, job_id,
                {"status": "error", "phase": f"Failed: {stage}", "error": str(e)[:2000]},
                epoch=epoch, finished=True,
            )
            if _keeps_built_tables(e):
                store.update_job(
                    engine, job_id, {"staging_cleanup": "kept: publish conflict"}, epoch=epoch,
                )
            elif stage != "cleanup":
                _discard(ctx, "failed")
        else:
            store.update_job(
                engine, job_id,
                {"phase": f"Retrying after an error in {stage}", "error": str(e)[:2000]},
                epoch=epoch,
            )
        return 1
    finally:
        if ctx.gate is not None:
            ctx.gate.stop()
        heartbeat.stop()
        try:
            store.release_lease(engine, job_id, epoch)
        except Exception as e:
            logger.warning(f"could not release the lease: {e}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Run (or resume) a bulk upload")
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--inline", action="store_true", help="run parts and chunks in this process")
    args = parser.parse_args(argv)

    from materializationengine.app import create_app
    from materializationengine.upload_state import db as upload_state_db

    app = create_app()
    with app.app_context():
        engine = upload_state_db.get_engine()
        from materializationengine.config import as_bool
        from materializationengine.utils import get_config_param

        fault = None
        if as_bool(get_config_param("UPLOAD_FAULT_INJECT_ENABLED", False)):
            fault = get_config_param("UPLOAD_FAULT_INJECT") or None
        max_in_flight = int(get_config_param("UPLOAD_RAY_MAX_IN_FLIGHT") or DEFAULT_MAX_IN_FLIGHT)

        def make_runner():
            from materializationengine.rayjobs import runner

            if args.inline:
                return runner.InlineRunner()
            import ray

            ray.init()
            logger.info("ray cluster resources: %s", ray.cluster_resources())
            return runner.RayRunner()

        return run_job(engine, args.job_id, make_runner, max_in_flight=max_in_flight, fault=fault)


if __name__ == "__main__":
    sys.exit(main())
