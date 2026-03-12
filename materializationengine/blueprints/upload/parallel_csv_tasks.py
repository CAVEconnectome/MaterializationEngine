"""
Parallel CSV ingestion pipeline (Option D).

Pipeline:
  prepare_csv_upload  — scans source blob, splits into N segment blobs in GCS,
                        creates staging table, drops indices, initialises Redis
                        checkpoint.
  conduct_csv_upload  — conductor (retrying task); dispatches batches of chunk
                        tasks, runs stale detection on every invocation, triggers
                        finalise when all chunks complete.
  process_csv_chunk   — leaf task (acks_late); downloads one segment blob,
                        applies SchemaProcessor, COPYs rows into staging table,
                        marks checkpoint COMPLETED.
  finalize_csv_upload — rebuilds indices, validates row count, cleans up GCS
                        segment blobs, returns result for the downstream chain.
"""
import io
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
from celery import chain, chord
from celery.exceptions import Retry
from celery.utils.log import get_task_logger
from flask import current_app
from google.cloud import storage

from materializationengine.blueprints.upload.checkpoint_manager import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_FAILED_PERMANENT,
    CHUNK_STATUS_FAILED_RETRYABLE,
    CHUNK_STATUS_PROCESSING,
    RedisCheckpointManager,
)
from materializationengine.blueprints.upload.processor import SchemaProcessor
from materializationengine.blueprints.upload.tasks import update_job_status
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache, db_manager
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import add_index

celery_logger = get_task_logger(__name__)

# Rows per segment blob / per chunk task.  Tune via UPLOAD_CHUNK_SIZE env var.
_DEFAULT_CHUNK_SIZE = 50_000

# How many chunk tasks to dispatch per conductor invocation.
_DISPATCH_BATCH_SIZE = 20

# Seconds before a PROCESSING chunk is considered stale.
_STALE_THRESHOLD = 600


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk_meta_redis_key(upload_session_id: str) -> str:
    return f"upload_chunk_meta:{upload_session_id}"


def _get_chunk_meta(redis_client, upload_session_id: str, chunk_index: int) -> dict:
    raw = redis_client.hget(_chunk_meta_redis_key(upload_session_id), str(chunk_index))
    if raw:
        return json.loads(raw)
    raise KeyError(f"No chunk meta for session {upload_session_id}, chunk {chunk_index}")


def _set_chunk_meta(redis_client, upload_session_id: str, chunk_index: int, meta: dict):
    from materializationengine.blueprints.upload.checkpoint_manager import REDIS_CLIENT
    REDIS_CLIENT.hset(
        _chunk_meta_redis_key(upload_session_id),
        str(chunk_index),
        json.dumps(meta),
    )
    REDIS_CLIENT.expire(_chunk_meta_redis_key(upload_session_id), 86400 * 7)


def _delete_segment_blob(bucket_name: str, blob_name: str):
    try:
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_name)
        if blob.exists():
            blob.delete()
    except Exception as e:
        celery_logger.warning(f"Could not delete segment blob {blob_name}: {e}")


# ---------------------------------------------------------------------------
# Task 1 — Preparation
# ---------------------------------------------------------------------------

@celery.task(name="orchestration:prepare_csv_upload", bind=True)
def prepare_csv_upload(
    self,
    file_path: str,
    schema_type: str,
    column_mapping: Dict[str, str],
    job_id_for_status: str,
    file_metadata: Dict[str, Any],
    datastack_info: Dict[str, Any],
    reference_table: Optional[str] = None,
    ignored_columns: Optional[List[str]] = None,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
    spatial_lookup_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    One-pass scan of the source CSV:
      - splits into N segment blobs in GCS (each chunk_size rows)
      - pre-assigns ID ranges so chunk tasks run fully independently
      - creates staging table and drops indices
      - initialises RedisCheckpointManager workflow
    """
    update_job_status(
        job_id_for_status,
        {"status": "processing", "phase": "Preparing parallel upload", "progress": 0},
    )

    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
    staging_database = current_app.config.get("STAGING_DATABASE_NAME")
    table_name = file_metadata["metadata"]["table_name"]
    schema = file_metadata["metadata"]["schema_type"]
    upload_session_id = job_id_for_status  # reuse job id as session namespace

    gcs_client = storage.Client()
    bucket = gcs_client.bucket(bucket_name)

    source_blob_name = file_path.split("/")[-1]
    source_blob = bucket.blob(source_blob_name)
    if not source_blob.exists():
        raise FileNotFoundError(f"Source blob '{source_blob_name}' not found in bucket '{bucket_name}'.")

    timestamp = datetime.now(timezone.utc)

    # ------------------------------------------------------------------
    # Single pass: split source into segment blobs, assign ID ranges
    # ------------------------------------------------------------------
    segment_blob_names: List[str] = []
    chunk_metas: List[dict] = []  # {blob_name, id_start, id_end, row_count}
    id_cursor = 0  # last assigned id (0-based, incremented per row)

    # SchemaProcessor is stateless for validation; we only need generate_ids flag
    _probe = SchemaProcessor(
        schema_type,
        reference_table,
        column_mapping=column_mapping,
        ignored_columns=ignored_columns,
        id_counter_start=0,
    )
    generate_ids = _probe.generate_ids

    chunk_index = 0
    with source_blob.open("rb") as blob_stream:
        reader = pd.read_csv(blob_stream, chunksize=chunk_size)
        for raw_chunk in reader:
            segment_blob_name = f"upload_seg_{upload_session_id}_{chunk_index:05d}.csv"
            segment_blob = bucket.blob(segment_blob_name)

            n_rows = len(raw_chunk)
            if generate_ids:
                id_start = id_cursor + 1
                id_end = id_cursor + n_rows  # inclusive
                id_cursor = id_end
            else:
                id_col = _probe.column_mapping.get("id", "id")
                id_start = int(raw_chunk[id_col].min())
                id_end = int(raw_chunk[id_col].max())

            # Write raw (untransformed) chunk to its segment blob
            buf = io.BytesIO()
            raw_chunk.to_csv(buf, index=False)
            buf.seek(0)
            segment_blob.upload_from_file(buf, content_type="text/csv")

            meta = {
                "blob_name": segment_blob_name,
                "id_start": id_start,
                "id_end": id_end,
                "row_count": n_rows,
            }
            chunk_metas.append(meta)
            segment_blob_names.append(segment_blob_name)
            chunk_index += 1

            update_job_status(
                job_id_for_status,
                {
                    "phase": "Preparing parallel upload: splitting CSV",
                    "progress": 5,
                    "current_chunk_num": chunk_index,
                },
            )

    total_chunks = len(chunk_metas)
    total_rows = sum(m["row_count"] for m in chunk_metas)
    celery_logger.info(
        f"Split '{source_blob_name}' into {total_chunks} segment blobs "
        f"({total_rows} rows) for session {upload_session_id}."
    )

    # ------------------------------------------------------------------
    # Persist chunk metadata to Redis
    # ------------------------------------------------------------------
    from materializationengine.blueprints.upload.checkpoint_manager import REDIS_CLIENT
    for i, meta in enumerate(chunk_metas):
        _set_chunk_meta(REDIS_CLIENT, upload_session_id, i, meta)

    # ------------------------------------------------------------------
    # Create staging table + drop indices
    # ------------------------------------------------------------------
    description = file_metadata["metadata"]["description"]
    user_id = file_metadata["metadata"]["user_id"]
    voxel_resolution_nm_x = file_metadata["metadata"]["voxel_resolution_nm_x"]
    voxel_resolution_nm_y = file_metadata["metadata"]["voxel_resolution_nm_y"]
    voxel_resolution_nm_z = file_metadata["metadata"]["voxel_resolution_nm_z"]
    write_permission = file_metadata["metadata"]["write_permission"]
    read_permission = file_metadata["metadata"]["read_permission"]
    flat_segmentation_source = file_metadata["metadata"].get("flat_segmentation_source")
    notice_text = file_metadata["metadata"].get("notice_text")
    force_overwrite = file_metadata["metadata"].get("force_overwrite", False)

    if file_metadata["metadata"].get("is_reference_schema"):
        table_metadata = {"reference_table": file_metadata["metadata"].get("reference_table")}
    else:
        table_metadata = None

    db_client = dynamic_annotation_cache.get_db(staging_database)

    try:
        db_client.annotation.create_table(
            table_name=table_name,
            schema_type=schema,
            description=description,
            user_id=user_id,
            voxel_resolution_x=voxel_resolution_nm_x,
            voxel_resolution_y=voxel_resolution_nm_y,
            voxel_resolution_z=voxel_resolution_nm_z,
            table_metadata=table_metadata,
            flat_segmentation_source=flat_segmentation_source,
            write_permission=write_permission,
            read_permission=read_permission,
            notice_text=notice_text,
        )
    except Exception as e:
        celery_logger.warning(f"create_table for {table_name} raised (may already exist): {e}")

    if force_overwrite:
        try:
            from sqlalchemy import text as sa_text
            with db_client.database.engine.begin() as conn:
                conn.execute(sa_text(f"TRUNCATE TABLE {table_name} CASCADE"))
            celery_logger.info(f"force_overwrite: truncated {table_name}")
        except Exception as e:
            celery_logger.warning(f"Could not truncate {table_name}: {e}")

    index_cache.drop_table_indices(table_name, db_client.database.engine)

    # ------------------------------------------------------------------
    # Initialise checkpoint
    # ------------------------------------------------------------------
    checkpoint_manager = RedisCheckpointManager(staging_database)
    workflow_data = checkpoint_manager.initialize_workflow(
        table_name=upload_session_id,
        task_id=self.request.id,
        datastack_name=datastack_info.get("datastack"),
    )
    checkpoint_manager.update_workflow(
        table_name=upload_session_id,
        total_chunks=total_chunks,
        total_row_estimate=total_rows,
        status="processing_chunks",
    )

    update_job_status(
        job_id_for_status,
        {
            "status": "processing",
            "phase": "CSV split complete, starting parallel ingestion",
            "progress": 10,
            "total_rows": total_rows,
            "total_chunks": total_chunks,
        },
    )

    return {
        "upload_session_id": upload_session_id,
        "total_chunks": total_chunks,
        "total_rows": total_rows,
        "table_name": table_name,
        "schema_type": schema_type,
        "column_mapping": column_mapping,
        "reference_table": reference_table,
        "ignored_columns": ignored_columns,
        "staging_database": staging_database,
        "bucket_name": bucket_name,
        "job_id_for_status": job_id_for_status,
        "file_metadata": file_metadata,
        "datastack_info": datastack_info,
        "timestamp_iso": timestamp.isoformat(),
        "spatial_lookup_params": spatial_lookup_params or {},
    }


# ---------------------------------------------------------------------------
# Task 2 — Conductor
# ---------------------------------------------------------------------------

@celery.task(
    name="orchestration:conduct_csv_upload",
    bind=True,
    max_retries=None,
    default_retry_delay=30,
)
def conduct_csv_upload(
    self,
    prepare_result: Dict[str, Any],
    batch_size: int = _DISPATCH_BATCH_SIZE,
) -> str:
    """
    Conductor task.  Runs stale detection, then dispatches a batch of
    process_csv_chunk tasks as a chord whose body is the next conductor
    invocation.  When all chunks complete, fires finalize_csv_upload.
    """
    upload_session_id = prepare_result["upload_session_id"]
    total_chunks = prepare_result["total_chunks"]
    staging_database = prepare_result["staging_database"]
    job_id_for_status = prepare_result["job_id_for_status"]

    checkpoint_manager = RedisCheckpointManager(staging_database)
    workflow_data = checkpoint_manager.get_workflow_data(upload_session_id)
    if not workflow_data:
        raise RuntimeError(f"Workflow data missing for upload session {upload_session_id}")

    # Stale detection on every invocation
    recovered_processing = checkpoint_manager.recover_stale_processing_chunks(
        upload_session_id, stale_threshold_seconds=_STALE_THRESHOLD
    )
    if recovered_processing:
        celery_logger.info(
            f"[{upload_session_id}] Recovered {recovered_processing} stale PROCESSING chunk(s)."
        )

    chunk_indices, _, new_pending_cursor = checkpoint_manager.get_chunks_to_process(
        table_name=upload_session_id,
        total_chunks=total_chunks,
        batch_size=batch_size,
        prioritize_failed_chunks=True,
    )

    if new_pending_cursor != workflow_data.current_pending_scan_cursor:
        checkpoint_manager.update_workflow(
            table_name=upload_session_id,
            current_pending_scan_cursor=new_pending_cursor,
        )

    if not chunk_indices:
        # Check whether all chunks are in a terminal state
        all_statuses = checkpoint_manager.get_all_chunk_statuses(upload_session_id)
        pending_left = False
        if all_statuses:
            for idx_str, status in all_statuses.items():
                if int(idx_str) < total_chunks and status not in (
                    CHUNK_STATUS_COMPLETED,
                    CHUNK_STATUS_FAILED_PERMANENT,
                ):
                    pending_left = True
                    break

        is_scan_done = new_pending_cursor >= total_chunks

        if not pending_left and is_scan_done:
            # All done — fire finalize
            celery_logger.info(
                f"[{upload_session_id}] All {total_chunks} chunks complete. Firing finalize."
            )
            finalize_csv_upload.apply_async(args=[prepare_result])
            return f"All chunks done for {upload_session_id}. Finalize dispatched."

        # Some chunks still in flight or stale — retry conductor
        celery_logger.info(
            f"[{upload_session_id}] No chunks to dispatch now; retrying conductor in "
            f"{0 if recovered_processing else 30}s."
        )
        raise self.retry(countdown=0 if recovered_processing else 30)

    # Mark each chunk as PROCESSING before dispatching
    for idx in chunk_indices:
        checkpoint_manager.set_chunk_status(upload_session_id, idx, CHUNK_STATUS_PROCESSING)

    # Build chunk task signatures
    chunk_tasks = [
        process_csv_chunk.si(
            prepare_result=prepare_result,
            chunk_index=idx,
        )
        for idx in chunk_indices
    ]

    # Chord: chunk tasks → next conductor invocation
    next_conductor = conduct_csv_upload.si(prepare_result, batch_size=batch_size)

    celery_logger.info(
        f"[{upload_session_id}] Dispatching {len(chunk_tasks)} chunk tasks "
        f"(indices {chunk_indices})."
    )
    update_job_status(
        job_id_for_status,
        {
            "phase": "Parallel CSV ingestion in progress",
            "progress": max(
                10,
                int(10 + 70 * workflow_data.completed_chunks / max(total_chunks, 1)),
            ),
            "completed_chunks": workflow_data.completed_chunks,
            "total_chunks": total_chunks,
        },
    )

    chord(chunk_tasks, body=next_conductor).apply_async()
    return f"Dispatched {len(chunk_tasks)} chunk tasks for {upload_session_id}."


# ---------------------------------------------------------------------------
# Task 3 — Chunk worker
# ---------------------------------------------------------------------------

@celery.task(
    name="process:process_csv_chunk",
    bind=True,
    acks_late=True,
    max_retries=3,
    default_retry_delay=60,
)
def process_csv_chunk(
    self,
    prepare_result: Dict[str, Any],
    chunk_index: int,
) -> Dict[str, Any]:
    """
    Downloads one segment blob, transforms it with SchemaProcessor, and
    COPYs the resulting rows into the staging table via psycopg2.
    Idempotent: deletes rows in the chunk's ID range before inserting, so
    a retry after a partial insert does not duplicate data.
    """
    upload_session_id = prepare_result["upload_session_id"]
    staging_database = prepare_result["staging_database"]
    bucket_name = prepare_result["bucket_name"]
    schema_type = prepare_result["schema_type"]
    column_mapping = prepare_result["column_mapping"]
    reference_table = prepare_result.get("reference_table")
    ignored_columns = prepare_result.get("ignored_columns")
    table_name = prepare_result["table_name"]
    timestamp = datetime.fromisoformat(prepare_result["timestamp_iso"])

    checkpoint_manager = RedisCheckpointManager(staging_database)

    from materializationengine.blueprints.upload.checkpoint_manager import REDIS_CLIENT
    try:
        meta = _get_chunk_meta(REDIS_CLIENT, upload_session_id, chunk_index)
    except KeyError:
        celery_logger.error(
            f"[{upload_session_id}] Chunk {chunk_index}: metadata missing — marking FAILED_PERMANENT."
        )
        checkpoint_manager.set_chunk_status(
            upload_session_id,
            chunk_index,
            CHUNK_STATUS_FAILED_PERMANENT,
            {"error_message": "Chunk metadata missing from Redis"},
        )
        return {"chunk_index": chunk_index, "status": "failed_permanent"}

    blob_name = meta["blob_name"]
    id_start = meta["id_start"]
    id_end = meta["id_end"]

    try:
        # ------------------------------------------------------------------
        # Download segment blob
        # ------------------------------------------------------------------
        gcs_client = storage.Client()
        blob = gcs_client.bucket(bucket_name).blob(blob_name)
        if not blob.exists():
            raise FileNotFoundError(f"Segment blob {blob_name} not found.")

        with blob.open("rb") as f:
            raw_df = pd.read_csv(f)

        # ------------------------------------------------------------------
        # Transform
        # ------------------------------------------------------------------
        processor = SchemaProcessor(
            schema_type,
            reference_table,
            column_mapping=column_mapping,
            ignored_columns=ignored_columns,
            id_counter_start=id_start - 1,  # process_chunk starts at _id_counter + 1
        )
        processed_df = processor.process_chunk(raw_df, timestamp)

        if processed_df.empty:
            celery_logger.info(
                f"[{upload_session_id}] Chunk {chunk_index}: all rows dropped (e.g. bad coordinates)."
            )
            checkpoint_manager.set_chunk_status(
                upload_session_id,
                chunk_index,
                CHUNK_STATUS_COMPLETED,
                {"rows_processed": 0},
            )
            _delete_segment_blob(bucket_name, blob_name)
            return {"chunk_index": chunk_index, "rows_inserted": 0}

        # ------------------------------------------------------------------
        # Idempotent delete of any previously inserted rows for this ID range,
        # then COPY into the staging table.
        # ------------------------------------------------------------------
        engine = db_manager.get_engine(staging_database)
        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cur:
                # Purge any rows from a previous (partial) attempt
                cur.execute(
                    f"DELETE FROM {table_name} WHERE id >= %s AND id <= %s",
                    (id_start, id_end),
                )

                # Build CSV buffer for COPY
                csv_buf = io.StringIO()
                processed_df.to_csv(csv_buf, index=False, header=True)
                csv_buf.seek(0)

                cur.copy_expert(
                    f"COPY {table_name} ({', '.join(processed_df.columns)}) "
                    f"FROM STDIN WITH CSV HEADER NULL ''",
                    csv_buf,
                )
            raw_conn.commit()
        except Exception:
            raw_conn.rollback()
            raise
        finally:
            raw_conn.close()

        rows_inserted = len(processed_df)
        celery_logger.info(
            f"[{upload_session_id}] Chunk {chunk_index}: inserted {rows_inserted} rows "
            f"(ids {id_start}–{id_end})."
        )

        checkpoint_manager.set_chunk_status(
            upload_session_id,
            chunk_index,
            CHUNK_STATUS_COMPLETED,
            {"rows_processed": rows_inserted},
        )
        _delete_segment_blob(bucket_name, blob_name)
        return {"chunk_index": chunk_index, "rows_inserted": rows_inserted}

    except Exception as e:
        celery_logger.error(
            f"[{upload_session_id}] Chunk {chunk_index} failed: {e}", exc_info=True
        )
        attempt = self.request.retries + 1
        if attempt >= self.max_retries:
            checkpoint_manager.set_chunk_status(
                upload_session_id,
                chunk_index,
                CHUNK_STATUS_FAILED_PERMANENT,
                {"error_message": str(e), "attempt_count": attempt},
            )
            return {"chunk_index": chunk_index, "status": "failed_permanent"}
        checkpoint_manager.set_chunk_status(
            upload_session_id,
            chunk_index,
            CHUNK_STATUS_FAILED_RETRYABLE,
            {"error_message": str(e), "attempt_count": attempt},
        )
        raise self.retry(exc=e)


# ---------------------------------------------------------------------------
# Task 4 — Finalise
# ---------------------------------------------------------------------------

@celery.task(name="orchestration:finalize_csv_upload", bind=True)
def finalize_csv_upload(
    self,
    prepare_result: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Rebuilds indices on the staging table, validates row count, cleans up
    any remaining GCS segment blobs, and returns the result dict expected
    by the downstream chain (run_spatial_lookup_workflow, etc.).
    """
    upload_session_id = prepare_result["upload_session_id"]
    staging_database = prepare_result["staging_database"]
    bucket_name = prepare_result["bucket_name"]
    table_name = prepare_result["table_name"]
    schema_type = prepare_result["schema_type"]
    total_chunks = prepare_result["total_chunks"]
    job_id_for_status = prepare_result["job_id_for_status"]
    file_metadata = prepare_result["file_metadata"]
    datastack_info = prepare_result["datastack_info"]

    update_job_status(
        job_id_for_status,
        {"status": "processing", "phase": "Finalizing: rebuilding indices", "progress": 80},
    )

    db_client = dynamic_annotation_cache.get_db(staging_database)
    schema_model = db_client.schema.create_annotation_model(table_name, schema_type)

    anno_indices = index_cache.add_indices_sql_commands(
        table_name, schema_model, db_client.database.engine
    )
    for i, index_sql in enumerate(anno_indices):
        celery_logger.info(f"[{upload_session_id}] Adding index {i+1}/{len(anno_indices)}")
        add_index(staging_database, index_sql)

    # ------------------------------------------------------------------
    # Row count validation
    # ------------------------------------------------------------------
    from sqlalchemy import text as sa_text
    with db_client.database.engine.connect() as conn:
        result = conn.execute(sa_text(f"SELECT COUNT(*) FROM {table_name}"))
        actual_rows = result.scalar()

    checkpoint_manager = RedisCheckpointManager(staging_database)
    workflow_data = checkpoint_manager.get_workflow_data(upload_session_id)
    expected_rows = workflow_data.rows_processed if workflow_data else None

    celery_logger.info(
        f"[{upload_session_id}] Row count: {actual_rows} actual"
        + (f" / {expected_rows} processed" if expected_rows else "")
    )

    # ------------------------------------------------------------------
    # Clean up any remaining segment blobs (e.g. from permanently failed chunks)
    # ------------------------------------------------------------------
    from materializationengine.blueprints.upload.checkpoint_manager import REDIS_CLIENT
    meta_key = _chunk_meta_redis_key(upload_session_id)
    for i in range(total_chunks):
        raw = REDIS_CLIENT.hget(meta_key, str(i))
        if raw:
            meta = json.loads(raw)
            _delete_segment_blob(bucket_name, meta["blob_name"])
    REDIS_CLIENT.delete(meta_key)

    checkpoint_manager.update_workflow(
        table_name=upload_session_id,
        status="completed",
    )

    update_job_status(
        job_id_for_status,
        {
            "status": "processing",
            "phase": "Database upload complete, awaiting spatial lookup",
            "progress": 100,
            "total_rows": actual_rows,
            "active_workflow_part": "spatial_lookup",
            "spatial_lookup_config": {
                "table_name": table_name,
                "datastack_name": datastack_info.get("datastack"),
                "database_name": staging_database,
            },
        },
    )

    # ------------------------------------------------------------------
    # Dispatch downstream chain: spatial lookup → monitor → transfer
    # ------------------------------------------------------------------
    from materializationengine.workflows.spatial_lookup import run_spatial_lookup_workflow
    from materializationengine.blueprints.upload.tasks import (
        monitor_spatial_workflow_completion,
        transfer_to_production,
    )

    slp = prepare_result.get("spatial_lookup_params", {})
    materialization_time_stamp = slp.get("materialization_time_stamp")
    chunk_scale_factor = slp.get("chunk_scale_factor", 2)
    supervoxel_batch_size = slp.get("supervoxel_batch_size", 50)

    downstream = chain(
        run_spatial_lookup_workflow.si(  # type: ignore[attr-defined]
            datastack_info=datastack_info,
            table_name=table_name,
            chunk_scale_factor=chunk_scale_factor,
            supervoxel_batch_size=supervoxel_batch_size,
            use_staging_database=True,
        ),
        monitor_spatial_workflow_completion.s(  # type: ignore[attr-defined]
            datastack_info=datastack_info,
            table_name_for_transfer=table_name,
            materialization_time_stamp=materialization_time_stamp,
            job_id_for_status=job_id_for_status,
        ),
        transfer_to_production.s(transfer_segmentation=True),  # type: ignore[attr-defined]
    )
    downstream.apply_async()

    celery_logger.info(
        f"[{upload_session_id}] Downstream chain dispatched: "
        f"spatial_lookup → monitor → transfer for table '{table_name}'."
    )

    return {
        "status_task": "completed_db_upload",
        "job_id_for_status": job_id_for_status,
        "table_name": table_name,
        "datastack_info": datastack_info,
    }
