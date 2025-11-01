import datetime
import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from celery import Task, chain, chord
from celery.exceptions import MaxRetriesExceededError
from celery.utils.log import get_task_logger
from cloudvolume.lib import Vec
from geoalchemy2 import Geometry
from sqlalchemy import (
    case,
    func,
    literal,
    select,
    union_all,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlalchemy.ext.declarative import declarative_base

from materializationengine.blueprints.upload.checkpoint_manager import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_ERROR,
    CHUNK_STATUS_FAILED_PERMANENT,
    CHUNK_STATUS_FAILED_RETRYABLE,
    CHUNK_STATUS_PROCESSING,
    CHUNK_STATUS_PROCESSING_SUBTASKS,
    RedisCheckpointManager,
)
from materializationengine.celery_init import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.cloudvolume_gateway import cloudvolume_cache
from materializationengine.database import db_manager, dynamic_annotation_cache
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import (
    add_index,
    get_materialization_info,
    update_metadata,
    workflow_complete,
)
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
    get_config_param,
    get_geom_from_wkb,
    get_query_columns_by_suffix,
)
from materializationengine.workflows.chunking import (
    ChunkingStrategy,
    reconstruct_chunk_bounds,
)
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_root_ids,
)

Base = declarative_base()

celery_logger = get_task_logger(__name__)


MAX_CHUNK_WORKFLOW_ATTEMPTS = 10


class ChunkProcessingError(Exception):
    """Base class for errors during chunk processing."""

    pass


class ChunkDataValidationError(ChunkProcessingError):
    """Error due to invalid or unprocessable data within a chunk.
    Leads to FAILED_PERMANENT for the chunk.
    """

    pass


class SystemicWorkflowError(ChunkProcessingError):
    """A critical error detected during chunk processing that should halt the entire workflow.
    Leads to the main workflow status being set to ERROR.
    """

    pass


@celery.task(
    name="workflow:run_spatial_lookup_workflow",
    bind=True,
    acks_late=True,
    # autoretry_for=(Exception,),
    # max_retries=3,
    # retry_backoff=True,
)
def run_spatial_lookup_workflow(
    self,
    datastack_info: dict,
    table_name: str,
    chunk_scale_factor: int = 1,
    supervoxel_batch_size: int = 50,
    use_staging_database: bool = False,
    resume_from_checkpoint: bool = False,
):
    """Spatial Lookup Workflow processes a table's points in chunks and inserts supervoxel IDs into the database."""
    start_time = time.time()

    staging_database_name = get_config_param("STAGING_DATABASE_NAME")
    database_name = (
        staging_database_name
        if use_staging_database
        else datastack_info["aligned_volume"]["name"]
    )

    checkpoint_manager = RedisCheckpointManager(database_name)

    existing_workflow = checkpoint_manager.get_workflow_data(table_name)
    should_resume = (
        resume_from_checkpoint
        and existing_workflow
        and existing_workflow.status not in [CHUNK_STATUS_COMPLETED, "failed"]
    )

    if should_resume:
        celery_logger.info(
            f"Resuming existing workflow for {table_name} from checkpoint."
        )
        checkpoint_manager.update_workflow(
            table_name=table_name, status="resuming", task_id=self.request.id
        )
        celery_logger.info(
            f"Resumed workflow state: {existing_workflow.completed_chunks or 0} completed chunks, "
            f"status: {existing_workflow.status}."
            f"last completed chunk index: {existing_workflow.latest_completed_chunk_info.index if existing_workflow.latest_completed_chunk_info else 'N/A'}"
        )
    else:
        if existing_workflow and not resume_from_checkpoint:
            celery_logger.info(
                f"Starting fresh workflow for {table_name}, resetting existing checkpoint."
            )
        else:
            celery_logger.info(f"Starting new workflow for {table_name}.")

        checkpoint_manager.initialize_workflow(
            table_name, self.request.id, datastack_info.get("datastack")
        )
        checkpoint_manager.update_workflow(
            table_name=table_name,
            status="initializing",
            latest_completed_chunk_info=None,
            completed_chunks=0,
            mat_info_idx=0,
        )

    materialization_time_stamp = datetime.datetime.utcnow()

    mat_metadata = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
        table_name=table_name,
        skip_row_count=True,
        database=database_name,
    )
    table_info = mat_metadata[0]

    if not mat_metadata:
        celery_logger.warning(
            f"No materialization metadata found for {table_name} in {database_name}. Workflow ending."
        )
        checkpoint_manager.update_workflow(
            table_name=table_name, status=CHUNK_STATUS_COMPLETED, total_chunks=0
        )
        return {
            "status": "completed_no_tables",
            "message": f"No tables to process for workflow {table_name}.",
        }

    checkpoint_manager.update_workflow(table_name=table_name, status="processing")

    processing_task = process_table_in_chunks.s(
        datastack_info=datastack_info,
        mat_metadata=table_info,
        workflow_name=table_name,
        annotation_table_name=table_info["annotation_table_name"],
        database_name=database_name,
        chunk_scale_factor=chunk_scale_factor,
        supervoxel_batch_size=supervoxel_batch_size,
        initial_run=not should_resume,
    )

    workflow_result = processing_task.apply_async()

    end_time = time.time()
    celery_logger.info(
        f"Spatial lookup workflow for {table_name} initiated in {end_time - start_time:.2f}s."
        f" Task ID: {workflow_result.id}"
    )

    return {
        "status": "initiated",
        "launcher_task_id": self.request.id,
        "initial_processing_task_id": workflow_result.id,
        "workflow_name": table_name,
        "database_name": database_name,
        "message": f"Spatial lookup workflow for '{table_name}' has been started/resumed.",
    }



@celery.task(name="workflow:update_workflow_status")
def update_workflow_status(database, table_name, status):
    """Helper task to update workflow status."""
    checkpoint_manager = RedisCheckpointManager(database)

    checkpoint_manager.update_workflow(table_name=table_name, status=status)
    celery_logger.info(
        f"Updated workflow status for {table_name} in {database} to {status}"
    )
    return f"Updated workflow status for {table_name} to {status}"


@celery.task(
    name="workflow:process_table_in_chunks",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
)
def process_table_in_chunks(
    self,
    datastack_info: dict,
    mat_metadata: dict,
    workflow_name: str,
    annotation_table_name: str,
    database_name: str,
    chunk_scale_factor: int,
    supervoxel_batch_size: int,
    batch_size_for_dispatch: int = 10,
    prioritize_failed_chunks: bool = True,
    initial_run: bool = False,
):
    """
    Processes a single annotation table in chunks.
    This task acts as a dispatcher:
    1. Gets a batch of chunk indices to process using RedisCheckpointManager.
    2. For each chunk index, reconstructs bounds and creates a `process_chunk` task.
    3. Chords these `process_chunk` tasks.
    4. If more chunks remain, the chord's callback re-invokes this `process_table_in_chunks` task.
    5. If no chunks remain, it triggers index rebuilding and final completion.
    """
    checkpoint_manager = RedisCheckpointManager(database_name)
    engine = db_manager.get_engine(database_name)

    try:
        workflow_data = checkpoint_manager.get_workflow_data(workflow_name)
        if not workflow_data:
            celery_logger.error(
                f"Workflow data not found for {workflow_name} in process_table_in_chunks. Aborting."
            )
            checkpoint_manager.update_workflow(
                workflow_name=workflow_name,
                status="failed",
                last_error="Workflow data missing in dispatcher",
            )
            return

        if not mat_metadata:
            celery_logger.error(
                f"No mat_metadata found for {annotation_table_name}. Cannot proceed."
            )
            checkpoint_manager.update_workflow(
                workflow_name=workflow_name,
                status="failed",
                last_error=f"Mat metadata missing for {annotation_table_name}",
            )
            return

        create_missing_segmentation_table(mat_metadata)

        if (
            initial_run
            or not workflow_data.chunking_parameters
            or not workflow_data.total_chunks
        ):
            celery_logger.info(
                f"Calculating/re-calculating chunking strategy for {annotation_table_name} (workflow: {workflow_name})."
            )

            if initial_run:
                index_cache.drop_table_indices(
                    mat_metadata["segmentation_table_name"],
                    engine,
                    drop_primary_key=False,
                )

            chunking = ChunkingStrategy(
                engine=engine,
                table_name=annotation_table_name,
                database=database_name,
                base_chunk_size=chunk_scale_factor * 1024,
            )
            chunking.select_strategy()

            chunking_params_dict = chunking.to_dict()

            checkpoint_manager.update_workflow(
                table_name=workflow_name,
                total_chunks=chunking.total_chunks,
                chunking_parameters=chunking_params_dict,
                chunking_strategy=chunking.strategy_name,
                used_chunk_size=chunking.actual_chunk_size,
                total_row_estimate=chunking.estimated_rows,
                min_enclosing_bbox=(
                    [chunking.min_coords.tolist(), chunking.max_coords.tolist()]
                    if chunking.min_coords is not None
                    and chunking.max_coords is not None
                    else None
                ),
                current_failed_retryable_scan_cursor=0,
                current_pending_scan_cursor=0,
                status="processing_chunks",
            )
            workflow_data = checkpoint_manager.get_workflow_data(workflow_name)
            if (
                not workflow_data
                or not workflow_data.chunking_parameters
                or workflow_data.total_chunks is None
            ):
                celery_logger.error(
                    f"Failed to update/fetch workflow_data after chunking calculation for {workflow_name}."
                )
                checkpoint_manager.update_workflow(
                    workflow_name=workflow_name,
                    status="failed",
                    last_error="Chunking data init failed",
                )
                return
            celery_logger.info(
                f"Chunking strategy for {annotation_table_name} (workflow: {workflow_name}): {chunking.total_chunks} chunks. Params stored."
            )
        else:
            celery_logger.info(
                f"Using existing chunking strategy for {annotation_table_name} (workflow: {workflow_name}) from checkpoint."
            )
            if workflow_data.status != "processing_chunks":  # Ensure status is correct
                checkpoint_manager.update_workflow(
                    table_name=workflow_name, status="processing_chunks"
                )

        if workflow_data.total_chunks == 0:
            celery_logger.info(
                f"Total chunks is 0 for {annotation_table_name}. Assuming no data or empty bounds. Proceeding to completion."
            )
            rebuild_task = rebuild_indices_for_spatial_lookup.si(
                mat_metadata, database_name
            )
            final_completion_tasks = chain(
                update_workflow_status.si(
                    database_name, workflow_name, CHUNK_STATUS_COMPLETED
                ),
                workflow_complete.si(
                    f"Spatial Lookup for {workflow_name} (table: {annotation_table_name}) completed - no chunks."
                ),
            )
            full_chain = chain(rebuild_task, final_completion_tasks).on_error(
                spatial_workflow_failed.s(
                    workflow_name=f"spatial_lookup_completion_{workflow_name}",
                    database_name=database_name,
                )
            )
            full_chain.apply_async()
            return f"No chunks to process for {annotation_table_name}. Finalizing."

        chunk_indices_to_process, new_failed_cursor, new_pending_cursor = (
            checkpoint_manager.get_chunks_to_process(
                table_name=workflow_name,
                total_chunks=workflow_data.total_chunks,
                batch_size=batch_size_for_dispatch,
                prioritize_failed_chunks=prioritize_failed_chunks,
            )
        )

        update_cursor_fields = {}

        if new_pending_cursor != workflow_data.current_pending_scan_cursor:
            update_cursor_fields["current_pending_scan_cursor"] = new_pending_cursor

        if update_cursor_fields:
            checkpoint_manager.update_workflow(
                table_name=workflow_name, **update_cursor_fields
            )

        if not chunk_indices_to_process:
            all_statuses = checkpoint_manager.get_all_chunk_statuses(workflow_name)
            pending_or_retryable_left = False
            if all_statuses:
                for chunk_idx_str, status_str in all_statuses.items():
                    if int(
                        chunk_idx_str
                    ) < workflow_data.total_chunks and status_str not in [
                        CHUNK_STATUS_COMPLETED,
                        CHUNK_STATUS_FAILED_PERMANENT,
                    ]:
                        celery_logger.warning(
                            f"Chunk {chunk_idx_str} has status {status_str} but get_chunks_to_process returned empty. Workflow might not be complete."
                        )
                        pending_or_retryable_left = True
                        break

            is_failed_scan_exhausted = (
                new_failed_cursor == 0 or new_failed_cursor is None
            )
            is_pending_scan_exhausted = new_pending_cursor >= workflow_data.total_chunks

            if not pending_or_retryable_left and (
                is_failed_scan_exhausted and is_pending_scan_exhausted
            ):
                celery_logger.info(
                    f"All chunks processed or permanently failed for {annotation_table_name} (workflow: {workflow_name}). Initiating final steps."
                )
                rebuild_task = rebuild_indices_for_spatial_lookup.si(
                    mat_metadata, database_name
                )
                final_completion_tasks = chain(
                    update_workflow_status.si(
                        database_name, workflow_name, CHUNK_STATUS_COMPLETED
                    ),
                    workflow_complete.si(
                        f"Spatial Lookup for {workflow_name} (table: {annotation_table_name}) fully completed."
                    ),
                )
                full_chain = chain(rebuild_task, final_completion_tasks).on_error(
                    spatial_workflow_failed.s(
                        workflow_name=f"spatial_lookup_completion_{workflow_name}",
                        database_name=database_name,
                    )
                )
                full_chain.apply_async()
                return f"All chunks processed for {annotation_table_name}. Finalizing."
            else:
                celery_logger.info(
                    f"No chunks returned by get_chunks_to_process for {workflow_name}, but scan may not be exhausted or non-terminal chunks exist. Retrying dispatcher."
                )
                raise self.retry(countdown=30)

        processing_tasks = []
        for chunk_idx_to_process in chunk_indices_to_process:
            min_corner_val, max_corner_val = reconstruct_chunk_bounds(
                chunk_index=chunk_idx_to_process,
                chunking_parameters=workflow_data.chunking_parameters,
            )

            chunk_task_signature = process_chunk.si(
                min_corner=min_corner_val,
                max_corner=max_corner_val,
                mat_metadata=mat_metadata,
                chunk_info={
                    "chunk_idx": chunk_idx_to_process,
                    "total_chunks": workflow_data.total_chunks,
                },
                database_name=database_name,
                supervoxel_sub_batch_size=supervoxel_batch_size,
                workflow_name=workflow_name,
            )

            processing_tasks.append(chunk_task_signature)

        if processing_tasks:
            celery_logger.info(
                f"Dispatching {len(processing_tasks)} chunks for workflow {workflow_name}. Chunk indices: {chunk_indices_to_process}"
            )
            next_processing_batch = process_table_in_chunks.si(
                datastack_info=datastack_info,
                mat_metadata=mat_metadata,
                workflow_name=workflow_name,
                annotation_table_name=annotation_table_name,
                database_name=database_name,
                chunk_scale_factor=chunk_scale_factor,
                supervoxel_batch_size=supervoxel_batch_size,
                batch_size_for_dispatch=batch_size_for_dispatch,
                prioritize_failed_chunks=True,
                initial_run=False,
            )

            chord_error_handler = spatial_workflow_failed.s(
                workflow_name=workflow_name,
                database_name=database_name,
            )

            task_chord = chord(processing_tasks, body=next_processing_batch).on_error(
                chord_error_handler
            )
            task_chord.apply_async()

        return f"Dispatched batch of {len(chunk_indices_to_process)} chunks for {annotation_table_name} (workflow {workflow_name})."

    except Exception as e:
        celery_logger.error(
            f"Critical error in process_table_in_chunks dispatcher for {workflow_name} (table {annotation_table_name}): {str(e)}",
            exc_info=True,
        )
        checkpoint_manager.update_workflow(
            table_name=workflow_name,
            status="failed",
            last_error=f"Dispatcher critical error: {str(e)}",
        )

        raise self.retry(exc=e, countdown=int(2**self.request.retries))


@celery.task(
    name="process:process_chunk",
    bind=True,
    acks_late=True,
    max_retries=10,
    retry_backoff=True,
)
def process_chunk(
    self: Task,
    min_corner: List[float],
    max_corner: List[float],
    mat_metadata: dict,
    chunk_info: dict,
    database_name: str,
    supervoxel_sub_batch_size: int = 50,
    workflow_name: str = None,
):
    start_time = time.time()

    checkpoint_manager = RedisCheckpointManager(database_name)
    chunk_idx = chunk_info.get("chunk_idx")

    if workflow_name is None or chunk_idx is None:
        celery_logger.error(
            "process_chunk called without workflow_name or chunk_idx. Cannot proceed."
        )
        raise ValueError("workflow_name and chunk_idx are required for process_chunk")

    log_prefix = f"[WF:{workflow_name}, SpChunk:{chunk_idx}, Task:{self.request.id}]"

    current_chunk_status_data = checkpoint_manager.get_failed_chunk_details(
        workflow_name, chunk_idx
    )
    workflow_attempt_count = 0
    if current_chunk_status_data and isinstance(
        current_chunk_status_data.get("attempt_count"), int
    ):
        workflow_attempt_count = current_chunk_status_data.get("attempt_count", 0)

    current_attempt_of_this_chunk_processing = workflow_attempt_count + 1

    if workflow_attempt_count >= MAX_CHUNK_WORKFLOW_ATTEMPTS:
        celery_logger.error(
            f"{log_prefix} Exceeded max workflow retries ({MAX_CHUNK_WORKFLOW_ATTEMPTS}) for this chunk. Current attempt base count is {workflow_attempt_count}. Marking as FAILED_PERMANENT."
        )
        error_payload = {
            "error_message": f"Exceeded max workflow retries ({MAX_CHUNK_WORKFLOW_ATTEMPTS}) for chunk. Last error: {current_chunk_status_data.get('error_message', 'N/A')}",
            "error_type": "MaxChunkWorkflowRetriesExceeded",
            "attempt_count": workflow_attempt_count,
        }
        checkpoint_manager.set_chunk_status(
            workflow_name, chunk_idx, CHUNK_STATUS_FAILED_PERMANENT, error_payload
        )
        return {
            "status": "failed_permanent_max_chunk_workflow_retries",
            "chunk_idx": chunk_idx,
        }

    try:
        celery_logger.info(
            f"{log_prefix} Starting processing (Celery attempt {self.request.retries + 1}, Chunk workflow attempt {current_attempt_of_this_chunk_processing})"
        )

        processing_payload = {"celery_task_id": self.request.id}
        processing_payload["chunk_workflow_attempt_count"] = (
            current_attempt_of_this_chunk_processing
        )

        checkpoint_manager.set_chunk_status(
            workflow_name, chunk_idx, CHUNK_STATUS_PROCESSING, processing_payload
        )

        pts_df = get_pts_from_bbox(
            database_name, np.array(min_corner), np.array(max_corner), mat_metadata
        )

        if pts_df is None or pts_df.empty:
            celery_logger.info(f"{log_prefix} No points in bounding box.")
            status_payload = {
                "rows_processed": 0,
                "message": "No points in bounding box for chunk",
                "chunk_bounding_box": {
                    "min_corner": min_corner,
                    "max_corner": max_corner,
                },
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_COMPLETED, status_payload
            )
            return {
                "status": "success_empty_chunk",
                "chunk_idx": chunk_idx,
                "rows_processed": 0,
            }

        points_count = len(pts_df)
        celery_logger.info(f"{log_prefix} Found {points_count} points in bounding box")

        all_point_data_for_sub_batches = []
        all_point_data_for_sub_batches.extend(
            {
                "id": row["id"],
                "type": row["type"],
                "pt_position": row["pt_position"],
            }
            for _, row in pts_df.iterrows()
        )

        sub_task_signatures = []
        num_total_points = len(all_point_data_for_sub_batches)
        num_sub_batches = 0

        mat_metadata_for_subtask = mat_metadata.copy()
        if isinstance(
            mat_metadata_for_subtask.get("materialization_time_stamp"),
            datetime.datetime,
        ):
            mat_metadata_for_subtask["materialization_time_stamp"] = (
                mat_metadata_for_subtask["materialization_time_stamp"].isoformat()
            )

        if "workflow_name" not in mat_metadata_for_subtask:
            mat_metadata_for_subtask["workflow_name"] = workflow_name

        for i in range(0, num_total_points, supervoxel_sub_batch_size):
            sub_batch_point_data_slice = all_point_data_for_sub_batches[
                i : i + supervoxel_sub_batch_size
            ]
            if sub_batch_point_data_slice:
                num_sub_batches += 1
                sub_task_signatures.append(
                    process_and_insert_sub_batch.s(
                        sub_batch_point_data=sub_batch_point_data_slice,
                        mat_info=mat_metadata_for_subtask,
                        database_name=database_name,
                        original_chunk_idx=chunk_idx,
                        sub_batch_idx=num_sub_batches,
                        workflow_name=workflow_name,
                    )
                )

        celery_logger.info(
            f"{log_prefix} Prepared {num_sub_batches} sub-tasks using 'process_and_insert_sub_batch'."
        )

        if not sub_task_signatures:
            celery_logger.warning(
                f"{log_prefix} No sub-tasks created for 'process_and_insert_sub_batch' despite points. Marking COMPLETED (0 rows)."
            )
            status_payload = {
                "rows_processed": 0,
                "message": "No sub-tasks for processing, though points were present.",
                "chunk_bounding_box": {
                    "min_corner": min_corner,
                    "max_corner": max_corner,
                },
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_COMPLETED, status_payload
            )
            return {
                "status": "success_no_subtasks_created",
                "chunk_idx": chunk_idx,
                "rows_processed": 0,
            }

        mat_metadata_for_callback = mat_metadata_for_subtask

        callback_sig = finalize_chunk_outcome.s(
            original_chunk_idx=chunk_idx,
            mat_info=mat_metadata_for_callback,
            database_name=database_name,
            workflow_name=workflow_name,
            total_expected_sub_batches=num_sub_batches,
            min_corner=min_corner,
            max_corner=max_corner,
        )

        chord_error_handler_sig = process_chunk_sub_chord_failed.s(
            original_chunk_idx=chunk_idx,
            workflow_name=workflow_name,
            database_name=database_name,
            parent_chunk_attempt_number=current_attempt_of_this_chunk_processing,
        )

        processing_chord = chord(
            header=sub_task_signatures, body=callback_sig
        ).on_error(chord_error_handler_sig)
        chord_result = processing_chord.apply_async()

        celery_logger.info(
            f"{log_prefix} Dispatched 'process_and_insert_sub_batch' sub-chord. ID: {chord_result.id}"
        )

        checkpoint_manager.set_chunk_status(
            workflow_name,
            chunk_idx,
            CHUNK_STATUS_PROCESSING_SUBTASKS,
            {"sub_chord_id": chord_result.id, "num_sub_batches": num_sub_batches},
        )

        total_dispatch_time = time.time() - start_time
        celery_logger.info(
            f"{log_prefix} Dispatched new sub-tasks in {total_dispatch_time:.2f}s."
        )
        return {
            "status": "success_new_sub_tasks_dispatched",
            "chunk_idx": chunk_idx,
            "sub_chord_id": chord_result.id,
            "num_sub_batches": num_sub_batches,
        }

    except (ChunkDataValidationError, IndexError) as e:
        celery_logger.error(f"{log_prefix} Data validation error: {e}", exc_info=False)
        error_payload = {
            "error_message": str(e),
            "error_type": type(e).__name__,
            "attempt_count": workflow_attempt_count + 1,
        }
        checkpoint_manager.set_chunk_status(
            workflow_name, chunk_idx, CHUNK_STATUS_FAILED_PERMANENT, error_payload
        )
        return {
            "status": "failed_permanent_data",
            "chunk_idx": chunk_idx,
            "error": str(e),
        }

    except SystemicWorkflowError as e:
        celery_logger.critical(
            f"{log_prefix} Systemic workflow error: {e}", exc_info=True
        )
        checkpoint_manager.update_workflow(
            table_name=workflow_name,
            status=CHUNK_STATUS_ERROR,
            last_error=f"Systemic error from chunk {chunk_idx}: {str(e)}",
        )

        raise

    except (OperationalError, DisconnectionError) as e:
        celery_logger.warning(
            f"{log_prefix} Transient DB/network error (Celery attempt {self.request.retries + 1}/{self.max_retries}): {e}"
        )
        try:
            raise self.retry(exc=e, countdown=int(2 ** (self.request.retries + 1)))
        except MaxRetriesExceededError:
            celery_logger.error(
                f"{log_prefix} Max Celery retries for DB/network error on process_chunk. Marking chunk FAILED_RETRYABLE."
            )
            error_payload = {
                "error_message": f"Max Celery retries for DB/network error in process_chunk: {str(e)}",
                "error_type": type(e).__name__,
                "attempt_count": workflow_attempt_count + 1,
                "celery_task_id": self.request.id,
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_FAILED_RETRYABLE, error_payload
            )
            return {
                "status": "failed_celery_retries_db_error_process_chunk",
                "chunk_idx": chunk_idx,
                "marked_retryable_in_checkpoint": True,
            }
        except Exception as retry_exc:
            celery_logger.error(
                f"{log_prefix} Error during Celery retry mechanism for DB error: {retry_exc}",
                exc_info=True,
            )
            error_payload = {
                "error_message": f"Celery retry mechanism failed for DB error in process_chunk: {str(retry_exc)}",
                "error_type": "CeleryRetryMechanismError",
                "attempt_count": workflow_attempt_count + 1,
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_FAILED_RETRYABLE, error_payload
            )
            return {
                "status": "failed_celery_retry_db_mechanism_error_process_chunk",
                "chunk_idx": chunk_idx,
                "marked_retryable_in_checkpoint": True,
            }

    except Exception as e:
        celery_logger.error(
            f"{log_prefix} Unexpected error in process_chunk (Celery attempt {self.request.retries + 1}/{self.max_retries}): {e}",
            exc_info=True,
        )
        try:
            raise self.retry(exc=e, countdown=int(2 ** (self.request.retries + 1)))
        except MaxRetriesExceededError:
            celery_logger.error(
                f"{log_prefix} Max Celery retries for unexpected error in process_chunk. Marking chunk FAILED_RETRYABLE."
            )
            error_payload = {
                "error_message": f"Max Celery retries for unexpected error in process_chunk: {str(e)}",
                "error_type": type(e).__name__,
                "attempt_count": workflow_attempt_count + 1,
                "celery_task_id": self.request.id,
                "traceback_summary": f"Max Celery retries for process_chunk unexpected error: {type(e).__name__}",
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_FAILED_RETRYABLE, error_payload
            )
            return {
                "status": "failed_celery_retries_unexpected_error_process_chunk",
                "chunk_idx": chunk_idx,
                "marked_retryable_in_checkpoint": True,
            }
        except Exception as retry_exc:
            celery_logger.error(
                f"{log_prefix} Error during Celery retry mechanism for unexpected error: {retry_exc}",
                exc_info=True,
            )
            error_payload = {
                "error_message": f"Celery retry mechanism failed for unexpected error: {str(retry_exc)}",
                "error_type": "CeleryRetryMechanismError",
                "attempt_count": workflow_attempt_count + 1,
            }
            checkpoint_manager.set_chunk_status(
                workflow_name, chunk_idx, CHUNK_STATUS_FAILED_RETRYABLE, error_payload
            )
            return {
                "status": "failed_celery_retry_unexpected_mechanism_error_process_chunk",
                "chunk_idx": chunk_idx,
                "marked_retryable_in_checkpoint": True,
            }


@celery.task(
    name="workflow:rebuild_indices_for_spatial_lookup", bind=True, acks_late=True
)
def rebuild_indices_for_spatial_lookup(self, mat_metadata: list, database: str):
    """Rebuild indices for a table after spatial lookup completion."""
    engine = db_manager.get_engine(database)
    segmentation_table_name = mat_metadata["segmentation_table_name"]

    seg_model = create_segmentation_model(mat_metadata)

    index_cache.drop_table_indices(
        segmentation_table_name, engine, drop_primary_key=True
    )

    seg_indices = index_cache.add_indices_sql_commands(
        table_name=segmentation_table_name, model=seg_model, engine=engine
    )

    if seg_indices:
        add_final_tasks = [add_index.si(database, command) for command in seg_indices]
        add_final_tasks.append(update_metadata.si(mat_metadata))
        add_final_tasks.append(
            workflow_complete.si(
                f"Spatial Lookup for {segmentation_table_name} completed"
            )
        )
        return chain(add_final_tasks)
    else:
        celery_logger.info(
            f"No indices to add for {segmentation_table_name}. Skipping index creation."
        )
        return update_metadata.si(mat_metadata).apply_async(
            link=workflow_complete.si(
                f"Spatial Lookup for {segmentation_table_name} completed"
            )
        )


def get_pts_from_bbox(database, min_corner, max_corner, mat_info):
    try:
        with db_manager.get_engine(database).begin() as connection:
            query = select_all_points_in_bbox(min_corner, max_corner, mat_info)
            result = connection.execute(select([query]))

            df = pd.DataFrame(result.fetchall())

            if df.empty:
                return None

            df.columns = result.keys()

            df["pt_position"] = df["pt_position"].apply(
                lambda pt: get_geom_from_wkb(pt)
            )
            return df

    except Exception as e:
        celery_logger.error(f"Error in get_pts_from_bbox: {str(e)}")
        celery_logger.error(f"min_corner: {min_corner}, max_corner: {max_corner}")
        celery_logger.error(f"aligned_volume: {mat_info.get('aligned_volume')}")
        raise e


def match_point_and_get_value(point, points_map):
    return points_map.get(str(point), 0)


def normalize_positions(point, scale_factor):
    scaled_point = np.floor(np.array(point) / scale_factor).astype(int)
    return tuple(scaled_point)


def point_to_chunk_position(cv, pt, mip=None):
    """
    Convert a point into the chunk position.

    pt: x,y,z triple
    mip:
      if None, pt is in physical coordinates
      else pt is in the coordinates of the indicated mip level

    Returns: Vec(chunk_x,chunk_y,chunk_z)
    """
    pt = Vec(*pt, dtype=np.float64)

    if mip is not None:
        pt *= cv.resolution(mip)

    pt /= cv.resolution(cv.watershed_mip)

    if cv.chunks_start_at_voxel_offset:
        pt -= cv.voxel_offset(cv.watershed_mip)

    return (pt // cv.graph_chunk_size).astype(np.int32)


def get_root_ids_from_supervoxels(
    materialization_data: dict, mat_metadata: dict
) -> dict:
    """Get root ids for each supervoxel for multiple point types.

    Args:
        materialization_data (dict): Data containing supervoxel IDs to process
        mat_metadata (dict): Materialization metadata with database info

    Returns:
        dict: Complete data with supervoxel IDs and their corresponding root IDs
    """
    start_time = time.time()

    pcg_table_name = mat_metadata.get("pcg_table_name")
    database = mat_metadata.get("database")

    try:
        materialization_time_stamp = datetime.datetime.strptime(
            mat_metadata.get("materialization_time_stamp"), "%Y-%m-%d %H:%M:%S.%f"
        )
    except ValueError:
        materialization_time_stamp = datetime.datetime.strptime(
            mat_metadata.get("materialization_time_stamp"), "%Y-%m-%dT%H:%M:%S.%f"
        )

    supervoxel_df = pd.DataFrame(materialization_data, dtype=object)

    drop_col_names = list(
        supervoxel_df.loc[:, supervoxel_df.columns.str.endswith("position")]
    )
    supervoxel_df = supervoxel_df.drop(labels=drop_col_names, axis=1)

    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=True)
    SegmentationModel = create_segmentation_model(mat_metadata)

    __, seg_model_cols, __ = get_query_columns_by_suffix(
        AnnotationModel, SegmentationModel, "root_id"
    )

    anno_ids = supervoxel_df["id"].tolist()

    root_ids_df = supervoxel_df.copy()

    supervoxel_col_names = [
        col for col in supervoxel_df.columns if col.endswith("supervoxel_id")
    ]

    root_id_col_names = [
        col.replace("supervoxel_id", "root_id") for col in supervoxel_col_names
    ]

    for col in root_id_col_names:
        if col not in root_ids_df.columns:
            root_ids_df[col] = 0

    existing_root_ids = {}
    with db_manager.session_scope(database) as session:
        try:
            results = (
                session.query(*seg_model_cols)
                .filter(SegmentationModel.id.in_(anno_ids))
                .all()
            )

            if results:
                existing_df = pd.DataFrame(results)
                for i, row in existing_df.iterrows():
                    row_id = row.get("id")
                    if row_id:
                        existing_root_ids[row_id] = row.to_dict()
        except Exception as e:
            celery_logger.warning(f"Error querying existing root IDs: {str(e)}")

    for idx, row in root_ids_df.iterrows():
        row_id = row["id"]
        if row_id in existing_root_ids:
            for root_col in root_id_col_names:
                existing_value = existing_root_ids[row_id].get(root_col)
                if existing_value and existing_value > 0:
                    root_ids_df.at[idx, root_col] = existing_value

    cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)

    for sv_col in supervoxel_col_names:
        root_col = sv_col.replace("supervoxel_id", "root_id")

        sv_mask = (root_ids_df[sv_col] > 0) & (
            (root_ids_df[root_col].isna()) | (root_ids_df[root_col] == 0)
        )

        if not sv_mask.any():
            continue

        supervoxels_to_lookup = root_ids_df.loc[sv_mask, sv_col]
        celery_logger.info(
            f"Looking up {len(supervoxels_to_lookup)} root IDs for {sv_col}"
        )

        if not supervoxels_to_lookup.empty:
            try:
                root_ids = get_root_ids(
                    cg_client, supervoxels_to_lookup, materialization_time_stamp
                )

                root_ids_df.loc[sv_mask, root_col] = root_ids

                zero_root_idx = supervoxels_to_lookup.index[root_ids == 0]
                if len(zero_root_idx) > 0:
                    zero_sv_ids = supervoxels_to_lookup.loc[zero_root_idx]
                    celery_logger.warning(
                        f"Found {len(zero_sv_ids)} supervoxels with no "
                        f"corresponding root IDs for {sv_col}: {zero_sv_ids.tolist()[:5]}..."
                    )
            except Exception as e:
                celery_logger.error(f"Error looking up root IDs for {sv_col}: {str(e)}")

    total_time = time.time() - start_time
    celery_logger.info(
        f"Root ID lookup complete: processed {len(root_ids_df)} rows "
        f"with {len(supervoxel_col_names)} supervoxel columns in {total_time:.2f}s"
    )

    return root_ids_df.to_dict(orient="records")


def select_3D_points_in_bbox(
    table_model: str, spatial_column_name: str, min_corner: List, max_corner: List
) -> select:
    """Generate a sqlalchemy statement that selects all points in the bounding box.

    Args:
        table_model (str): Annotation table model
        spatial_column_name (str): Name of the spatial column
        min_corner (List): Min corner of the bounding box
        max_corner (List): Max corner of the bounding box

    Returns:
        select: sqlalchemy statement that selects all points in the bounding box
    """
    start_coord = np.array2string(min_corner).strip("[]")
    end_coord = np.array2string(max_corner).strip("[]")

    # Format raw SQL string
    spatial_column = getattr(table_model, spatial_column_name)
    return select(
        [
            table_model.id.label("id"),
            spatial_column.label("pt_position"),
            literal(spatial_column.name.split("_", 1)[0]).label("type"),
        ]
    ).where(
        spatial_column.intersects_nd(
            func.ST_3DMakeBox(f"POINTZ({start_coord})", f"POINTZ({end_coord})")
        )
    )


def select_all_points_in_bbox(
    min_corner: np.array,
    max_corner: np.array,
    mat_info: dict,
) -> union_all:
    """Iterates through each Point column in the annotation table and creates
    a query of the union of all points in the bounding box.

    Args:
        min_corner (np.array): Min corner of the bounding box
        max_corner (np.array): Max corner of the bounding box
        mat_info (dict): Materialization info for a given table

    Returns:
        union_all: sqlalchemy statement that creates the union of all points
                   for all geometry columns in the bounding box
    """
    db = dynamic_annotation_cache.get_db(mat_info["database"])
    table_name = mat_info["annotation_table_name"]
    schema = db.database.get_table_schema(table_name)
    mat_info["schema"] = schema
    AnnotationModel = create_annotation_model(mat_info)
    SegmentationModel = create_segmentation_model(mat_info)

    spatial_columns = []
    for annotation_column in AnnotationModel.__table__.columns:
        if (
            isinstance(annotation_column.type, Geometry)
            and "Z" in annotation_column.type.geometry_type.upper()
        ):
            supervoxel_column_name = (
                f"{annotation_column.name.rsplit('_', 1)[0]}_supervoxel_id"
            )
            # skip lookup for column if not in Segmentation Model
            if getattr(SegmentationModel, supervoxel_column_name, None):
                spatial_columns.append(
                    annotation_column.name
                )  # use column name instead of Column object
            else:
                continue
    selects = [
        select_3D_points_in_bbox(
            AnnotationModel, spatial_column, min_corner, max_corner
        )
        for spatial_column in spatial_columns
    ]
    return union_all(*selects).alias("points_in_bbox")


def convert_array_to_int(value):
    # Check if the value is a NumPy array
    if isinstance(value, np.ndarray):
        # Convert a single-element NumPy array to an integer
        return (
            value[0] if value.size == 1 else 0
        )  # Replace 0 with appropriate default value
    elif isinstance(value, int):
        # If the value is already an integer, return it as is
        return value
    else:
        # Handle other unexpected data types, perhaps with a default value or an error
        return 0


def insert_segmentation_data(
    data: pd.DataFrame,
    mat_metadata: dict,
):
    """Inserts the segmentation data into the database using PostgreSQL's upsert functionality.

    Args:
        data (pd.DataFrame): Dataframe containing the segmentation data
        mat_metadata (dict): Materialization info for a given table

    Returns:
        int: Number of rows affected in the database
    """
    start_time = time.time()
    database = mat_metadata["database"]
    SegmentationModel = create_segmentation_model(mat_metadata)

    seg_columns = SegmentationModel.__table__.columns.keys()
    segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
    data_df = pd.DataFrame(data, dtype=object)

    # Convert supervoxel IDs
    supervoxel_id_cols = [
        col for col in data_df.columns if col.endswith("_supervoxel_id")
    ]
    for col in supervoxel_id_cols:
        data_df[col] = data_df[col].apply(convert_array_to_int)

    common_cols = segmentation_dataframe.columns.intersection(data_df.columns)
    df = pd.merge(
        segmentation_dataframe[common_cols], data_df[common_cols], how="right"
    )
    df = df.infer_objects().fillna(0)
    df = df.reindex(columns=segmentation_dataframe.columns, fill_value=0)

    records = df.to_dict(orient="records")

    if not records:
        celery_logger.info("No records to process")
        return 0

    table = SegmentationModel.__table__

    update_columns = [col for col in seg_columns if col != "id"]

    with db_manager.session_scope(database) as session:
        rows_affected = 0

        chunk_size = 1000
        for i in range(0, len(records), chunk_size):
            chunk = records[i : i + chunk_size]

            if not chunk:
                continue

            stmt = insert(table).values(chunk)

            update_dict = {}
            for col in update_columns:
                update_dict[col] = case(
                    [(stmt.excluded[col] > 0, stmt.excluded[col])],
                    else_=getattr(table.c, col),
                )

            stmt = stmt.on_conflict_do_update(
                constraint=table.primary_key, set_=update_dict
            )

            result = session.execute(stmt)
            rows_affected += result.rowcount

        celery_logger.info(
            f"Total rows affected: {rows_affected} in {time.time() - start_time:.2f} seconds"
        )
        return rows_affected


@celery.task(
    name="workflow:process_and_insert_sub_batch",
    bind=True,
    acks_late=True,
    autoretry_for=(OperationalError, DisconnectionError, ChunkDataValidationError),
    max_retries=3,
    retry_backoff=True,
)
def process_and_insert_sub_batch(
    self: Task,
    sub_batch_point_data: List[Dict],
    mat_info: Dict,
    database_name: str,
    original_chunk_idx: int,
    sub_batch_idx: int,
    workflow_name: str,
):
    """
    Processes a sub-batch of points:
    1. Looks up supervoxel IDs.
    2. Reconstructs data for root ID lookup (pivots).
    3. Looks up root IDs.
    4. Inserts segmentation data into the database.
    Retries on transient errors. Reports status upon completion or permanent failure.
    """
    log_prefix = f"[WF:{workflow_name}, SpChunk:{original_chunk_idx}, SubBatch:{sub_batch_idx}, Task:{self.request.id}]"
    celery_logger.info(
        f"{log_prefix} Starting processing for {len(sub_batch_point_data)} points."
    )

    if not sub_batch_point_data:
        celery_logger.info(
            f"{log_prefix} Received empty sub_batch_point_data. Nothing to process."
        )
        return {
            "status": "success_empty_batch",
            "rows_processed": 0,
            "sub_batch_idx": sub_batch_idx,
        }

    try:
        points_for_sv_lookup = [item["pt_position"] for item in sub_batch_point_data]
        if not points_for_sv_lookup:
            celery_logger.info(
                f"{log_prefix} No 'pt_position' found in sub_batch_point_data. Nothing to look up."
            )
            return {
                "status": "success_no_points_for_lookup",
                "rows_processed": 0,
                "sub_batch_idx": sub_batch_idx,
            }

        segmentation_source = mat_info["segmentation_source"]
        coord_resolution_xyz = mat_info["coord_resolution"]
        cv = cloudvolume_cache.get_cv(segmentation_source)

        sv_data_tuple_keys = cv.scattered_points(
            points_for_sv_lookup, coord_resolution=coord_resolution_xyz
        )
        sv_id_data_aggregated = {str(k): int(v) for k, v in sv_data_tuple_keys.items()}
        celery_logger.info(
            f"{log_prefix} Supervoxel lookup found {len(sv_id_data_aggregated)} mappings for {len(points_for_sv_lookup)} points."
        )

        if not sv_id_data_aggregated:
            celery_logger.info(f"{log_prefix} No supervoxels found for this sub-batch.")
            return {
                "status": "success_no_supervoxels_found",
                "rows_processed": 0,
                "sub_batch_idx": sub_batch_idx,
            }

        scale_factor = cv.resolution / np.array(coord_resolution_xyz)

        pivoted_data_map = {}
        for item_dict in sub_batch_point_data:
            item_id = item_dict["id"]
            original_pt_position = item_dict["pt_position"]
            scaled_pos_tuple = normalize_positions(original_pt_position, scale_factor)
            svid = match_point_and_get_value(scaled_pos_tuple, sv_id_data_aggregated)

            type_str = item_dict.get("type", "unknown")
            col_name = (
                f"{type_str}_supervoxel_id"
                if "pt" in type_str
                else f"{type_str}_pt_supervoxel_id"
            )

            if item_id not in pivoted_data_map:
                pivoted_data_map[item_id] = {"id": item_id}
            pivoted_data_map[item_id][col_name] = svid

        pivoted_data_for_root_lookup = list(pivoted_data_map.values())

        if not pivoted_data_for_root_lookup:
            celery_logger.info(
                f"{log_prefix} No data after pivoting for root ID lookup."
            )
            return {
                "status": "success_empty_pivot",
                "rows_processed": 0,
                "sub_batch_idx": sub_batch_idx,
            }

        mat_info_for_root_lookup = mat_info.copy()
        if isinstance(
            mat_info_for_root_lookup.get("materialization_time_stamp"),
            datetime.datetime,
        ):
            mat_info_for_root_lookup["materialization_time_stamp"] = (
                mat_info_for_root_lookup["materialization_time_stamp"].isoformat()
            )

        root_id_data = get_root_ids_from_supervoxels(
            pivoted_data_for_root_lookup, mat_info_for_root_lookup
        )

        affected_rows = 0
        if root_id_data and len(root_id_data) > 0:
            celery_logger.info(
                f"{log_prefix} Inserting/updating {len(root_id_data)} root ID records."
            )
            affected_rows = insert_segmentation_data(
                root_id_data, mat_info_for_root_lookup
            )
            celery_logger.info(
                f"{log_prefix} DB insert/update affected {affected_rows} rows."
            )
        else:
            celery_logger.info(
                f"{log_prefix} No root ID data produced to insert for this sub-batch."
            )

        return {
            "status": "success",
            "rows_processed": affected_rows,
            "sub_batch_idx": sub_batch_idx,
        }

    except ChunkDataValidationError as e:
        celery_logger.error(
            f"{log_prefix} Non-retryable ChunkDataValidationError: {e}", exc_info=False
        )
        raise

    except (OperationalError, DisconnectionError) as e:
        celery_logger.warning(
            f"{log_prefix} Transient DB/network error: {e}. Celery will retry."
        )
        raise self.retry(exc=e)

    except Exception as e:
        celery_logger.error(
            f"{log_prefix} Unexpected error in sub-batch processing: {e}", exc_info=True
        )
        try:
            raise self.retry(exc=e)
        except MaxRetriesExceededError:
            celery_logger.error(
                f"{log_prefix} Max retries exceeded for unexpected error. Sub-batch failed permanently."
            )
            raise
        except Exception as final_e:
            celery_logger.error(
                f"{log_prefix} Could not invoke retry for unexpected error: {final_e}. Sub-batch failed permanently."
            )
            raise


@celery.task(
    name="workflow:finalize_chunk_outcome",
    bind=True,
    acks_late=True,
    autoretry_for=(OperationalError, DisconnectionError),
    max_retries=3,
    retry_backoff=True,
)
def finalize_chunk_outcome(
    self: Task,
    results_list: List[Dict],
    original_chunk_idx: int,
    mat_info: Dict,
    database_name: str,
    workflow_name: str,
    total_expected_sub_batches: int,
    min_corner: List[float],
    max_corner: List[float],
):
    """
    Finalizes the outcome of a spatial chunk after all its sub-batches
    (process_and_insert_sub_batch) have been processed.
    This task acts as the body of a Celery chord.
    """
    log_prefix = f"[WF:{workflow_name}, SpChunk:{original_chunk_idx}, FinalizeTask:{self.request.id}]"
    celery_logger.info(
        f"{log_prefix} Starting finalization. Received {len(results_list)} results for {total_expected_sub_batches} expected sub-batches."
    )

    checkpoint_manager = RedisCheckpointManager(database_name)

    total_rows_processed = 0
    successful_sub_batches = 0
    failed_sub_batches_count = 0

    for result in results_list:
        if result and isinstance(result, dict) and result.get("status") == "success":
            total_rows_processed += result.get("rows_processed", 0)
            successful_sub_batches += 1
        else:
            failed_sub_batches_count += 1
            celery_logger.warning(
                f"{log_prefix} Sub-batch result indicates failure or is missing: {result}"
            )

    if successful_sub_batches < total_expected_sub_batches:
        celery_logger.warning(
            f"{log_prefix} {successful_sub_batches}/{total_expected_sub_batches} sub-batches succeeded. "
            f"{failed_sub_batches_count} sub-batches appear to have failed."
        )

    try:
        status_payload = {
            "rows_processed": total_rows_processed,
            "message": f"Finalized chunk processing. {successful_sub_batches}/{total_expected_sub_batches} sub-batches successful.",
            "chunk_bounding_box": {"min_corner": min_corner, "max_corner": max_corner},
            "successful_sub_batches": successful_sub_batches,
            "failed_sub_batches": failed_sub_batches_count,
        }
        checkpoint_manager.set_chunk_status(
            workflow_name, original_chunk_idx, CHUNK_STATUS_COMPLETED, status_payload
        )
        celery_logger.info(
            f"{log_prefix} Successfully finalized and marked chunk {original_chunk_idx} as COMPLETED. "
            f"Total rows processed in this chunk: {total_rows_processed}."
        )
        return {
            "status": "success",
            "original_chunk_idx": original_chunk_idx,
            "rows_processed": total_rows_processed,
            "successful_sub_batches": successful_sub_batches,
        }
    except (OperationalError, DisconnectionError) as e:
        celery_logger.error(
            f"{log_prefix} Transient error updating checkpoint: {type(e).__name__}: {e}",
            exc_info=True,
        )
        raise self.retry(exc=e)
    except Exception as e_final:
        celery_logger.error(
            f"{log_prefix} Error during finalization (root ID lookup or DB insert): {type(e_final).__name__}: {e_final}",
            exc_info=True,
        )
        error_payload = {
            "error_message": f"Finalization error: {str(e_final)}",
            "error_type": type(e_final).__name__,
        }
        checkpoint_manager.set_chunk_status(
            workflow_name,
            original_chunk_idx,
            CHUNK_STATUS_FAILED_RETRYABLE,
            error_payload,
        )
        raise


@celery.task(name="workflow:process_chunk_sub_chord_failed", bind=True, acks_late=True)
def process_chunk_sub_chord_failed(
    self: Task,
    request,
    exc,
    traceback,
    original_chunk_idx: int,
    workflow_name: str,
    database_name: str,
    parent_chunk_attempt_number: int,
):
    """
    Handles failures within the sub-batch processing chord for a given spatial chunk.
    Updates the chunk's status in Redis to FAILED_RETRYABLE or FAILED_PERMANENT
    based on the parent chunk's attempt count.
    """
    log_prefix = f"[WF:{workflow_name}, SpChunk:{original_chunk_idx}, SubChordFailTask:{self.request.id}, ParentAttempt:{parent_chunk_attempt_number}]"

    error_message_detail = (
        f"Sub-chord processing failed. Exception: {type(exc).__name__}: {str(exc)}"
    )
    celery_logger.error(
        f"{log_prefix} {error_message_detail}\\nTraceback:\\n{traceback}"
    )

    checkpoint_manager = RedisCheckpointManager(database_name)

    next_status: str
    final_error_message: str

    if parent_chunk_attempt_number >= MAX_CHUNK_WORKFLOW_ATTEMPTS:
        next_status = CHUNK_STATUS_FAILED_PERMANENT
        final_error_message = (
            f"Sub-chord failed. Parent chunk attempt {parent_chunk_attempt_number} has reached/exceeded "
            f"max attempts ({MAX_CHUNK_WORKFLOW_ATTEMPTS}). Marking chunk FAILED_PERMANENT. "
            f"Error: {error_message_detail}"
        )
        celery_logger.error(f"{log_prefix} {final_error_message}")
    else:
        next_status = CHUNK_STATUS_FAILED_RETRYABLE
        final_error_message = (
            f"Sub-chord failed. Parent chunk attempt {parent_chunk_attempt_number}. "
            f"Marked chunk FAILED_RETRYABLE for workflow {workflow_name}. Error: {error_message_detail}"
        )
        celery_logger.warning(f"{log_prefix} {final_error_message}")

    error_payload = {
        "error_message": final_error_message,
        "error_type": type(exc).__name__,
        "attempt_count": parent_chunk_attempt_number,
        "failed_chord_id": (request.id if request else "Unknown"),
        "traceback_snippet": str(traceback)[:1000] if traceback else "N/A",
    }

    try:
        checkpoint_manager.set_chunk_status(
            workflow_name, original_chunk_idx, next_status, error_payload
        )
        celery_logger.info(
            f"{log_prefix} Successfully updated chunk {original_chunk_idx} status to {next_status}."
        )
    except Exception as e_redis:
        celery_logger.error(
            f"{log_prefix} CRITICAL: Failed to update checkpoint for chunk {original_chunk_idx} to {next_status} after sub-chord failure. Error: {e_redis}",
            exc_info=True,
        )


@celery.task(name="workflow:workflow_failed", bind=True, acks_late=True)
def spatial_workflow_failed(
    self: Task,
    request_obj_uuid_or_exc: object = None,
    task_id_or_traceback: object = None,
    workflow_name: str = None,
    database_name: str = None,
    custom_message: str = None,
    chunk_idx: int = None,
):
    """
    Handles the failure of a workflow or process chunk.

    If called as error link from a chord failure (e.g., chord of process_chunk tasks):
        request_obj_uuid_or_exc: UUID of the chord that failed.
        task_id_or_traceback: Usually None or a GroupResult ID from Celery internals.
    If called after a task failure where an exception is caught and propagated:
        request_obj_uuid_or_exc: Exception instance.
        task_id_or_traceback: Traceback string.
    """
    final_status = CHUNK_STATUS_FAILED_PERMANENT
    error_info = custom_message or "Unknown workflow error"
    log_message_prefix = (
        f"[WF:{workflow_name or 'UnknownWF'}, DB:{database_name or 'UnknownDB'}]"
    )
    if chunk_idx is not None:
        log_message_prefix += f", SpChunk:{chunk_idx}"

    actual_exc = None
    actual_traceback = None

    if isinstance(request_obj_uuid_or_exc, Exception):
        actual_exc = request_obj_uuid_or_exc
        if isinstance(task_id_or_traceback, str):
            actual_traceback = task_id_or_traceback
        extracted_error_message = (
            f"Task error: {type(actual_exc).__name__}: {str(actual_exc)}"
        )
        error_info = (
            f"{custom_message} | {extracted_error_message}"
            if custom_message
            else extracted_error_message
        )
    elif isinstance(request_obj_uuid_or_exc, str):
        error_info = f"Chord/Task {request_obj_uuid_or_exc} failed. Message: {custom_message or 'Chord failure'}"
    elif custom_message:
        error_info = custom_message

    celery_logger.error(
        f"{log_message_prefix} Workflow/Process failed. Final Error: {error_info}"
    )
    if actual_exc and actual_traceback:
        celery_logger.error(f"{log_message_prefix} Traceback:\n{actual_traceback}")
    elif isinstance(task_id_or_traceback, str):
        celery_logger.error(
            f"{log_message_prefix} Traceback provided:\n{task_id_or_traceback}"
        )

    if workflow_name and database_name:
        checkpoint_manager = RedisCheckpointManager(database_name)
        try:
            checkpoint_manager.update_workflow(
                table_name=workflow_name, status=final_status, last_error=error_info
            )
            celery_logger.info(
                f"{log_message_prefix} Updated workflow {workflow_name} status to {final_status} in Redis."
            )
        except Exception as e_redis:
            celery_logger.error(
                f"{log_message_prefix} Could not update Redis for failed workflow {workflow_name}: {e_redis}"
            )
    else:
        celery_logger.error(
            f"{log_message_prefix} spatial_workflow_failed called without workflow_name or database_name. Cannot update Redis status for workflow."
        )
