import datetime
import json
import time
from typing import List

import numpy as np
import pandas as pd
from celery import chain, chord
from celery.utils.log import get_task_logger
from cloudvolume.lib import Vec
from geoalchemy2 import Geometry
from sqlalchemy import (
    func,
    literal,
    select,
    union_all,
)
from sqlalchemy.exc import DisconnectionError, OperationalError
from sqlalchemy.ext.declarative import declarative_base

from materializationengine.blueprints.upload.checkpoint_manager import (
    RedisCheckpointManager,
)
from materializationengine.celery_init import celery
from materializationengine.cloudvolume_gateway import cloudvolume_cache
from materializationengine.database import db_manager, dynamic_annotation_cache
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import (
    add_index,
    get_materialization_info,
    monitor_workflow_state,
    update_metadata,
    workflow_complete,
)
from materializationengine.throttle import throttle_celery
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
    get_config_param,
    get_geom_from_wkb,
)
from materializationengine.workflows.chunking import ChunkingStrategy
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_new_root_ids,
)

Base = declarative_base()

celery_logger = get_task_logger(__name__)


@celery.task(
    name="workflow:run_spatial_lookup_workflow",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=True,
)
def run_spatial_lookup_workflow(
    self,
    datastack_info: dict,
    table_name: str,
    chunk_scale_factor: int = 1,
    supervoxel_batch_size: int = 50,
    get_root_ids: bool = True,
    upload_to_database: bool = True,
    use_staging_database: bool = False,
    resume_from_checkpoint: bool = False,
):
    """Spatial Lookup Workflow processes a table's points in chunks and inserts supervoxel IDs into the database."""
    start_time = time.time()

    staging_database = get_config_param("STAGING_DATABASE_NAME")
    database = (
        staging_database
        if use_staging_database
        else datastack_info["aligned_volume"]["name"]
    )

    checkpoint_manager = RedisCheckpointManager(database)

    existing_workflow = checkpoint_manager.get_workflow_data(table_name)
    should_resume = (
        resume_from_checkpoint
        and existing_workflow
        and existing_workflow.status not in ["completed", "failed"]
    )
    if should_resume:
        celery_logger.info(
            f"Resuming existing workflow for {table_name} from checkpoint"
        )
        checkpoint_manager.update_workflow(
            table_name=table_name, status="resuming", task_id=self.request.id
        )

        if existing_workflow.last_processed_chunk:
            celery_logger.info(
                f"Resuming from chunk index {existing_workflow.last_processed_chunk.get('index', 0)} "
                f"with {existing_workflow.completed_chunks or 0} completed chunks"
            )
    else:
        if existing_workflow and not resume_from_checkpoint:
            celery_logger.info(
                f"Starting fresh workflow for {table_name}, ignoring existing checkpoint"
            )
        else:
            celery_logger.info(f"Starting new workflow for {table_name}")

        checkpoint_manager.initialize_workflow(table_name, self.request.id)
        checkpoint_manager.update_workflow(
            table_name=table_name,
            status="initializing",
            last_processed_chunk=None,
            completed_chunks=0,
            mat_info_idx=0,
        )

    materialization_time_stamp = datetime.datetime.utcnow()

    table_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
        table_name=table_name,
        skip_row_count=True,
        database=database,
    )

    checkpoint_manager.update_workflow(
        table_name=table_name, total_chunks=len(table_info), status="processing"
    )

    table_tasks = []

    mat_info_idx, _ = checkpoint_manager.get_checkpoint_state(table_name)

    for i, mat_metadata in enumerate(table_info):
        if i < mat_info_idx:
            celery_logger.info(f"Skipping already processed table {i} for {table_name}")
            continue

        table_task = process_table_in_chunks.si(
            datastack_info=datastack_info,
            mat_metadata=mat_metadata,
            database=database,
            chunk_scale_factor=chunk_scale_factor,
            supervoxel_batch_size=supervoxel_batch_size,
            get_root_ids=get_root_ids,
            upload_to_database=upload_to_database,
            table_name=table_name,
            mat_info_idx=i,
        )
        table_tasks.append(table_task)

    if table_tasks:
        workflow = chord(
            table_tasks,
            chain(
                update_workflow_status.si(database, table_name, "completed"),
                workflow_complete.si(
                    f"Spatial Lookup workflow for {table_name} completed"
                ),
            ),
        ).on_error(
            spatial_workflow_failed.s(
                mat_info=mat_metadata, workflow_name=f"spatial_lookup_{table_name}"
            )
        )

        workflow_result = workflow.apply_async()

        is_complete = monitor_workflow_state(workflow_result)
        completion_time = time.time() - start_time
        celery_logger.info(
            f"Spatial lookup workflow for {table_name} initiated "
            f"in {completion_time:.2f}s for {len(table_info)} tables"
        )

        return {
            "status": "in_progress",
            "workflow_id": self.request.id,
            "tables_count": len(table_info),
            "tables_remaining": len(table_tasks),
            "execution_time": completion_time,
        }
    else:
        checkpoint_manager.update_workflow(table_name=table_name, status="completed")

        return {
            "status": "completed",
            "tables_count": len(table_info),
            "tables_remaining": 0,
            "message": "No tables to process for spatial lookup",
        }


@celery.task(name="workflow:update_workflow_status")
def update_workflow_status(database, table_name, status):
    """Helper task to update workflow status."""
    checkpoint_manager = RedisCheckpointManager(database)

    checkpoint_manager.update_workflow(table_name=table_name, status=status)
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
    datastack_info,
    mat_metadata,
    database,
    chunk_scale_factor,
    supervoxel_batch_size,
    get_root_ids,
    upload_to_database,
    table_name,
    mat_info_idx,
    chunk_offset=0,
    batch_size=100,
    chunking_info=None,
):
    """Process a table in chunks, respecting memory constraints and throttling."""
    try:
        engine = db_manager.get_engine(database)

        checkpoint_manager = RedisCheckpointManager(database)

        workflow_data = checkpoint_manager.get_workflow_data(table_name)
        if not workflow_data:
            workflow_data = checkpoint_manager.initialize_workflow(
                table_name, self.request.id
            )

        checkpoint_manager.update_workflow(
            table_name=table_name, mat_info_idx=mat_info_idx, status="processing_chunks"
        )

        if (
            workflow_data
            and workflow_data.last_processed_chunk
            and mat_info_idx == workflow_data.mat_info_idx
        ):
            chunk_offset = workflow_data.last_processed_chunk.index + 1
            celery_logger.info(f"Resuming from chunk {chunk_offset} for {table_name}")

            # If we have chunking info from workflow data, use it
            if workflow_data.chunking_strategy and workflow_data.used_chunk_size:
                chunking_info = {
                    "strategy_name": workflow_data.chunking_strategy,
                    "actual_chunk_size": workflow_data.used_chunk_size,
                    "total_chunks": workflow_data.total_chunks,
                    "min_coords": (
                        workflow_data.min_enclosing_bbox[0]
                        if workflow_data.min_enclosing_bbox
                        else None
                    ),
                    "max_coords": (
                        workflow_data.min_enclosing_bbox[1]
                        if workflow_data.min_enclosing_bbox
                        else None
                    ),
                }
                celery_logger.info(
                    f"Using chunking strategy from checkpoint: {workflow_data.chunking_strategy}"
                )

        create_missing_segmentation_table(mat_metadata)

        index_cache.drop_table_indices(
            mat_metadata["segmentation_table_name"], engine, drop_primary_key=False
        )

        chunking = ChunkingStrategy(
            engine=engine,
            table_name=table_name,
            database=database,
            base_chunk_size=chunk_scale_factor * 1024,
        )

        if chunking_info:
            if isinstance(chunking_info, dict):
                chunking.strategy_name = chunking_info.get("strategy_name", "grid")
                chunking.actual_chunk_size = chunking_info.get(
                    "actual_chunk_size", chunking.base_chunk_size
                )

                if "total_chunks" in chunking_info and isinstance(
                    chunking_info["total_chunks"], int
                ):
                    chunking.total_chunks = chunking_info["total_chunks"]

                if (
                    "min_coords" in chunking_info
                    and chunking_info["min_coords"] is not None
                ):
                    chunking.min_coords = np.array(chunking_info["min_coords"])
                if (
                    "max_coords" in chunking_info
                    and chunking_info["max_coords"] is not None
                ):
                    chunking.max_coords = np.array(chunking_info["max_coords"])
                if "estimated_rows" in chunking_info:
                    chunking.estimated_rows = chunking_info["estimated_rows"]
            else:
                celery_logger.warning(
                    f"chunking_info is not a dictionary: {type(chunking_info)}"
                )

            celery_logger.info(
                f"Using precomputed chunking strategy with {chunking.total_chunks} chunks"
            )
        else:
            celery_logger.info("Calculating spatial chunking strategy (first run)")
            chunking.select_strategy()

            checkpoint_manager.update_workflow(
                table_name=table_name,
                chunking_strategy=chunking.strategy_name,
                used_chunk_size=chunking.actual_chunk_size,
                total_chunks=chunking.total_chunks,
                total_row_estimate=chunking.estimated_rows,
                min_enclosing_bbox=(
                    [
                        (
                            chunking.min_coords.tolist()
                            if isinstance(chunking.min_coords, np.ndarray)
                            else chunking.min_coords
                        ),
                        (
                            chunking.max_coords.tolist()
                            if isinstance(chunking.max_coords, np.ndarray)
                            else chunking.max_coords
                        ),
                    ]
                    if chunking.min_coords is not None
                    and chunking.max_coords is not None
                    else None
                ),
            )

        if chunking.total_chunks <= 0:
            celery_logger.warning(
                f"total_chunks was 0 or not set. Forcing strategy calculation for {table_name}"
            )
            chunking.select_strategy()

            checkpoint_manager.update_workflow(
                table_name=table_name,
                chunking_strategy=chunking.strategy_name,
                used_chunk_size=chunking.actual_chunk_size,
                total_chunks=chunking.total_chunks,
                total_row_estimate=chunking.estimated_rows,
                min_enclosing_bbox=(
                    [
                        (
                            chunking.min_coords.tolist()
                            if isinstance(chunking.min_coords, np.ndarray)
                            else chunking.min_coords
                        ),
                        (
                            chunking.max_coords.tolist()
                            if isinstance(chunking.max_coords, np.ndarray)
                            else chunking.max_coords
                        ),
                    ]
                    if chunking.min_coords is not None
                    and chunking.max_coords is not None
                    else None
                ),
            )

            if chunking.total_chunks <= 0:
                celery_logger.warning(
                    f"total_chunks still 0 after strategy selection. Setting default value for {table_name}"
                )
                chunking.total_chunks = max(1, batch_size)

        celery_logger.info(
            f"Processing {table_name} with total_chunks={chunking.total_chunks}, chunk_offset={chunk_offset}"
        )

        total_chunks = chunking.total_chunks

        if chunk_offset >= total_chunks and total_chunks > 0:
            celery_logger.info(
                f"All {total_chunks} chunks processed for {table_name}. Rebuilding indices."
            )
            checkpoint_manager.update_workflow(
                table_name=table_name, status="rebuilding_indices"
            )
            return rebuild_indices_for_spatial_lookup.si(
                mat_metadata, database
            ).apply_async(
                link_error=spatial_workflow_failed.s(
                    mat_info=mat_metadata,
                    workflow_name=f"spatial_lookup_index_rebuild_{table_name}",
                )
            )

        batch_end = min(chunk_offset + batch_size, total_chunks)

        if mat_metadata.get("throttle_queues"):
            throttled = throttle_celery.wait_if_needed(queue_name="process")

        chunk_gen = chunking.skip_to_index(chunk_offset)()

        chunk_tasks = []
        chunk_idx = chunk_offset

        for min_corner, max_corner in chunk_gen:
            if chunk_idx >= batch_end:
                break

            chunk_task = process_chunk.si(
                min_corner=(
                    min_corner.tolist()
                    if isinstance(min_corner, np.ndarray)
                    else min_corner
                ),
                max_corner=(
                    max_corner.tolist()
                    if isinstance(max_corner, np.ndarray)
                    else max_corner
                ),
                mat_metadata=mat_metadata,
                get_root_ids=get_root_ids,
                upload_to_database=upload_to_database,
                chunk_info={
                    "chunk_idx": chunk_idx,
                    "total_chunks": total_chunks,
                },
                database=database,
                supervoxel_batch_size=supervoxel_batch_size,
                table_name=table_name,
            )
            chunk_tasks.append(chunk_task)
            chunk_idx += 1

        celery_logger.info(
            f"Processing chunks {chunk_offset}-{batch_end-1} of {total_chunks} for {table_name}"
        )

        checkpoint_manager.update_workflow(
            table_name=table_name,
            submitted_chunks=batch_end - chunk_offset,
            last_processed_chunk={
                "min_corner": min_corner,
                "max_corner": max_corner,
                "index": chunk_idx - 1,
            },
        )

        chunking_dict = chunking.to_dict()

        if batch_end == total_chunks:
            workflow = chord(
                chunk_tasks,
                rebuild_indices_for_spatial_lookup.si(mat_metadata, database),
            )
        else:
            next_batch_task = process_table_in_chunks.si(
                datastack_info=datastack_info,
                mat_metadata=mat_metadata,
                database=database,
                chunk_scale_factor=chunk_scale_factor,
                supervoxel_batch_size=supervoxel_batch_size,
                get_root_ids=get_root_ids,
                upload_to_database=upload_to_database,
                table_name=table_name,
                mat_info_idx=mat_info_idx,
                chunk_offset=batch_end,
                batch_size=batch_size,
                chunking_info=chunking_dict,
            )

            workflow = chord(chunk_tasks, next_batch_task)

        result = workflow.apply_async()

        celery_logger.info(
            f"Started workflow for batch {chunk_offset}-{batch_end-1} with task ID: {result.id}"
        )

        return f"Processing chunk batch {chunk_offset}-{batch_end-1} of {total_chunks} for {table_name}"

    except Exception as e:
        celery_logger.error(f"Error in process_table_in_chunks: {str(e)}")
        checkpoint_manager = RedisCheckpointManager(database)
        checkpoint_manager.update_workflow(
            table_name=table_name, last_error=str(e), status="error"
        )

        error_report = {
            "error": str(e),
            "chunk_offset": chunk_offset,
            "table_name": table_name,
        }
        celery_logger.error(f"Error report: {json.dumps(error_report)}")
        spatial_workflow_failed.delay(
            exc=e,
            mat_info=mat_metadata,
            workflow_name=f"spatial_lookup_chunk_setup_{table_name}",
        )
        raise self.retry(exc=e, countdown=int(2**self.request.retries))


@celery.task(
    name="process:process_chunk",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=10,
    retry_backoff=True,
)
def process_chunk(
    self,
    min_corner,
    max_corner,
    mat_metadata,
    get_root_ids,
    upload_to_database,
    chunk_info,
    database,
    supervoxel_batch_size=50,
    table_name=None,
):
    """
    Process a single spatial chunk - lookup supervoxel IDs and root IDs.
    """
    start_time = time.time()
    checkpoint_manager = None

    if table_name:
        checkpoint_manager = RedisCheckpointManager(database)

    try:
        pts_df = get_pts_from_bbox(
            database, np.array(min_corner), np.array(max_corner), mat_metadata
        )

        if pts_df is None or pts_df.empty:
            if checkpoint_manager:
                checkpoint_manager.increment_completed(
                    table_name=table_name,
                    rows_processed=0,
                    last_processed_chunk={
                        "min_corner": min_corner,
                        "max_corner": max_corner,
                        "index": chunk_info.get("chunk_idx", 0),
                    },
                )
            return None

        points_count = len(pts_df["id"])
        celery_logger.info(f"Found {points_count} points in bounding box")

        data = get_scatter_points(
            pts_df, mat_metadata, batch_size=supervoxel_batch_size
        )
        if data is None:
            if checkpoint_manager:
                checkpoint_manager.increment_completed(
                    table_name=table_name,
                    rows_processed=0,
                    last_processed_chunk={
                        "min_corner": min_corner,
                        "max_corner": max_corner,
                        "index": chunk_info.get("chunk_idx", 0),
                    },
                )
            return None

        svids_count = len(data["id"])

        affected_rows = 0
        if get_root_ids and svids_count > 0:
            root_id_data = get_new_root_ids(data, mat_metadata)

            if upload_to_database and root_id_data and len(root_id_data) > 0:
                affected_rows = insert_segmentation_data(root_id_data, mat_metadata)
                celery_logger.info(
                    f"Actually inserted/updated {affected_rows} rows in database"
                )

        total_time = time.time() - start_time
        celery_logger.info(
            f"Completed chunk {chunk_info['chunk_idx']} in {total_time:.2f}s: "
            f"{points_count} points, {svids_count} supervoxels, {affected_rows} rows affected"
        )

        if checkpoint_manager:
            checkpoint_manager.increment_completed(
                table_name=table_name,
                rows_processed=affected_rows,
                last_processed_chunk={
                    "min_corner": min_corner,
                    "max_corner": max_corner,
                    "index": chunk_info.get("chunk_idx", 0),
                },
            )

        return {
            "status": "completed",
            "points_processed": points_count,
            "affected_rows": affected_rows,
        }
    except (OperationalError, DisconnectionError) as db_error:
        celery_logger.warning(
            f"Database connection error in chunk {chunk_info['chunk_idx']}: {str(db_error)}"
            f"This may be due to a concurrent database operation. Retrying..."
        )
        db_manager.cleanup()

        if checkpoint_manager:
            checkpoint_manager.record_chunk_failure(
                table_name=table_name,
                chunk_index=chunk_info.get("chunk_idx", 0),
                error=str(db_error),
            )

        raise self.retry(exc=db_error, countdown=int(2**self.request.retries))
    except Exception as e:
        error_msg = f"Error processing chunk {chunk_info['chunk_idx']}: {str(e)}"
        celery_logger.error(error_msg)

        if checkpoint_manager:
            checkpoint_manager.record_chunk_failure(
                table_name=table_name,
                chunk_index=chunk_info.get("chunk_idx", 0),
                error=str(e),
            )

        raise self.retry(exc=e, countdown=int(2**self.request.retries))


@celery.task(
    name="workflow:rebuild_indices_for_spatial_lookup",
    bind=True,
    acks_late=True,
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
    point_tuple = tuple(point)
    return points_map.get(point_tuple, 0)


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


def get_scatter_points(pts_df, mat_info, batch_size=500):
    """Process supervoxel ID lookups in smaller batches to improve performance."""
    segmentation_source = mat_info["segmentation_source"]
    coord_resolution = mat_info["coord_resolution"]
    cv = cloudvolume_cache.get_cv(segmentation_source)
    scale_factor = cv.resolution / coord_resolution

    all_points = []
    all_types = []
    all_ids = []
    sv_id_data = {}  # To accumulate supervoxel IDs

    df = pts_df.copy()
    df["pt_position_scaled"] = df["pt_position"].apply(
        lambda x: normalize_positions(x, scale_factor)
    )
    df["chunk_key"] = df.pt_position_scaled.apply(
        lambda x: str(point_to_chunk_position(cv.meta, x, mip=0))
    )

    df = df.sort_values(by="chunk_key")

    total_batches = (len(df) + batch_size - 1) // batch_size
    celery_logger.info(
        f"Processing {len(df)} points in {total_batches} batches of {batch_size}"
    )

    for batch_idx, batch_start in enumerate(range(0, len(df), batch_size)):
        batch_end = min(batch_start + batch_size, len(df))
        batch_df = df.iloc[batch_start:batch_end]

        celery_logger.info(
            f"Processing batch {batch_idx+1}/{total_batches} with {len(batch_df)} points"
        )

        # Get point data
        batch_points = batch_df["pt_position"].tolist()
        batch_types = batch_df["type"].tolist()
        batch_ids = batch_df["id"].tolist()

        # Call scattered_points on this batch
        start_time = time.time()
        batch_sv_data = cv.scattered_points(
            batch_points, coord_resolution=coord_resolution
        )
        elapsed = time.time() - start_time
        celery_logger.info(
            f"Batch {batch_idx+1} scattered_points call took {elapsed:.2f}s"
        )

        # Accumulate results
        all_points.extend(batch_points)
        all_types.extend(batch_types)
        all_ids.extend(batch_ids)
        sv_id_data.update(batch_sv_data)

    result_df = pd.DataFrame(
        {"id": all_ids, "type": all_types, "pt_position": all_points}
    )

    result_df["pt_position_scaled"] = result_df["pt_position"].apply(
        lambda x: normalize_positions(x, scale_factor)
    )
    result_df["svids"] = result_df["pt_position_scaled"].apply(
        lambda x: match_point_and_get_value(x, sv_id_data)
    )

    result_df.drop(columns=["pt_position_scaled"], inplace=True)
    if result_df["type"].str.contains("pt").all():
        result_df["type"] = result_df["type"].apply(lambda x: f"{x}_supervoxel_id")
    else:
        result_df["type"] = result_df["type"].apply(lambda x: f"{x}_pt_supervoxel_id")

    return _safe_pivot_svid_df_to_dict(result_df)


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
    db = dynamic_annotation_cache.get_db(mat_info["aligned_volume"])
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
    """Inserts the segmentation data into the database using SQLAlchemy ORM with bulk operations.

    Args:
        data (pd.DataFrame): Dataframe containing the segmentation data
        mat_metadata (dict): Materialization info for a given table

    Returns:
        int: Number of rows actually modified in the database
    """
    start_time = time.time()
    database = mat_metadata["database"]
    SegmentationModel = create_segmentation_model(mat_metadata)

    # Process dataframe as before
    seg_columns = SegmentationModel.__table__.columns.keys()
    segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
    data_df = pd.DataFrame(data, dtype=object)

    # Convert supervoxel IDs
    supervoxel_id_cols = [
        col for col in data_df.columns if col.endswith("_supervoxel_id")
    ]
    for col in supervoxel_id_cols:
        data_df[col] = data_df[col].apply(convert_array_to_int)

    # find the common columns between the two dataframes
    common_cols = segmentation_dataframe.columns.intersection(data_df.columns)
    df = pd.merge(
        segmentation_dataframe[common_cols], data_df[common_cols], how="right"
    )
    df = df.infer_objects().fillna(0)
    df = df.reindex(columns=segmentation_dataframe.columns, fill_value=0)

    # Convert to dictionary records
    records = df.to_dict(orient="records")

    with db_manager.session_scope(database) as session:
        # Track actual modifications
        rows_actually_modified = 0

        # Get all existing IDs in one query
        existing_ids = set(
            id_tuple[0]
            for id_tuple in session.query(SegmentationModel.id)
            .filter(SegmentationModel.id.in_([r["id"] for r in records]))
            .all()
        )

        # Split records into new and existing
        new_records = []
        records_to_update = []

        for record in records:
            if record["id"] in existing_ids:
                records_to_update.append(record)
            else:
                new_records.append(record)

        # Bulk insert new records
        if new_records:
            new_objects = [SegmentationModel(**record) for record in new_records]
            session.bulk_save_objects(new_objects)
            rows_actually_modified += len(new_records)
            celery_logger.info(f"Inserted {len(new_records)} new records")

        if records_to_update:
            id_to_record_map = {record["id"]: record for record in records_to_update}
            current_records = (
                session.query(SegmentationModel)
                .filter(SegmentationModel.id.in_(list(id_to_record_map.keys())))
                .all()
            )

            update_values = []
            for db_record in current_records:
                new_record = id_to_record_map[db_record.id]
                update_dict = {"id": db_record.id}

                has_non_zero_updates = False
                for column_name in seg_columns:
                    if column_name == "id":
                        continue
                    new_value = new_record.get(column_name, 0)
                    if new_value != 0:
                        update_dict[column_name] = new_value
                        has_non_zero_updates = True

                if has_non_zero_updates:
                    update_values.append(update_dict)

            if update_values:
                session.bulk_update_mappings(SegmentationModel, update_values)
                rows_actually_modified += len(update_values)
                celery_logger.info(f"Updated {len(update_values)} existing records")

    celery_logger.info(
        f"Total modifications: {rows_actually_modified} rows in {time.time() - start_time:.2f} seconds"
    )
    return rows_actually_modified


def _safe_pivot_svid_df_to_dict(df: pd.DataFrame) -> dict:
    """Custom pivot function to preserve uint64 dtype values."""
    # Check if required columns exist in the DataFrame
    required_columns = ["id", "type", "svids"]
    if any(col not in df.columns for col in required_columns):
        raise ValueError(f"DataFrame must contain columns: {required_columns}")

    # Get the unique column names from the DataFrame
    columns = ["id"] + df["type"].unique().tolist()

    # Initialize an output dict with lists for each column
    output_dict = {col: [] for col in columns}

    # Group the DataFrame by "id" and iterate over each group
    for row_id, group in df.groupby("id"):
        output_dict["id"].append(row_id)

        # Initialize other columns with 0 for the current row_id
        for col in columns[1:]:
            output_dict[col].append(0)

        # Update the values for each type
        for _, row in group.iterrows():
            col_type = row["type"]
            if col_type in output_dict:
                idx = len(output_dict["id"]) - 1
                output_dict[col_type][idx] = row["svids"]

    return output_dict


@celery.task(name="workflow:workflow_failed", bind=True)
def spatial_workflow_failed(self, exc, task_id=None, mat_info=None, workflow_name=None):
    """
    Handles workflow failures with detailed error reporting.


    Args:
        exc: Exception information (automatically provided by Celery)
        task_id: ID of the failed task (optional)
        mat_info: Metadata about the task that failed (optional)
        workflow_name: Name of the workflow that failed (optional)

    Returns:
        dict: Error report details
    """
    error_type = (
        exc.__class__.__name__ if hasattr(exc, "__class__") else type(exc).__name__
    )
    error_message = str(exc)
    traceback_str = getattr(exc, "traceback", None)
    current_time = datetime.datetime.utcnow().isoformat()

    error_report = {
        "status": "FAILED",
        "error_type": error_type,
        "error_message": error_message,
        "task_id": task_id or self.request.id,
        "timestamp": current_time,
        "workflow_name": workflow_name or "Unknown Workflow",
    }

    if mat_info:
        try:
            workflow_context = {
                "table_name": mat_info.get("annotation_table_name"),
                "segmentation_table": mat_info.get("segmentation_table_name"),
                "database": mat_info.get("database"),
                "aligned_volume": mat_info.get("aligned_volume"),
                "pcg_table_name": mat_info.get("pcg_table_name"),
            }
            error_report["workflow_context"] = workflow_context
        except Exception as context_exc:
            celery_logger.error(
                f"Error extracting workflow context: {str(context_exc)}"
            )
            error_report["workflow_context_error"] = str(context_exc)

    if traceback_str:
        error_report["traceback"] = traceback_str

    celery_logger.error(
        f"Workflow {workflow_name or 'task'} failed: {error_type}: {error_message}",
        extra={
            "error_report": error_report,
            "task_id": task_id or self.request.id,
        },
    )

    celery_logger.error(
        f"DETAILED_ERROR_LOG: {json.dumps(error_report, default=str)}",
    )

    if error_type in ["MemoryError", "TimeoutError", "BrokerConnectionError"]:
        celery_logger.critical(
            f"CRITICAL WORKFLOW FAILURE: {workflow_name or 'Unknown'} - {error_type}: {error_message}",
            extra={"error_report": error_report},
        )

    return error_report
