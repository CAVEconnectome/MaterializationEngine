import datetime
import os
from typing import Generator, List
import time

from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import SegmentationMetadata
from sqlalchemy import and_, func, text
from sqlalchemy.exc import ProgrammingError, NoResultFound
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
    get_config_param,
)

celery_logger = get_task_logger(__name__)


def generate_chunked_model_ids(
    mat_metadata: dict, use_segmentation_model=False
) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialization.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[List]
        list of list containing start and end indices
    """
    celery_logger.info("Chunking supervoxel ids")
    if use_segmentation_model:
        AnnotationModel = create_segmentation_model(mat_metadata)
    else:
        AnnotationModel = create_annotation_model(mat_metadata)
    chunk_size = mat_metadata.get("chunk_size")

    if not chunk_size:
        ROW_CHUNK_SIZE = get_config_param("MATERIALIZATION_ROW_CHUNK_SIZE")
        chunk_size = ROW_CHUNK_SIZE

    chunked_ids = chunk_ids(mat_metadata, AnnotationModel.id, chunk_size)

    return chunked_ids


def create_chunks(data_list: List, chunk_size: int) -> Generator:
    """Create chunks from list with fixed size

    Args:
        data_list (List): list to chunk
        chunk_size (int): size of chunk

    Yields:
        List: generator of chunks
    """
    if len(data_list) <= chunk_size:
        chunk_size = len(data_list)
    for i in range(0, len(data_list), chunk_size):
        yield data_list[i : i + chunk_size]


@celery.task(name="workflow:fin", acks_late=True, bind=True)
def fin(self, *args, **kwargs):
    return True


@celery.task(name="workflow:workflow_complete", acks_late=True, bind=True)
def workflow_complete(self, workflow_name):
    return f"{workflow_name} completed successfully"


def get_materialization_info(
    datastack_info: dict,
    analysis_version: int = None,
    materialization_time_stamp: datetime.datetime.utcnow = None,
    skip_table: bool = False,
    row_size: int = 1_000_000,
    table_name: str = None,
) -> List[dict]:

    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization.

    Args:
        datastack_info (dict): Datastack info
        analysis_version (int, optional): Analysis version to use for frozen materialization. Defaults to None.
        skip_table (bool, optional): Triggers row count for skipping tables larger than row_size arg. Defaults to False.
        row_size (int, optional): Row size number to check. Defaults to 1_000_000.

    Returns:
        List[dict]: [description]
    """
    celery_logger.info("Collecting materialization metadata")
    aligned_volume_name = datastack_info["aligned_volume"]["name"]
    pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]
    segmentation_source = datastack_info.get("segmentation_source")

    if not materialization_time_stamp:
        materialization_time_stamp = datetime.datetime.utcnow()

    db = dynamic_annotation_cache.get_db(aligned_volume_name)

    annotation_tables = db.database.get_valid_table_names()
    if table_name is not None:
        annotation_tables = [next(a for a in annotation_tables if a == table_name)]
    metadata = []
    celery_logger.debug(f"Annotation tables: {annotation_tables}")
    for annotation_table in annotation_tables:
        row_count = db.database.get_table_row_count(annotation_table, filter_valid=True)
        max_id = db.database.get_max_id_value(annotation_table)
        min_id = db.database.get_min_id_value(annotation_table)
        if row_count == 0:
            continue

        if row_count >= row_size and skip_table:
            continue

        md = db.database.get_table_metadata(annotation_table)
        vx = md.get("voxel_resolution_x", None)
        vy = md.get("voxel_resolution_y", None)
        vz = md.get("voxel_resolution_z", None)
        vx = vx or 1.0
        vy = vy or 1.0
        vz = vz or 1.0
        voxel_resolution = [vx, vy, vz]

        reference_table = md.get("reference_table")

        if max_id and max_id > 0:
            table_metadata = {
                "annotation_table_name": annotation_table,
                "datastack": datastack_info["datastack"],
                "aligned_volume": str(aligned_volume_name),
                "schema": db.database.get_table_schema(annotation_table),
                "max_id": int(max_id),
                "min_id": int(min_id),
                "row_count": row_count,
                "add_indices": True,
                "coord_resolution": voxel_resolution,
                "reference_table": reference_table,
                "materialization_time_stamp": str(materialization_time_stamp),
                "table_count": len(annotation_tables),
            }

            if not reference_table:
                segmentation_table_name = build_segmentation_table_name(
                    annotation_table, pcg_table_name
                )
                try:
                    segmentation_metadata = db.segmentation.get_segmentation_table_metadata(
                        annotation_table, pcg_table_name
                    )
                    create_segmentation_table = False
                except NoResultFound as e:
                    celery_logger.warning(f"SEGMENTATION TABLE DOES NOT EXIST: {e}")
                    segmentation_metadata = {"last_updated": None}
                    create_segmentation_table = True
                last_updated_time_stamp = segmentation_metadata.get("last_updated")

                if not last_updated_time_stamp:
                    last_updated_time_stamp = None
                else:
                    last_updated_time_stamp = str(last_updated_time_stamp)

                table_metadata.update(
                    {
                        "create_segmentation_table": create_segmentation_table,
                        "segmentation_table_name": segmentation_table_name,
                        "temp_mat_table_name": f"temp__{annotation_table}",
                        "pcg_table_name": pcg_table_name,
                        "segmentation_source": segmentation_source,
                        "last_updated_time_stamp": last_updated_time_stamp,
                        "chunk_size": get_config_param(
                            "MATERIALIZATION_ROW_CHUNK_SIZE"
                        ),
                        "queue_length_limit": get_config_param("QUEUE_LENGTH_LIMIT"),
                        "throttle_queues": get_config_param("THROTTLE_QUEUES"),
                        "find_all_expired_roots": datastack_info.get(
                            "find_all_expired_roots", False
                        ),
                    }
                )
            if analysis_version:
                table_metadata.update(
                    {
                        "analysis_version": analysis_version,
                        "analysis_database": f"{datastack_info['datastack']}__mat{analysis_version}",
                    }
                )

            metadata.append(table_metadata.copy())
            celery_logger.debug(metadata)
    db.database.cached_session.close()
    celery_logger.info(f"Metadata collected for {len(metadata)} tables")
    return metadata


@celery.task(name="workflow:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs


def query_id_range(column, start_id: int, end_id: int):
    if end_id:
        return and_(column >= start_id, column < end_id)
    else:
        return column >= start_id


def chunk_ids(mat_metadata, model, chunk_size: int):
    aligned_volume = mat_metadata.get("aligned_volume")
    session = sqlalchemy_cache.get(aligned_volume)

    q = session.query(
        model, func.row_number().over(order_by=model).label("row_count")
    ).from_self(model)

    if chunk_size > 1:
        q = q.filter(text("row_count %% %d=1" % chunk_size))

    chunks = [id for id, in q]

    while chunks:
        chunk_start = chunks.pop(0)
        chunk_end = chunks[0] if chunks else None
        yield [chunk_start, chunk_end]


@celery.task(
    name="workflow:update_metadata",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def update_metadata(self, mat_metadata: dict):
    """Update 'last_updated' column in the segmentation
    metadata table for a given segmentation table.


    Args:
        mat_metadata (dict): materialization metadata

    Returns:
        str: description of table that was updated
    """
    aligned_volume = mat_metadata["aligned_volume"]
    segmentation_table_name = mat_metadata["segmentation_table_name"]

    session = sqlalchemy_cache.get(aligned_volume)

    materialization_time_stamp = mat_metadata["materialization_time_stamp"]
    try:
        last_updated_time_stamp = datetime.datetime.strptime(
            materialization_time_stamp, "%Y-%m-%d %H:%M:%S.%f"
        )
    except ValueError:
        last_updated_time_stamp = datetime.datetime.strptime(
            materialization_time_stamp, "%Y-%m-%dT%H:%M:%S.%f"
        )

    try:
        seg_metadata = (
            session.query(SegmentationMetadata)
            .filter(SegmentationMetadata.table_name == segmentation_table_name)
            .one()
        )
        seg_metadata.last_updated = last_updated_time_stamp
        session.commit()
    except Exception as e:
        celery_logger.error(f"SQL ERROR: {e}")
        session.rollback()
    finally:
        session.close()
    return {
        f"Table: {segmentation_table_name}": f"Time stamp {materialization_time_stamp}"
    }


@celery.task(
    name="workflow:add_index",
    bind=True,
    acks_late=True,
    task_reject_on_worker_lost=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def add_index(self, database: dict, command: str):
    """Add an index or a contrainst to a table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        command (str): sql command to create an index or constraint

    Raises:
        self.retry: retries task when an error creating an index occurs

    Returns:
        str: String of SQL command
    """
    engine = sqlalchemy_cache.get_engine(database)

    # increase maintenance memory to improve index creation speeds,
    # reset to default after index is created
    ADD_INDEX_SQL = f"""
        SET maintenance_work_mem to '1GB';
        {command}
        SET maintenance_work_mem to '64MB';
    """

    try:
        with engine.begin() as conn:
            celery_logger.info(f"Adding index: {command}")
            result = conn.execute(ADD_INDEX_SQL)
    except ProgrammingError as index_error:
        celery_logger.error(index_error)
        return "Index already exists"
    except Exception as e:
        celery_logger.error(f"Index creation failed: {e}")
        raise self.retry(exc=e, countdown=3)

    return f"Index {command} added to table"


def monitor_task_states(task_ids: List, polling_rate: int = 0.2):
    while True:
        results = [AsyncResult(task_id, app=celery) for task_id in task_ids]
        celery_logger.debug(f"Celery results {results}")
        result_status = []
        for result in results:
            if result.state == "FAILURE":
                raise Exception(result.traceback)
            result_status.append(result.state)

        celery_logger.debug(f"Celery task status: {result_status}")

        if all(x == "SUCCESS" for x in result_status):
            return True

        time.sleep(polling_rate)


def monitor_workflow_state(workflow: AsyncResult, polling_rate: int = 0.2):

    while True:
        celery_logger.debug("WAITING FOR TASKS TO COMPLETE...")
        if workflow.ready():
            celery_logger.debug(f"WORKFLOW IDS: {workflow.id}, READY")
        if workflow.successful():
            celery_logger.debug("CHAIN COMPLETE")
            return True
        time.sleep(polling_rate)


def check_if_task_is_running(task_name: str, worker_name_prefix: str) -> bool:
    """Check if a task is running under a worker with a specified prefix name.
    If the task is found to be in an active state then return True.

    Parameters
    ----------
    task_name : str
        name of task to check if it is running
    worker_name_prefix : str
        prefix of celery worker, used to check specific queue.

    Returns
    -------
    bool
        True if task_name is running else False
    """
    inspector = celery.control.inspect()
    active_tasks_dict = inspector.active()

    workflow_active_tasks = next(
        v for k, v in active_tasks_dict.items() if worker_name_prefix in k
    )

    for active_task in workflow_active_tasks:
        if task_name in active_task.values():
            celery_logger.info(f"Task {task_name} is running...")
            return True
    return False
