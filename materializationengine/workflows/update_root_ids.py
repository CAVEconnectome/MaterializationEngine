import datetime
from functools import lru_cache

import numpy as np
import pandas as pd
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_init import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.database import db_manager
from materializationengine.shared_tasks import (
    fin,
    get_materialization_info,
    monitor_task_states,
    monitor_workflow_state,
    update_metadata,
    workflow_complete,
    generate_chunked_model_ids,
)
from materializationengine.throttle import throttle_celery
from materializationengine.utils import create_segmentation_model
from requests import HTTPError
from sqlalchemy.sql import or_, text

celery_logger = get_task_logger(__name__)


@celery.task(name="workflow:update_root_ids_task")
def expired_root_id_workflow(datastack_info: dict, **kwargs):
    """Workflow to process expired root ids and lookup and
    update table with current root ids.

    Args:
        datastack_info (dict): Workflow metadata
    """

    materialization_time_stamp = datetime.datetime.utcnow()

    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
    )
    for mat_metadata in mat_info:
        if mat_metadata.get("segmentation_table_name"):
            update_root_ids_workflow(mat_metadata).apply_async(
                kwargs={"Datastack": datastack_info["datastack"]}
            )
    return True


def update_root_ids_workflow(mat_metadata: dict):
    """Celery workflow that updates expired root ids in a
    segmentation table. Returns celery chain primitive without executing it.

    Workflow:
        - Lookup supervoxel id associated with expired root id
        - Lookup new root id for the supervoxel
        - Update database row with new root id

    Once all root ids in a given table are updated the associated entry in the
    metadata data will also be updated.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        chunked_roots (List[int]): chunks of expired root ids to lookup

    Returns:
        chain: chain of celery tasks (not executed - returns signature only)
    """

    celery_logger.info("Preparing expired root id workflow...")
    
    # Generate chunks synchronously (lightweight operation)
    if mat_metadata.get("lookup_all_root_ids"):
        chunked_ids = generate_chunked_model_ids(mat_metadata)
    else:
        chunked_ids = get_expired_root_ids_from_pcg(mat_metadata)

    if not chunked_ids:
        celery_logger.info("No expired root IDs to process")
        return fin.si()

    # Build the workflow chain - don't execute it here
    # The caller will execute it as part of a larger chain
    update_root_workflow = chain(
        chord(
            [
                group(update_root_ids.si(root_ids, mat_metadata))
                for root_ids in chunked_ids
            ],
            fin.si(),
        ),
        update_metadata.si(mat_metadata),
    )
    
    # Return the chain signature - don't execute it here
    return update_root_workflow


def get_expired_root_ids_from_pcg(mat_metadata: dict, expired_chunk_size: int = 100):
    """Find expired root ids from last updated timestamp. Returns chunked lists as
    generator.

    Args:
        mat_metadata (dict): [description]
        expired_chunk_size (int, optional): [description]. Defaults to 100.

    Returns:
        None: If no expired root ids are found between last updated and current time.

    Yields:
        list: list of expired root ids
    """

    last_updated_ts = mat_metadata.get("last_updated_time_stamp")
    pcg_table_name = mat_metadata.get("pcg_table_name")
    find_all_expired_roots = mat_metadata.get("find_all_expired_roots", False)
    materialization_time_stamp_str = mat_metadata.get("materialization_time_stamp")
    materialization_time_stamp = datetime.datetime.strptime(
        materialization_time_stamp_str, "%Y-%m-%d %H:%M:%S.%f"
    )

    if find_all_expired_roots:
        last_updated_ts = None
    elif last_updated_ts:
        last_updated_ts = datetime.datetime.strptime(
            last_updated_ts, "%Y-%m-%d %H:%M:%S.%f"
        )
    else:
        last_updated_ts = datetime.datetime.utcnow() - datetime.timedelta(days=5)

    celery_logger.info(f"Looking up expired root ids since: {last_updated_ts}")

    old_roots = lookup_expired_root_ids(
        pcg_table_name, last_updated_ts, materialization_time_stamp
    )
    if old_roots is None:
        celery_logger.info(f"No root ids have expired since {str(last_updated_ts)}")
        return None
    else:
        celery_logger.info(f"Amount of expired root ids: {len(old_roots)}")
        yield from generate_chunked_root_ids(old_roots, expired_chunk_size)


def generate_chunked_root_ids(old_roots, expired_chunk_size):
    if len(old_roots) < expired_chunk_size:
        chunks = len(old_roots)
    else:
        chunks = len(old_roots) // expired_chunk_size

    chunked_root_ids = np.array_split(old_roots, chunks)

    for x in chunked_root_ids:
        yield x.tolist()


@lru_cache(maxsize=32)
def lookup_expired_root_ids(
    pcg_table_name, last_updated_ts, materialization_time_stamp
):
    cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)
    try:
        old_roots, __ = cg_client.get_delta_roots(
            last_updated_ts, materialization_time_stamp
        )
        if old_roots.size != 0:
            return old_roots
        return None
    except HTTPError as e:
        raise e


def get_supervoxel_id_queries(root_id_chunk: list, mat_metadata: dict):
    """Get supervoxel ids associated with expired root ids

    Args:
        root_id_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        dict: supervoxels of a group of expired root ids
        None: no supervoxel ids exist for the expired root id
    """
    database = mat_metadata.get("database")
    SegmentationModel = create_segmentation_model(mat_metadata)

    with db_manager.session_scope(database) as session:

        columns = [column.name for column in SegmentationModel.__table__.columns]
        root_id_columns = [column for column in columns if "root_id" in column]

        supervoxel_queries = []
        for root_id_column in root_id_columns:
            prefix = root_id_column.rsplit("_", 2)[0]
            supervoxel_name = f"{prefix}_supervoxel_id"
            root_id_att = getattr(SegmentationModel, root_id_column)
            sv_id_att = getattr(SegmentationModel, supervoxel_name)
            sv_ids_query = session.query(
                SegmentationModel.id,
                root_id_att,
                sv_id_att,
            )
            if mat_metadata.get("lookup_all_root_ids", False):
                if root_id_chunk[1] is None:
                    root_id_chunk[1] = mat_metadata.get("max_id")
                stmt = sv_ids_query.filter(
                    or_(SegmentationModel.id).between(
                        int(root_id_chunk[0]), int(root_id_chunk[1])
                    )
                )
            else:
                stmt = sv_ids_query.filter(
                    or_(or_(root_id_att).in_(root_id_chunk), root_id_att == None)
                )

            sv_ids_query = stmt.statement.compile(compile_kwargs={"literal_binds": True})
            supervoxel_queries.append({f"{root_id_column}": str(sv_ids_query)})

    return supervoxel_queries or None


@celery.task(
    name="workflow:update_root_ids",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def update_root_ids(self, root_id_chunk: list, mat_metadata: dict):
    """Get new roots from supervoxels ids of expired roots.

    Args:
        supervoxel_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        dict: dicts of new root_ids
    """
    supervoxel_queries = get_supervoxel_id_queries(root_id_chunk, mat_metadata)
    if not supervoxel_queries:
        return fin.si()

    database = mat_metadata.get("database")
    query_chunk_size = mat_metadata.get("chunk_size", 100)

    tasks = []

    engine = db_manager.get_engine(database)

    # https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results
    for query_dict in supervoxel_queries:
        query_stmt = text(list(query_dict.values())[0])
        with engine.connect() as conn:
            proxy = conn.execution_options(stream_results=True).execute(query_stmt)
            while "batch not empty":
                if mat_metadata.get("throttle_queues"):
                    throttle_celery.wait_if_queue_full(queue_name="process")
                batch = proxy.fetchmany(
                    query_chunk_size
                )  # fetch n_rows from chunk_size
                if not batch:
                    celery_logger.debug(
                        f"No rows left for {mat_metadata['annotation_table_name']}"
                    )
                    break
                supervoxel_data = pd.DataFrame(
                    batch, columns=batch[0].keys(), dtype=object
                ).to_dict(orient="records")

                task = get_new_root_ids.si(supervoxel_data, mat_metadata).apply_async()
                tasks.append(task.id)

            proxy.close()
    try:
        tasks_completed = monitor_task_states(tasks)
    except Exception as e:
        celery_logger.error(f"Monitor reports task failed: {e}")
        raise self.retry(exc=e, countdown=3)
    return fin.si()


@celery.task(
    name="process:get_new_root_ids",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def get_new_root_ids(self, supervoxel_data, mat_metadata):
    pcg_table_name = mat_metadata.get("pcg_table_name")

    materialization_time_stamp = mat_metadata["materialization_time_stamp"]
    try:
        formatted_mat_ts = datetime.datetime.strptime(
            materialization_time_stamp, "%Y-%m-%dT%H:%M:%S.%f"
        )
    except:
        formatted_mat_ts = datetime.datetime.strptime(
            materialization_time_stamp, "%Y-%m-%d %H:%M:%S.%f"
        )
    root_ids_df = pd.DataFrame(supervoxel_data, dtype=object)

    supervoxel_col_name = list(
        root_ids_df.loc[:, root_ids_df.columns.str.endswith("supervoxel_id")]
    )
    root_id_col_name = list(
        root_ids_df.loc[:, root_ids_df.columns.str.endswith("root_id")]
    )
    supervoxel_df = root_ids_df.loc[:, supervoxel_col_name[0]]
    supervoxel_data = supervoxel_df.to_list()

    root_id_array = lookup_new_root_ids(
        pcg_table_name, supervoxel_data, formatted_mat_ts
    )

    del supervoxel_data

    root_ids_df.loc[supervoxel_df.index, root_id_col_name[0]] = root_id_array
    root_ids_df.drop(columns=[supervoxel_col_name[0]])

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")
    
    with db_manager.session_scope(aligned_volume) as session:
        try:
            data = root_ids_df.to_dict(orient="records")
            session.bulk_update_mappings(SegmentationModel, data)
        except Exception as e:
            raise self.retry(exc=e, countdown=3)

    return f"Number of rows updated: {len(data)}"


def lookup_new_root_ids(pcg_table_name, supervoxel_data, formatted_mat_ts):
    cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)
    return np.squeeze(cg_client.get_roots(supervoxel_data, timestamp=formatted_mat_ts))
