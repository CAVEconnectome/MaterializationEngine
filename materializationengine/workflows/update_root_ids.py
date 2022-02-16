import datetime
from typing import List

import numpy as np
import pandas as pd
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from materializationengine.celery_init import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.database import sqlalchemy_cache
from materializationengine.shared_tasks import (
    fin,
    create_chunks,
    update_metadata,
    get_materialization_info,
)
from materializationengine.utils import create_segmentation_model
from sqlalchemy.sql import or_, text


celery_logger = get_task_logger(__name__)


@celery.task(name="process:update_root_ids_task")
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
    workflow = []
    for mat_metadata in mat_info:
        if not mat_metadata["reference_table"]:
            chunked_roots = get_expired_root_ids_from_pcg(mat_metadata)
            if chunked_roots:
                process_root_ids = update_root_ids_workflow(
                    mat_metadata, chunked_roots
                )  # final task which will process a return status/timing etc...
                workflow.append(process_root_ids)
            else:
                continue
    if len(workflow) <= 1:
        return "No root ids to update"
    workflow = chord(workflow, fin.s())
    return workflow.apply_async(kwargs={"Datastack": datastack_info["datastack"]})


@celery.task(
    name="process:update_root_ids_workflow",
    bind=True,
    acks_late=True,
)
def update_root_ids_workflow(self, mat_metadata: dict, chunked_roots: List[int]):
    """Celery workflow that updates expired root ids in a
    segmentation table.

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
        chain: chain of celery tasks
    """
    update_expired_roots_workflow = chain(
        chord(
            [
                group(update_root_ids(root_ids, mat_metadata))
                for root_ids in chunked_roots
            ],
            fin.si(),
        ),
        update_metadata.si(mat_metadata),
    )
    return update_expired_roots_workflow


def update_root_ids(root_ids: List[int], mat_metadata: dict) -> True:
    """Chunks supervoxel data and distributes
    supervoxels ids to parallel worker tasks that
    lookup new root_ids and update the database table.

    Args:
        root_ids (List[int]): List of expired root_ids
        mat_metadata (dict): metadata for tasks
    """
    segmentation_table_name = mat_metadata.get("segmentation_table_name")
    celery_logger.info(f"Starting root_id updating on {segmentation_table_name} table")
    celery_logger.info(f"LENGTH OF ROOTS {root_ids}")

    return chain(
        get_supervoxel_id_queries.si(root_ids, mat_metadata),
        get_expired_roots_from_db.s(mat_metadata),
    )


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
    is_empty = np.all((old_roots == []))

    if is_empty or old_roots is None:
        return None
    else:
        return generate_chunked_root_ids(old_roots, expired_chunk_size)


def generate_chunked_root_ids(old_roots, expired_chunk_size):
    if len(old_roots) < expired_chunk_size:
        chunks = len(old_roots)
    else:
        chunks = len(old_roots) // expired_chunk_size

    chunked_root_ids = np.array_split(old_roots, chunks)

    for x in chunked_root_ids:
        yield x.tolist()


def lookup_expired_root_ids(
    pcg_table_name, last_updated_ts, materialization_time_stamp
):
    cg = chunkedgraph_cache.init_pcg(pcg_table_name)
    try:
        old_roots, __ = cg.get_proofread_root_ids(
            last_updated_ts, materialization_time_stamp
        )
        return old_roots
    except ValueError as e:
        celery_logger.info(
            f"No expired root ids found between {last_updated_ts} and {materialization_time_stamp}: {e}"
        )
        return None


@celery.task(
    name="process:get_supervoxel_id_queries",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def get_supervoxel_id_queries(self, root_id_chunk: list, mat_metadata: dict):
    """Get supervoxel ids associated with expired root ids

    Args:
        root_id_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        dict: supervoxels of a group of expired root ids
        None: no supervoxel ids exist for the expired root id
    """
    aligned_volume = mat_metadata.get("aligned_volume")
    SegmentationModel = create_segmentation_model(mat_metadata)

    session = sqlalchemy_cache.get(aligned_volume)
    columns = [column.name for column in SegmentationModel.__table__.columns]
    root_id_columns = [column for column in columns if "root_id" in column]

    supervoxel_queries = []
    for root_id_column in root_id_columns:
        prefix = root_id_column.rsplit("_", 2)[0]
        supervoxel_name = f"{prefix}_supervoxel_id"
        celery_logger.info(f"ROOT ID CHUNK {root_id_chunk}")
        sv_ids_query = session.query(
            SegmentationModel.id,
            getattr(SegmentationModel, root_id_column),
            getattr(SegmentationModel, supervoxel_name),
        ).filter(or_(getattr(SegmentationModel, root_id_column)).in_(root_id_chunk))

        sv_ids_query = sv_ids_query.statement.compile(
            compile_kwargs={"literal_binds": True}
        )
        supervoxel_queries.append({f"{root_id_column}": str(sv_ids_query)})
        celery_logger.debug(f"SQL STATEMENT {sv_ids_query}")

    return supervoxel_queries or None


@celery.task(
    name="process:get_expired_roots_from_db",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def get_expired_roots_from_db(self, supervoxel_queries: list, mat_metadata: dict):
    """Get new roots from supervoxels ids of expired roots

    Args:
        supervoxel_chunk (list): [description]
        mat_metadata (dict): [description]

    Returns:
        dict: dicts of new root_ids
    """
    aligned_volume = mat_metadata.get("aligned_volume")
    query_chunk_size = mat_metadata.get("chunk_size", 100)

    tasks = []

    engine = sqlalchemy_cache.get_engine(aligned_volume)

    # https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results
    for query_dict in supervoxel_queries:
        column_name = list(query_dict.keys())[0]
        query_stmt = text(list(query_dict.values())[0])
        with engine.connect() as conn:
            proxy = conn.execution_options(stream_results=True).execute(query_stmt)
            while "batch not empty":
                batch = proxy.fetchmany(
                    query_chunk_size
                )  # fetch n_rows from chunk_size
                if not batch:
                    break
                celery_logger.info(batch)
                supervoxel_data = pd.DataFrame(
                    batch, columns=batch[0].keys(), dtype=object
                ).to_dict(orient="records")

                tasks.append(get_new_root_ids.si(supervoxel_data, mat_metadata))
            proxy.close()
    return group(tasks).apply_async


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
    session = sqlalchemy_cache.get(aligned_volume)
    data = root_ids_df.to_dict(orient="records")
    try:
        session.bulk_update_mappings(SegmentationModel, data)
        session.commit()
    except Exception as e:
        session.rollback()
        celery_logger.error(f"ERROR: {e}")
        raise self.retry(exc=e, countdown=3)
    finally:
        session.close()
    return f"Number of rows updated: {len(data)}"


def lookup_new_root_ids(pcg_table_name, supervoxel_data, formatted_mat_ts):
    cg = chunkedgraph_cache.init_pcg(pcg_table_name)
    return np.squeeze(cg.get_roots(supervoxel_data, time_stamp=formatted_mat_ts))
