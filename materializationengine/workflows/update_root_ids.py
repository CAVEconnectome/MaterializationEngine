import datetime
import itertools
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
    run_with_limit,
    update_metadata,
    workflow_complete,
    generate_chunked_model_ids,
)
from materializationengine.throttle import throttle_celery
from materializationengine.utils import create_segmentation_model, get_config_param
from materializationengine.workflows import root_id_update_log
from requests import HTTPError
from sqlalchemy.sql import and_, or_, text

celery_logger = get_task_logger(__name__)

# The chunk that stands for "every row with a supervoxel id and a NULL root id": each run
# looks those up once, in a chunk of their own, next to the expired roots.
MISSING_ROOTS = "missing"
# Rows per chunk task when every root id is looked up (each task then streams its rows in
# batches of the usual chunk_size), and how many chunk tasks may be unfinished at a time.
LOOKUP_ALL_CHUNK_ROWS = 50_000
DEFAULT_MAX_CHUNKS_IN_FLIGHT = 20


def _max_chunks_in_flight() -> int:
    return int(get_config_param("UPDATE_ROOT_IDS_MAX_IN_FLIGHT", DEFAULT_MAX_CHUNKS_IN_FLIGHT))


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


def as_of_last_update(mat_metadata: dict) -> dict:
    """The metadata with its time stamp set to the table's last update (now if it has none)."""
    last_updated = mat_metadata.get("last_updated_time_stamp")
    when = datetime.datetime.fromisoformat(str(last_updated)) if last_updated else datetime.datetime.utcnow()
    return {**mat_metadata, "materialization_time_stamp": when.strftime("%Y-%m-%d %H:%M:%S.%f")}


def update_root_ids_workflow(mat_metadata: dict):
    """Update the root ids of a segmentation table.

    A regular update looks up, as of the run's materialization time stamp, the rows of
    every root that expired since the table's last update and the rows whose root id is
    NULL (with a supervoxel id, in a chunk of their own), then sets the table's
    last_updated to that time.

    A repair leaves last_updated as it is and looks up as of it (as of now if the table
    has none), so the next regular update still covers everything that expired since:
        - lookup_all_root_ids: every row of the table, for when roots may be stale without
          knowing which;
        - only_missing_roots: only the rows whose root id is NULL.

    Each row is looked up from its supervoxel id, and only rows whose root changed are
    written. Chunk tasks are queued as earlier ones finish (at most
    UPDATE_ROOT_IDS_MAX_IN_FLIGHT at a time), so a large table never floods the broker.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice

    Returns:
        celery signature
    """

    celery_logger.info("Setup expired root id workflow...")
    repair = bool(mat_metadata.get("lookup_all_root_ids") or mat_metadata.get("only_missing_roots"))
    if repair:
        mat_metadata = as_of_last_update(mat_metadata)
    if mat_metadata.get("lookup_all_root_ids"):
        chunks = generate_chunked_model_ids({**mat_metadata, "chunk_size": LOOKUP_ALL_CHUNK_ROWS})
    elif mat_metadata.get("only_missing_roots"):
        chunks = [MISSING_ROOTS]
    else:
        chunks = itertools.chain(get_expired_root_ids_from_pcg(mat_metadata) or [], [MISSING_ROOTS])

    # Every regular run is recorded, including one that changes nothing (a manifest row
    # with no updates), so the recorded windows chain without gaps. A repair is not: it is
    # as of the last update, a time a run already recorded.
    if repair:
        mat_metadata = {**mat_metadata, "root_id_update_log": False}
    record_updates = root_id_update_log.enabled(mat_metadata)
    if record_updates:
        root_id_update_log.start(mat_metadata)
    succeeded = False
    try:
        sent = run_with_limit(
            (update_root_ids.si(chunk, mat_metadata) for chunk in chunks), _max_chunks_in_flight()
        )
        celery_logger.info(
            f"Looked up root ids of '{mat_metadata.get('segmentation_table_name')}' in {sent} chunks, "
            f"as of {mat_metadata['materialization_time_stamp']}"
        )
        if not repair:
            monitor_task_states([update_metadata.si(mat_metadata).apply_async().id])
        succeeded = True
    finally:
        if record_updates:
            root_id_update_log.finalize(mat_metadata, succeeded=succeeded)
    return workflow_complete.si("update_root_ids_workflow")


@celery.task(name="workflow:lookup_all_root_ids_workflow")
def lookup_all_root_ids_workflow(datastack_info: dict, table_name: str, **kwargs):
    """Look up every root id of one table from its supervoxel ids.

    For a table whose root ids may be stale without knowing which: every row is looked
    up as of the table's last update (now if it has none), only rows whose root changed
    are written, and last_updated is left as it is, so the next regular update covers
    whatever expired since.
    """
    mat_metadata = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=datetime.datetime.utcnow(),
        table_name=table_name,
    )[0]
    update_root_ids_workflow({**mat_metadata, "lookup_all_root_ids": True})
    return True


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
        # get_delta_roots can list an old root more than once (on wclee_aedes_brain, 119 of
        # 1295 in one window). Two chunks holding the same root run in parallel, both find its
        # rows before either commits, and both look them up and write them again.
        unique_roots = np.unique(np.asarray(old_roots))
        celery_logger.info(
            f"Amount of expired root ids: {len(unique_roots)}"
            + (f" ({len(old_roots) - len(unique_roots)} duplicates dropped)" if len(unique_roots) < len(old_roots) else "")
        )
        yield from generate_chunked_root_ids(unique_roots, expired_chunk_size)


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
            if root_id_chunk == MISSING_ROOTS:
                # rows with nothing to look up yet (no supervoxel id) are left alone
                stmt = sv_ids_query.filter(and_(root_id_att.is_(None), sv_id_att.isnot(None)))
            elif mat_metadata.get("lookup_all_root_ids", False):
                start, end = root_id_chunk
                if end is None:  # the last chunk runs to the end of the table
                    stmt = sv_ids_query.filter(SegmentationModel.id >= int(start))
                else:
                    stmt = sv_ids_query.filter(SegmentationModel.id.between(int(start), int(end)))
            else:
                stmt = sv_ids_query.filter(root_id_att.in_(root_id_chunk))

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

    old_roots = root_ids_df[root_id_col_name[0]].copy()
    root_ids_df.loc[supervoxel_df.index, root_id_col_name[0]] = root_id_array
    # only rows whose root changed are written and recorded (a full re-lookup finds most
    # unchanged)
    changed = (old_roots.isna() | (old_roots != root_ids_df[root_id_col_name[0]])).to_numpy()
    root_ids_df, old_roots = root_ids_df[changed], old_roots[changed]
    if root_ids_df.empty:
        return "Number of rows updated: 0"

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    with db_manager.session_scope(aligned_volume) as session:
        try:
            data = root_ids_df.drop(columns=[supervoxel_col_name[0]]).to_dict(orient="records")
            session.bulk_update_mappings(SegmentationModel, data)
        except Exception as e:
            raise self.retry(exc=e, countdown=3)

    if root_id_update_log.enabled(mat_metadata):
        root_id_update_log.record_chunk(
            mat_metadata, root_ids_df, old_roots, root_id_col_name[0], supervoxel_col_name[0], self.request.id
        )
    return f"Number of rows updated: {len(data)}"


def lookup_new_root_ids(pcg_table_name, supervoxel_data, formatted_mat_ts):
    cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)
    return np.squeeze(cg_client.get_roots(supervoxel_data, timestamp=formatted_mat_ts))
