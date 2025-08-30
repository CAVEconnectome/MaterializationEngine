import datetime
import time
from typing import List

import cloudvolume
import numpy as np
import pandas as pd
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from dynamicannotationdb.models import SegmentationMetadata
from materializationengine.celery_init import celery
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.database import dynamic_annotation_cache, db_manager
from materializationengine.throttle import throttle_celery
from materializationengine.shared_tasks import (
    generate_chunked_model_ids,
    fin,
    query_id_range,
    create_chunks,
    update_metadata,
    get_materialization_info,
    monitor_workflow_state,
    monitor_task_states,
    workflow_complete,
)
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
    get_geom_from_wkb,
    get_query_columns_by_suffix,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import or_
from sqlalchemy.sql import func, text

celery_logger = get_task_logger(__name__)


@celery.task(name="workflow:process_new_annotations_workflow")
def process_new_annotations_workflow(
    datastack_info: dict, table_name: str = None, **kwargs
):
    """Base live materialization

    Workflow paths:
        check if supervoxel column is empty:
            if last_updated is NULL:
                -> workflow : find missing supervoxels > cloudvolume lookup supervoxels >
                              get root ids > find missing root_ids >
                              lookup supervoxel ids from sql >
                              get root_ids > merge root_ids list > insert root_ids
            else:
                -> find missing supervoxels > cloudvolume lookup |
                    - > find new root_ids between time stamps  ---> merge root_ids list > upsert root_ids

    Parameters
    ----------
    datastack_info : dict
        datastack to run this workflow on
    table_name : str (optional)
        individual table to run this workflow on
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
        skip_table=True,
        table_name=table_name,
    )

    for mat_metadata in mat_info:
        if mat_metadata["row_count"] < 1_000_000 and mat_metadata.get(
            "segmentation_table_name"
        ):
            annotation_chunks = generate_chunked_model_ids(mat_metadata)
            process_chunks_workflow = chain(
                ingest_new_annotations_workflow(
                    mat_metadata
                ),  # return here is required for chords
                update_metadata.si(mat_metadata),
            )  # final task which will process a return status/timing etc...

            process_chunks_workflow.apply_async(
                kwargs={"Datastack": datastack_info["datastack"]}
            )


@celery.task(
    name="process:ingest_table_svids",
    bind=True,
    acks_late=True,
)
def ingest_table_svids(
    self, datastack_info: dict, table_name: str, annotation_ids: list = None
):
    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=None,
        skip_table=True,
        table_name=table_name,
        skip_row_count=True if annotation_ids else False,
    )
    mat_metadata = mat_info[0]  # only one entry for a single table
    table_created = create_missing_segmentation_table(mat_metadata)
    if table_created:
        celery_logger.info(
            f'Table created: {mat_metadata.get("segmentation_table_name")}'
        )
    if annotation_ids:
        ingest_workflow = ingest_new_annotations.si(
            None, mat_metadata, annotation_ids, lookup_root_ids=False
        )
        ingest_workflow.apply_async()
    else:
        annotation_chunks = generate_chunked_model_ids(mat_metadata)
        ingest_workflow = chain(
            chord(
                [
                    chain(
                        ingest_new_annotations.si(
                            annotation_chunk, mat_metadata, lookup_root_ids=False
                        ),
                    )
                    for annotation_chunk in annotation_chunks
                ],
                fin.si(),
            )
        ).apply_async()


@celery.task(
    name="process:ingest_new_annotations",
    acks_late=True,
    bind=True,
    autoretry_for=(Exception,),
    max_retries=6,
)
def ingest_new_annotations(
    self,
    chunk: List[int],
    mat_metadata: dict,
    ids_list: List[int] = None,
    lookup_root_ids: bool = True,
):
    """Find annotations with missing entries in the segmentation
    table. Lookup supervoxel ids at the spatial point then
    optionally find the current root id at the materialized timestamp.
    Finally insert the supervoxel and root ids into the
    segmentation table.

    Parameters
    ----------
    chunk : List[int]
        list of annotation bounds
    mat_metadata : dict
         metadata associated with the materialization
    lookup_root_ids : True
        lookup root ids after segmentation ids are resolved
    ids_list : List[int], optional
        list of annotation ids, by default None

    Returns
    -------
    dict
        Name of table and runtime of task.

    Raises
    ------
    self.retry
        re-queue the tasks if failed. Retries 6 times.
    """
    try:
        start_time = time.time()
        missing_data = get_annotations_with_missing_supervoxel_ids(
            mat_metadata, chunk, ids_list
        )
        celery_logger.debug(f"Missing data {missing_data}")
        if not missing_data:
            celery_logger.debug("NO MISSING SVIDS")
            return fin.si()
        supervoxel_data = get_cloudvolume_supervoxel_ids(missing_data, mat_metadata)
        if not lookup_root_ids:
            df = pd.DataFrame(supervoxel_data, dtype=object)
            drop_col_names = list(df.loc[:, df.columns.str.endswith("position")])
            df = df.drop(drop_col_names, axis=1)
            segmentation_data = df.to_dict(orient="records")
        else:
            segmentation_data = get_new_root_ids(supervoxel_data, mat_metadata)
        result = insert_segmentation_data(segmentation_data, mat_metadata)
        celery_logger.debug(result)
        run_time = time.time() - start_time
        table_name = mat_metadata["annotation_table_name"]
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
    return {
        "Table name": f"{table_name}",
        "Run time": f"{run_time}",
    }


@celery.task(name="workflow:process_dense_missing_roots_workflow")
def process_dense_missing_roots_workflow(datastack_info: dict, **kwargs):
    """Chunk supervoxel ids and lookup root ids in batches
    for all tables in the database.


    -> workflow :
        find missing root_ids >
        lookup supervoxel ids from sql >
        get root_ids >
        merge root_ids list >
        insert root_ids


    Parameters
    ----------
    aligned_volume_name : str
        [description]
    segmentation_source : dict
        [description]
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
    )
    # filter for missing root ids (min/max ids)
    for mat_metadata in mat_info:
        if mat_metadata.get("segmentation_table_name"):
            find_dense_missing_root_ids_workflow(mat_metadata)


@celery.task(name="workflow:process_sparse_missing_roots_workflow")
def process_sparse_missing_roots_workflow(datastack_info: dict, table_name: str, **kwargs):
    """Find missing (ie NULL) root ids in the segmentation table. If missing root ids
    are found, lookup root ids. Uses last updated time stamp to find missing
    root ids.
    
    Parameters
    ----------
    datastack_info : dict
        datastack to run this workflow on
    table_name : str
        individual table to run this workflow on

    """
    mat_metadata = get_materialization_info(datastack_info, table_name=table_name)[0]
    mat_metadata["materialization_time_stamp"] = mat_metadata["last_updated_time_stamp"] # override materialization time stamp
    find_missing_root_ids_workflow(mat_metadata)


def batch_missing_root_ids_query(query, mat_metadata):
    # https://docs.sqlalchemy.org/en/14/core/connections.html#using-server-side-cursors-a-k-a-stream-results
    engine = db_manager.get_engine(mat_metadata["aligned_volume"])
    query_stmt = text(str(query))
    query_chunk_size = mat_metadata.get("chunk_size", 100)
    tasks = []
    with engine.connect() as conn:
        proxy = conn.execution_options(stream_results=True).execute(query_stmt)
        while "batch not empty":
            if mat_metadata.get("throttle_queues"):
                throttle_celery.wait_if_queue_full(queue_name="process")
            batch = proxy.fetchmany(query_chunk_size)  # fetch n_rows from chunk_size
            if not batch:
                celery_logger.debug(
                    "No rows left for %s", mat_metadata["annotation_table_name"]
                )
                break
            missing_root_data = [row[0] for row in batch] # convert from ResultProxy tuple object to serialize as json
            task = lookup_root_ids.si(mat_metadata, missing_root_data).apply_async()
            tasks.append(task.id)

        proxy.close()
    return tasks


def find_missing_root_ids_workflow(mat_metadata: dict):
    """Find missing root ids in the segmentation table. If missing root ids
    are found, lookup supervoxel ids and root ids in batches.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    celery task

    """
    query = get_ids_with_missing_roots(mat_metadata)
    tasks = batch_missing_root_ids_query(query, mat_metadata)
    tasks_completed = monitor_task_states(tasks)

    return fin.si()


def get_ids_with_missing_roots(mat_metadata: dict):
    """Get a chunk generator of the primary key ids for rows that contain
    at least one missing root id. Finds the min and max primary key id values
    globally across the table where a missing root id is present in a column.

    Args:
        mat_metadata (dict): materialization metadata

    Returns:
        query: sqlalchemy query which returns a list of IDs to
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")
    with db_manager.session_scope(aligned_volume) as session:

        columns = [seg_column.name for seg_column in SegmentationModel.__table__.columns]
        root_id_columns = [
            root_column for root_column in columns if "root_id" in root_column
        ]
        query_columns = [
            getattr(SegmentationModel, root_id_column).is_(None)
            for root_id_column in root_id_columns
        ]
        query = session.query(SegmentationModel.id).filter(or_(*query_columns))
        stmt = query.statement.compile(compile_kwargs={"literal_binds": True})

        return stmt


def find_dense_missing_root_ids_workflow(mat_metadata: dict):
    """Find missing root ids in the segmentation table. If missing root ids
    are found, lookup supervoxel ids and root ids in batches.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    celery task

    """
    missing_root_id_chunks = get_dense_ids_with_missing_roots(mat_metadata)
    seg_table = mat_metadata.get("segmentation_table_name")
    if missing_root_id_chunks:
        missing_root_id_chunks = [c for c in missing_root_id_chunks]
        process_chunks_workflow = chain(
            lookup_dense_missing_root_ids_workflow(mat_metadata, missing_root_id_chunks)
        ).apply_async()
        tasks_completed = monitor_workflow_state(process_chunks_workflow)
        if tasks_completed:
            return fin.si()
    else:
        celery_logger.debug(
            f"Skipped missing root id lookup for '{seg_table}', no missing root ids found"
        )
        return fin.si()


def get_dense_ids_with_missing_roots(mat_metadata: dict) -> List:
    """Get a chunk generator of the primary key ids for rows that contain
    at least one missing root id. Finds the min and max primary key id values
    globally across the table where a missing root id is present in a column.

    Args:
        mat_metadata (dict): materialization metadata

    Returns:
        List: generator of chunked primary key ids.
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    with db_manager.session_scope(aligned_volume) as session:

        columns = [seg_column.name for seg_column in SegmentationModel.__table__.columns]
        root_id_columns = [
            root_column for root_column in columns if "root_id" in root_column
        ]
        query_columns = [
            getattr(SegmentationModel, root_id_column).is_(None)
            for root_id_column in root_id_columns
        ]
        max_id = (
            session.query(func.max(SegmentationModel.id))
            .filter(or_(*query_columns))
            .scalar()
        )
        min_id = (
            session.query(func.min(SegmentationModel.id))
            .filter(or_(*query_columns))
            .scalar()
        )
        if min_id and max_id:
            if min_id < max_id:
                id_range = range(min_id, max_id + 1)
                return create_chunks(id_range, 500)
            elif min_id == max_id:
                return [min_id, min_id]
        else:
            celery_logger.info(
                f"No missing root_ids found in '{SegmentationModel.__table__.name}'"
            )
            return None


def lookup_dense_missing_root_ids_workflow(
    mat_metadata: dict, missing_root_id_chunks: List[int]
):
    """Celery workflow that finds and looks up missing root ids.

    Workflow:
                - Lookup supervoxel id(s)
                - Get root ids from supervoxels
                - insert into segmentation table

        Args:
            mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
            missing_root_id_chunks (List[int]): list of pk ids that have a missing root_id

        Returns:
            chain: chain of celery tasks
    """
    return chain(
        chord(
            [
                group(
                    lookup_root_ids_chunk.si(mat_metadata, missing_root_id_chunk),
                )
                for missing_root_id_chunk in missing_root_id_chunks
            ],
            fin.si(),
        ),
        update_metadata.si(mat_metadata),
    )


@celery.task(
    name="workflow:lookup_root_ids",
    acks_late=True,
    bind=True,
    autoretry_for=(Exception,),
    max_retries=6,
)
def lookup_root_ids(self, mat_metadata: dict, missing_root_ids: List[int]):
    """Get root ids from supervoxels. Insert into database.

    Args:
        mat_metadata (dict): metadata associated with the materialization
        missing_root_ids (List[int]): list of annotation ids

    Raises:
        self.retry: re-queue the tasks if failed. Retries 6 times.

    Returns:
        str: Name of table and runtime of task.
    """
    try:
        start_time = time.time()
        supervoxel_data = get_sql_supervoxel_ids(missing_root_ids, mat_metadata)
        root_id_data = get_new_root_ids(supervoxel_data, mat_metadata)
        result = update_segmentation_data(root_id_data, mat_metadata)
        celery_logger.info(result)
        run_time = time.time() - start_time
        table_name = mat_metadata["annotation_table_name"]
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
    return {"Table name": f"{table_name}", "Run time": f"{run_time}"}


@celery.task(
    name="workflow:lookup_root_ids_chunk",
    acks_late=True,
    bind=True,
    autoretry_for=(Exception,),
    max_retries=6,
)
def lookup_root_ids_chunk(self, mat_metadata: dict, missing_root_id_chunk: List[int]):
    """Get supervoxel ids with in chunk range. Lookup root_ids
    and insert into database.

    Args:
        mat_metadata (dict): metadata associated with the materialization
        missing_root_id_chunk (List[int]): list of annotation ids

    Raises:
        self.retry: re-queue the tasks if failed. Retries 6 times.

    Returns:
        str: Name of table and runtime of task.
    """
    try:
        start_time = time.time()
        chunk = [missing_root_id_chunk[0], missing_root_id_chunk[-1]]
        supervoxel_data = get_sql_supervoxel_ids_chunks(chunk, mat_metadata)
        root_id_data = get_new_root_ids(supervoxel_data, mat_metadata)
        result = update_segmentation_data(root_id_data, mat_metadata)
        celery_logger.info(result)
        run_time = time.time() - start_time
        table_name = mat_metadata["annotation_table_name"]
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=e, countdown=3)
    return {"Table name": f"{table_name}", "Run time": f"{run_time}"}


def find_ids_with_specified_roots(mat_metadata: dict, specific_root_ids=List[int]):
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    with db_manager.session_scope(aligned_volume) as session:
        engine = db_manager.get_engine(aligned_volume)
        # Columns to check for root_ids
        columns = [seg_column.name for seg_column in SegmentationModel.__table__.columns]
        root_id_columns = [
            root_column for root_column in columns if "root_id" in root_column
        ]

        root_id_queries = []
        for root_id_column in root_id_columns:
            query_columns = session.query(SegmentationModel.id).filter(
                getattr(SegmentationModel, root_id_column).in_(specific_root_ids)
            )

            compiled_statement = query_columns.statement.compile(engine)
            params = compiled_statement.params
            sql_str_with_params = str(compiled_statement).replace("\n", "")
            for key, value in params.items():
                sql_str_with_params = sql_str_with_params.replace(f"%({key})s", str(value))

            root_id_queries.append({f"{root_id_column}": sql_str_with_params})

        return root_id_queries


@celery.task(
    name="workflow:fix_root_id_workflow",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def fix_root_id_workflow(
    self, datastack_info: dict, table_name: str, bad_synapse_root_ids: List[int]
):
    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=None,
        skip_table=False,
        table_name=table_name,
    )
    for mat_metadata in mat_info:
        queries = find_ids_with_specified_roots(mat_metadata, bad_synapse_root_ids)

        aligned_volume = mat_metadata.get("aligned_volume")
        query_chunk_size = mat_metadata.get("chunk_size", 100)
        engine = db_manager.get_engine(aligned_volume)
        tasks = []
        for query_dict in queries:
            root_id_key = list(query_dict.keys())[
                0
            ]  # Extracting the root_id key from the dict
            query_stmt = text(list(query_dict.values())[0])
            with engine.connect() as conn:
                proxy = conn.execution_options(stream_results=True).execute(query_stmt)
                while "batch not empty":
                    batch = proxy.fetchmany(query_chunk_size)
                    if not batch:
                        break
                    data = pd.DataFrame(batch, columns=batch[0].keys(), dtype=object)
                    bad_root_ids = data.to_dict(orient="list")  # list of dicts
                    task = set_root_id_to_none_task.si(
                        mat_metadata, root_id_key, bad_root_ids
                    ).apply_async()
                    tasks.append(task.id)

        try:
            tasks_completed = monitor_task_states(tasks)
        except Exception as e:
            celery_logger.error(f"Monitor reports task failed: {e}")
            raise self.retry(exc=e, countdown=3)
        return workflow_complete.si("fix_root_id_workflow")


@celery.task(
    name="process:set_root_id_to_none_task",
    bind=True,
    acks_late=True,
    max_retries=5,
    autoretry_for=(Exception,),
)
def set_root_id_to_none_task(
    self, mat_metadata: dict, root_id_column: str, bad_root_ids: dict
):
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    with db_manager.session_scope(aligned_volume) as session:
        ids = bad_root_ids["id"]
        session.query(SegmentationModel).filter(SegmentationModel.id.in_(ids)).update(
            {getattr(SegmentationModel, root_id_column): None},
            synchronize_session="fetch",
        )


def ingest_new_annotations_workflow(mat_metadata: dict):
    """Celery workflow to ingest new annotations. In addition, it will
    create missing segmentation data table if it does not exist.
    Returns celery chain primitive.

    Workflow:
        - Create linked segmentation table if not exists
        - Find annotation data with missing segmentation data:
            - Lookup supervoxel id(s)
            - Get root ids from supervoxels
            - insert into segmentation table

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        annotation_chunks (List[int]): list of annotation primary key ids

    Returns:
        chain: chain of celery tasks
    """
    celery_logger.info("Ingesting new annotations...")
    if mat_metadata["row_count"] >= 1_000_000:
        return fin.si()
    annotation_chunks = generate_chunked_model_ids(mat_metadata)
    table_created = create_missing_segmentation_table(mat_metadata)
    if table_created:
        celery_logger.info(f'Table created: {mat_metadata["segmentation_table_name"]}')

    ingest_workflow = chain(
        chord(
            [
                chain(
                    ingest_new_annotations.si(annotation_chunk, mat_metadata),
                )
                for annotation_chunk in annotation_chunks
            ],
            fin.si(),
        )
    ).apply_async()
    tasks_completed = monitor_workflow_state(ingest_workflow)
    if tasks_completed:
        return fin.si()


@celery.task(name="workflow:create_missing_segmentation_table", acks_late=True)
def create_missing_segmentation_table(mat_metadata: dict) -> bool:
    """Create missing segmentation tables associated with an annotation table if it
    does not already exist.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns:
        bool: if segmentation table was created or already exists
    """
    segmentation_table_name = mat_metadata.get("segmentation_table_name")
    database = mat_metadata.get("database")

    SegmentationModel = create_segmentation_model(mat_metadata)

    with db_manager.session_scope(database) as session:
        engine = db_manager.get_engine(database)

        if (
            not session.query(SegmentationMetadata)
            .filter(SegmentationMetadata.table_name == segmentation_table_name)
            .scalar()
        ):
            SegmentationModel.__table__.create(bind=engine, checkfirst=True)
            creation_time = datetime.datetime.utcnow()
            metadata_dict = {
                "annotation_table": mat_metadata.get("annotation_table_name"),
                "schema_type": mat_metadata.get("schema"),
                "table_name": segmentation_table_name,
                "valid": True,
                "created": creation_time,
                "pcg_table_name": mat_metadata.get("pcg_table_name"),
            }

            seg_metadata = SegmentationMetadata(**metadata_dict)
            session.add(seg_metadata)
    return True


def get_annotations_with_missing_supervoxel_ids(
    mat_metadata: dict, chunk: List[int], ids_list: List[int] = None
) -> dict:
    """Get list of valid annotation and their ids to lookup existing supervoxel ids. If there
    are missing supervoxels they will be set as None for cloudvolume lookup.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata
    chunk : list
        chunked range to for sql id query

    Returns
    -------
    dict
        dict of annotation and segmentation data
    """

    database = mat_metadata.get("database")
    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=True)
    SegmentationModel = create_segmentation_model(mat_metadata)

    with db_manager.session_scope(database) as session:

        anno_model_cols, __, supervoxel_columns = get_query_columns_by_suffix(
            AnnotationModel, SegmentationModel, "supervoxel_id"
        )

        query = session.query(*anno_model_cols)
        if ids_list:
            id_query = AnnotationModel.id.in_(ids_list)
        else:
            id_query = query_id_range(AnnotationModel.id, chunk[0], chunk[1])

    annotation_data = [
        data
        for data in query.filter(id_query)
        .order_by(AnnotationModel.id)
        .filter(AnnotationModel.valid == True)
        .join(SegmentationModel, isouter=True)
        .filter(SegmentationModel.id == None)
    ]

    annotation_dataframe = pd.DataFrame(annotation_data, dtype=object)
    if not annotation_dataframe.empty:
        wkb_data = annotation_dataframe.loc[
            :, annotation_dataframe.columns.str.endswith("position")
        ]

        annotation_dict = {}
        for column, wkb_points in wkb_data.items():
            annotation_dict[column] = [
                get_geom_from_wkb(wkb_point) for wkb_point in wkb_points
            ]

        for key, value in annotation_dict.items():
            annotation_dataframe.loc[:, key] = value

        segmentation_dataframe = pd.DataFrame(columns=supervoxel_columns, dtype=object)
        segmentation_dataframe = segmentation_dataframe.fillna(value=np.nan)
        mat_df = pd.concat((segmentation_dataframe, annotation_dataframe), axis=1)
        materialization_data = mat_df.to_dict(orient="list")
    else:
        materialization_data = None
    return materialization_data


def get_cloudvolume_supervoxel_ids(
    materialization_data: dict, mat_metadata: dict
) -> dict:
    """Lookup missing supervoxel ids.

    Parameters
    ----------
    materialization_data : dict
        dict of annotation and segmentation data
    metadata : dict
        Materialization metadata

    Returns
    -------
    dict
        dict of annotation and with updated supervoxel id data
    """
    mat_df = pd.DataFrame(materialization_data, dtype=object)

    segmentation_source = mat_metadata.get("segmentation_source")
    coord_resolution = mat_metadata.get("coord_resolution")

    cv = cloudvolume.CloudVolume(
        segmentation_source, mip=0, use_https=True, bounded=False, fill_missing=True
    )

    position_data = mat_df.loc[:, mat_df.columns.str.endswith("position")]
    for data in mat_df.itertuples():
        for col in list(position_data):
            supervoxel_column = f"{col.rsplit('_', 1)[0]}_supervoxel_id"
            if np.isnan(getattr(data, supervoxel_column)):
                pos_data = getattr(data, col)
                pos_array = np.asarray(pos_data)
                try:
                    svid = get_sv_id(
                        cv, pos_array, coord_resolution
                    )  # pylint: disable=maybe-no-member
                except Exception as e:
                    celery_logger.error(
                        f"Failed to get SVID: {pos_array}, {coord_resolution}. Error {e}"
                    )
                    raise e
                mat_df.loc[mat_df.id == data.id, supervoxel_column] = svid
    return mat_df.to_dict(orient="list")


def get_sv_id(cv, pos_array: np.array, coord_resolution: list) -> np.array:
    svid = np.squeeze(
        cv.download_point(pt=pos_array, size=1, coord_resolution=coord_resolution)
    )  # pylint: disable=maybe-no-member
    return svid


def get_sql_supervoxel_ids(ids: List[int], mat_metadata: dict) -> List[int]:
    """Iterates over columns with 'supervoxel_id' present in the name and
    returns supervoxel ids between start and stop ids.

    Parameters
    ----------
    ids: List[int]
        list of IDs to cahnge
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[int]
        list of supervoxel ids between 'start_id' and 'end_id'
    """
    segmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")
    with db_manager.session_scope(aligned_volume) as session:

        columns = [
            model_column.name for model_column in segmentationModel.__table__.columns
        ]
        supervoxel_id_columns = [
            model_column for model_column in columns if "supervoxel_id" in model_column
        ]
        mapped_columns = [
            getattr(segmentationModel, supervoxel_id_column)
            for supervoxel_id_column in supervoxel_id_columns
        ]
        filter_query = session.query(segmentationModel.id, *mapped_columns)
        query = filter_query.filter(segmentationModel.id.in_(ids))

        data = query.all()
        df = pd.DataFrame(data)
        return df.to_dict(orient="list")


def get_sql_supervoxel_ids_chunks(chunks: List[int], mat_metadata: dict) -> List[int]:
    """Iterates over columns with 'supervoxel_id' present in the name and
    returns supervoxel ids between start and stop ids.

    Parameters
    ----------
    chunks: dict
        name of database to target
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[int]
        list of supervoxel ids between 'start_id' and 'end_id'
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")
    with db_manager.session_scope(aligned_volume) as session:

        columns = [
            model_column.name for model_column in SegmentationModel.__table__.columns
        ]
        supervoxel_id_columns = [
            model_column for model_column in columns if "supervoxel_id" in model_column
        ]
        mapped_columns = [
            getattr(SegmentationModel, supervoxel_id_column)
            for supervoxel_id_column in supervoxel_id_columns
        ]
        filter_query = session.query(SegmentationModel.id, *mapped_columns)
        if len(chunks) > 1:
            query = filter_query.filter(
                or_(SegmentationModel.id).between(int(chunks[0]), int(chunks[1]))
            )
        elif len(chunks) == 1:
            query = filter_query.filter(SegmentationModel.id == chunks[0])

        data = query.all()
        df = pd.DataFrame(data)
        return df.to_dict(orient="list")



def get_new_root_ids(materialization_data: dict, mat_metadata: dict) -> dict:
    """Get root ids

    Args:
        materialization_data (dict): supervoxel data for root_id lookup
        mat_metadata (dict): Materialization metadata

    Returns:
        dict: root_ids to be inserted into db
    """
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
    anno_ids = supervoxel_df["id"].to_list()

    with db_manager.session_scope(database) as session:
        current_root_ids = [
                data
                for data in session.query(*seg_model_cols).filter(
                    or_(SegmentationModel.id.in_(anno_ids))
                )
            ]

    supervoxel_col_names = list(
        supervoxel_df.loc[:, supervoxel_df.columns.str.endswith("supervoxel_id")]
    )

    if current_root_ids:
        # merge root_id df with supervoxel df
        df = pd.DataFrame(current_root_ids, dtype=object)
        root_ids_df = pd.merge(supervoxel_df, df)

    else:
        # create empty dataframe with root_id columns
        root_id_columns = [
            col_name.replace("supervoxel_id", "root_id")
            for col_name in supervoxel_col_names
            if "supervoxel_id" in col_name
        ]
        df = pd.DataFrame(columns=root_id_columns, dtype=object).fillna(value=np.nan)
        root_ids_df = pd.concat((supervoxel_df, df), axis=1)

    cols = [x for x in root_ids_df.columns if "root_id" in x]

    cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)

    # filter missing root_ids and lookup root_ids if missing or zero
    mask = np.logical_and.reduce(
        [(root_ids_df[col].isna() | (root_ids_df[col] == 0)) for col in cols]
    )
    missing_root_rows = root_ids_df.loc[mask]
    if not missing_root_rows.empty:
        supervoxel_data = missing_root_rows.loc[:, supervoxel_col_names]
        for col_name in supervoxel_data:
            if "supervoxel_id" in col_name:
                root_id_name = col_name.replace("supervoxel_id", "root_id")
                data = missing_root_rows.loc[:, col_name]
                root_id_array = get_root_ids(
                    cg_client, data, materialization_time_stamp
                )
                root_ids_df.loc[data.index, root_id_name] = root_id_array

    return root_ids_df.to_dict(orient="records")


def get_root_ids(cgclient, data, materialization_time_stamp):
    root_id_array = np.squeeze(
        cgclient.get_roots(data, timestamp=materialization_time_stamp)
    )
    return root_id_array


def update_segmentation_data(materialization_data: dict, mat_metadata: dict) -> dict:
    if not materialization_data:
        return {"status": "empty"}

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    with db_manager.session_scope(aligned_volume) as session:
        session.bulk_update_mappings(SegmentationModel, materialization_data)

    return f"Number of rows updated: {len(materialization_data)}"


def insert_segmentation_data(materialization_data: dict, mat_metadata: dict) -> dict:
    """Insert supervoxel and root id data into segmentation table.

    Args:
        materialization_data (dict): supervoxel and/or root id data
        mat_metadata (dict): materialization metadata

    Returns:
        dict: returns description of number of rows inserted
    """
    if not materialization_data:
        return {"status": "empty"}

    SegmentationModel = create_segmentation_model(mat_metadata)
    aligned_volume = mat_metadata.get("aligned_volume")

    engine = db_manager.get_engine(aligned_volume)

    with engine.begin() as connection:
        connection.execute(
            SegmentationModel.__table__.insert(), materialization_data
        )

    return {"Segmentation data inserted": len(materialization_data)}
