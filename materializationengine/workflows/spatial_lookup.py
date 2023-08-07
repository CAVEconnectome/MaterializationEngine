import collections
import datetime
import itertools
import random
import time
from itertools import islice
from typing import List

import numpy as np
import pandas as pd
from celery import group
from celery.utils.log import get_task_logger
from geoalchemy2 import Geometry
from shapely import wkb
from sqlalchemy import case, func, literal, select, text, union_all
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from materializationengine.celery_init import celery
from materializationengine.cloudvolume_gateway import cloudvolume_cache
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.shared_tasks import (
    get_materialization_info,
    workflow_complete,
)
from materializationengine.throttle import throttle_celery
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_new_root_ids,
)

celery_logger = get_task_logger(__name__)


def get_cloud_volume_info(segmentation_source: str) -> dict:
    """Get the bounding box, chunk size, and resolution from cloudvolume.

    Args:
        segmentation_source (str): Name of the cloud volume source

    Raises:
        e: Cloud volume error

    Returns:
        dict: Dictionary of the cloudvolume bounding box, chunk size, and resolution
    """
    try:
        cv = cloudvolume_cache.get_cv(segmentation_source)
    except Exception as e:
        celery_logger.error(e)
        raise e
    bbox = cv.bounds.expand_to_chunk_size(cv.chunk_size)
    bbox = bbox.to_list()
    chunk_size = cv.chunk_size.tolist()

    return {
        "bbox": bbox,
        "chunk_size": np.array(chunk_size),
        "resolution": cv.scale["resolution"],
    }


def get_table_bounding_boxes(aligned_volume: str, table_name: str) -> dict:
    """Get the bounding boxes of the annotation table.

    Args:
        aligned_volume (str): Name of the aligned volume to use as the database
        table_name (str): Name of the annotation table

    Returns:
        dict: Dictionary of bounding boxes for each column in the annotation table
    """
    db = dynamic_annotation_cache.get_db(aligned_volume)
    schema = db.database.get_table_schema(table_name)
    AnnotationModel = db.schema.create_annotation_model(table_name, schema)
    SegmentationModel = db.schema.create_segmentation_model(
        table_name, schema, "minnie3_v1"
    )
    engine = sqlalchemy_cache.get_engine(aligned_volume)
    bbox_data = []

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
                with engine.begin() as connection:
                    start_time = time.time()
                    query_str = f"SELECT ST_3DExtent({annotation_column}) FROM {AnnotationModel.__table__}"
                    bbox3d = connection.execute(query_str).scalar()
                    celery_logger.debug(bbox3d)
                    total_time = time.time() - start_time

                    coords = bbox3d[6:-1].split(",")
                    box3d_array = np.array(
                        [list(map(int, coord.split())) for coord in coords]
                    )
                    bbox_data.append(box3d_array)
                    celery_logger.debug(
                        f"Time to find bounding box of column {annotation_column} is {total_time}"
                    )
                    celery_logger.debug(
                        f"Bounding box for {annotation_column}:  min_max_xyz): {box3d_array}"
                    )
            else:
                continue

    return bbox_data


def chunk_generator(
    min_coord: np.array,
    max_coord: np.array,
    chunk_size: List,
    chunk_scale_factor: int = 1,
) -> tuple:
    """Generate chunks of a given size that cover a given bounding box.

    Args:
        min_coord (np.array): Min corner of the bounding box
        max_coord (np.array): Max corner of the bounding box
        chunk_size (List): Chunk size
        chunk_scale_factor (int, optional): Scale factor for chunk size. Defaults to 1.

    Yields:
        Iterator[tuple]: chunk min corner, chunk max corner, chunk index, total number of chunks
    """
    # Determine the number of chunks in each dimension
    chunk_size = chunk_size * chunk_scale_factor
    num_chunks = np.ceil((max_coord - min_coord) / chunk_size).astype(int)
    i_chunks = np.prod(num_chunks)
    # Iterate over the chunks
    for chunk_index in itertools.product(*(range(n) for n in num_chunks)):
        chunk_index = np.array(chunk_index)

        # Determine the min and max corner of this chunk
        min_corner = min_coord + chunk_index * chunk_size
        max_corner = np.minimum(min_corner + chunk_size, max_coord)

        yield min_corner, max_corner, chunk_index, i_chunks


def calc_min_enclosing_and_sub_volumes(
    input_bboxes: List,
    global_bbox: List,
    chunk_size: List,
    cv_resolution: List,
    coord_resolution: List,
) -> tuple:
    """Calculate the minimum enclosing bounding box and sub-volumes
    that are outside of the global bounding box.

    Args:
        input_bboxes (List): bounding boxes of the annotation table
        global_bbox (List): the cloud volume bounding box
        chunk_size (List): cloud volume chunk size
        cv_resolution (List): cloud volume resolution
        coord_resolution (List): annotation table resolution

    Returns:
        tuple: the minimum enclosing bounding box and sub-volumes that are outside of the global bounding box
    """
    min_corner = np.array([np.inf, np.inf, np.inf])
    max_corner = np.array([-np.inf, -np.inf, -np.inf])
    global_bbox = np.array([global_bbox], dtype=float).reshape(2, 3)
    sub_volumes = []

    scale_factor = np.array(cv_resolution) / np.array(coord_resolution)
    global_bbox *= scale_factor

    for bbox in input_bboxes:
        # Align bbox to chunk size
        min_chunk_aligned = bbox[0] // chunk_size * chunk_size
        max_chunk_aligned = (bbox[1] + chunk_size - 1) // chunk_size * chunk_size
        aligned_bbox = np.array([min_chunk_aligned, max_chunk_aligned])
        # Check if bbox is not within global bbox
        if np.any(global_bbox[0] > aligned_bbox[0]) or np.any(
            aligned_bbox[1] > global_bbox[1]
        ):
            # Calculate sub-volumes that are outside of the global bbox
            outside_min = np.minimum(aligned_bbox[0], global_bbox[0])
            outside_max = np.maximum(aligned_bbox[1], global_bbox[1])

            if outside_min[0] < global_bbox[0, 0]:
                sub_volumes.append(
                    np.array(
                        [
                            outside_min,
                            [global_bbox[0, 0] - 1, outside_max[1], outside_max[2]],
                        ]
                    )
                )
            if outside_max[0] > global_bbox[1, 0]:
                sub_volumes.append(
                    np.array(
                        [
                            [global_bbox[1, 0] + 1, outside_min[1], outside_min[2]],
                            outside_max,
                        ]
                    )
                )
            if outside_min[1] < global_bbox[0, 1]:
                sub_volumes.append(
                    np.array(
                        [
                            outside_min,
                            [outside_max[0], global_bbox[0, 1] - 1, outside_max[2]],
                        ]
                    )
                )["chunk_size"]
            if outside_max[1] > global_bbox[1, 1]:
                sub_volumes.append(
                    np.array(
                        [
                            [outside_min[0], global_bbox[1, 1] + 1, outside_min[2]],
                            outside_max,
                        ]
                    )
                )
            if outside_min[2] < global_bbox[0, 2]:
                sub_volumes.append(
                    np.array(
                        [
                            outside_min,
                            [outside_max[0], outside_max[1], global_bbox[0, 2] - 1],
                        ]
                    )
                )
            if outside_max[2] > global_bbox[1, 2]:
                sub_volumes.append(
                    np.array(
                        [
                            [outside_min[0], outside_min[1], global_bbox[1, 2] + 1],
                            outside_max,
                        ]
                    )
                )

        # Update min and max corners within global_bbox
        min_corner = np.minimum(min_corner, np.maximum(aligned_bbox[0], global_bbox[0]))
        max_corner = np.maximum(max_corner, np.minimum(aligned_bbox[1], global_bbox[1]))
        print(f"MIN: {min_corner}, MAX: {max_corner}, {global_bbox}")
    if np.any(min_corner == np.inf) or np.any(max_corner == -np.inf):
        return None, []  # No input bounding boxes were within the global bounding box

    aligned_bbox = np.array([min_corner, max_corner])
    return aligned_bbox, sub_volumes


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
    # print(spatial_column.name)
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

    AnnotationModel = db.schema.create_annotation_model(table_name, schema)
    SegmentationModel = db.schema.create_segmentation_model(
        table_name, schema, mat_info["segmentation_source"]
    )

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


def format_pt_data(pt_array: np.array, chunk_size: List) -> collections.defaultdict:
    """Format point data into a dictionary of chunk start and points in that chunk.

    Args:
        pt_array (np.array): An array of points
        chunk_size (List): Chunk size to use for formatting

    Returns:
        collections.defaultdict: Dictionary of chunk start and points in that chunk
    """
    chunk_map_dict = collections.defaultdict(set)

    processing_chunk_size = np.array(chunk_size)
    chunk_starts = (
        (pt_array).astype(int) // processing_chunk_size * processing_chunk_size
    )  # // is floor division
    for point, chunk_start in zip(pt_array, chunk_starts):

        celery_logger.debug(f"POINT: {point}, CHUNK START: {chunk_start}")
        chunk_map_dict[tuple(chunk_start)].add(tuple(point))
    return chunk_map_dict


def load_chunk(
    cv: cloudvolume_cache, chunk_start: np.array, chunk_end: np.array
) -> np.array:
    """Load a chunk from cloudvolume.

    Args:
        cv (cloudvolume_cache): Cloudvolume instance
        chunk_start (np.array): Start of the chunk coordinates
        chunk_end (np.array): End of the chunk coordinates

    Returns:
        np.array: A cutout of the volume
    """
    return cv[
        chunk_start[0] : chunk_end[0],
        chunk_start[1] : chunk_end[1],
        chunk_start[2] : chunk_end[2],
    ]


def get_svids_in_chunk(
    cv: cloudvolume_cache,
    chunk_map_key: tuple,
    chunk_data: np.array,
    coord_resolution: List,
) -> tuple:
    """Get the supervoxel ids in a chunk.

    Args:
        cv (cloudvolume_cache): Cloudvolume instance
        chunk_map_key (tuple): Chunk start coordinates
        chunk_data (np.array): Points in the chunk
        coord_resolution (List): Annotation table resolution

    Returns:
        tuple: Points in the chunk, supervoxel ids in the chunk
    """
    chunk_start = np.array(chunk_map_key)
    points = np.array(list(chunk_data))

    resolution = np.array(cv.scale["resolution"]) / coord_resolution

    indices = (points // resolution).astype(int) - chunk_start

    mn, mx = indices.min(axis=0), indices.max(axis=0)
    chunk_end = chunk_start + mx + 1
    chunk_start += mn
    indices -= mn

    chunk = load_chunk(cv, chunk_start, chunk_end)
    return points, chunk[indices[:, 0], indices[:, 1], indices[:, 2]]


def get_svids_from_df(df, mat_info: dict) -> pd.DataFrame:
    """Get the supervoxel ids from a dataframe of points.

    Args:
        df (pd.DataFrame): Dataframe containing annotation points
        mat_info (dict): Materialization info for a given table

    Returns:
        pd.DataFrame: Dataframe of points with supervoxel ids
    """
    segmentation_source = mat_info["segmentation_source"]
    coord_resolution = mat_info["coord_resolution"]
    cv = cloudvolume_cache.get_cv(segmentation_source)
    pt_pos_data = df["pt_position"].apply(np.array)
    pt_array = np.squeeze(np.stack(pt_pos_data.values))
    if pt_array.ndim == 1:
        pt_array = pt_array[np.newaxis, :]
    chunk_map_dict = format_pt_data(pt_array, cv.chunk_size.tolist())
    svids = [
        get_svids_in_chunk(
            cv, chunk_map_key=k, chunk_data=v, coord_resolution=coord_resolution
        )
        for k, v in chunk_map_dict.items()
    ]
    svid_dict = dict(
        zip(
            [tuple(p) for svid in svids for p in svid[0]],
            [i for svid in svids for i in svid[1]],
        )
    )
    df["svids"] = np.array([svid_dict[tuple(pt)] for pt in pt_array])

    # Add the supervoxel id column to the type column name
    if df["type"].str.contains("pt").all():
        df["type"] = df["type"].apply(lambda x: f"{x}_supervoxel_id")
    else:
        df["type"] = df["type"].apply(lambda x: f"{x}_pt_supervoxel_id")

    # Pivot the dataframe
    df = df.pivot(index="id", columns="type", values="svids")

    # Reset index
    df.reset_index(inplace=True)
    return df


def get_min_enclosing_bbox(cv_info: dict, mat_info: dict) -> tuple:
    """Calculate the minimum enclosing bounding box and sub-volumes using
    the cloud volume bounding box and the annotation table bounding boxes.

    Args:
        cv_info (dict): Dictionary of the cloudvolume bounding box, chunk size, and resolution
        mat_info (dict): Materialization info for a given table

    Returns:
        tuple: The minimum enclosing bounding box and sub-volumes
    """
    aligned_volume = mat_info["aligned_volume"]
    table_name = mat_info["annotation_table_name"]
    coord_resolution = mat_info["coord_resolution"]

    annotation_table_bounding_boxes = get_table_bounding_boxes(
        aligned_volume=aligned_volume, table_name=table_name
    )
    min_enclosing_bbox, outside_volume = calc_min_enclosing_and_sub_volumes(
        annotation_table_bounding_boxes,
        cv_info["bbox"],
        cv_info["chunk_size"],
        cv_resolution=cv_info["resolution"],
        coord_resolution=coord_resolution,
    )
    return min_enclosing_bbox, outside_volume


# Create a group for the tasks
def chunk_tasks(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    iterable = iter(iterable)
    while True:
        chunk = list(islice(iterable, n))
        if not chunk:
            return
        yield chunk


@celery.task(
    name="workflow:spatial_lookup_workflow",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def run_spatial_lookup_workflow(
    self,
    datastack_info: dict,
    table_name: str,
    chunk_scale_factor: int = 12,
    get_root_ids: bool = True,
):
    """Run the spatial lookup workflow.

    Args:
        self (celery.task): Celery task
        datastack_info (dict): Datastack info
        table_name (str): Annotation table name
        chunk_scale_factor (int, optional): Scale factor for chunk size. Defaults to 12.

    Raises:
        e: Exception

    Returns:
        celery.task: Celery task
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    table_info = get_materialization_info(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
        table_name=table_name,
        skip_row_count=True,
    )

    r = self.app.redis
    for mat_info in table_info:
        try:
            table_created = create_missing_segmentation_table(mat_info)
            celery_logger.info(f"Segmentation table created or exits: {table_created}")
            cv_info = get_cloud_volume_info(mat_info["segmentation_source"])
            celery_logger.info(f"Cloud volume info: {cv_info}")

            min_enclosing_bbox, _ = get_min_enclosing_bbox(
                cv_info,
                mat_info,
            )
            celery_logger.info(f"Mim enclosing bbox: {min_enclosing_bbox}")
        except Exception as e:
            celery_logger.error(e)
            raise self.retry(exc=e, countdown=3)

        task_group_chunk_size = 100  # define your chunk size here

        chunks = chunk_tasks(
            chunk_generator(
                min_enclosing_bbox[0],
                min_enclosing_bbox[1],
                cv_info["chunk_size"],
                chunk_scale_factor,
            ),
            task_group_chunk_size,
        )

        # Count the total number of tasks
        num_tasks = sum(1 for _ in chunks)

        # Set the counter in Redis
        r.set("total_tasks", num_tasks * task_group_chunk_size)

        # Reset the chunks generator as it has been consumed
        chunks = chunk_tasks(
            chunk_generator(
                min_enclosing_bbox[0],
                min_enclosing_bbox[1],
                cv_info["chunk_size"],
                chunk_scale_factor,
            ),
            task_group_chunk_size,
        )

        jobs = [
            group(
                process_spatially_chunked_svids.si(
                    stmt_str=str(
                        select(
                            [
                                select_all_points_in_bbox(
                                    min_corner, max_corner, mat_info
                                )
                            ]
                        ).compile(compile_kwargs={"literal_binds": True})
                    ),
                    mat_info=mat_info,
                    get_root_ids=get_root_ids,
                )
                for min_corner, max_corner, chunk_index, num_chunks in chunk_data
            )
            for chunk_data in chunks
        ]

        for job in jobs:
            if mat_info.get("throttle_queues"):
                throttle_celery.wait_if_queue_full(queue_name="process")
            result = job.apply_async()

    return workflow_complete.si("Spatial Lookup Workflow")


@celery.task(
    name="process:process_spatially_chunked_svids",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=10,
)
def process_spatially_chunked_svids(
    self, stmt_str: str, mat_info: dict, get_root_ids: bool = True
):
    """Reads the points from the database and gets the supervoxel ids for each point.

    Args:
        self (celery.task): Celery task
        stmt_str (str): sqlalchemy query statement string
        mat_info (dict): Materialization info for a given table

    Raises:
        e: Exception

    Returns:
        celery.task: Celery task
    """

    try:
        stmt = text(stmt_str)
        engine = sqlalchemy_cache.get_engine(aligned_volume=mat_info["aligned_volume"])
        with engine.connect() as connection:
            df = pd.read_sql(stmt, connection)
            if not df.empty:
                df["pt_position"] = df["pt_position"].apply(
                    lambda geom: np.array(wkb.loads(bytes(geom)).coords, dtype=np.int64)
                )
                data = get_svids_from_df(df, mat_info)
                celery_logger.info(f"Number of svids: {len(data)}")
                if get_root_ids:
                    data = get_new_root_ids(data, mat_info)
                is_inserted = insert_segmentation_data(data, mat_info)
                celery_logger.debug(
                    f"Data inserted: {is_inserted}, Number of rows: {len(data)}"
                )
    except Exception as e:
        celery_logger.error(e)
        self.retry(exc=e, countdown=int(random.uniform(2, 8) ** self.request.retries))


def insert_segmentation_data(
    data: pd.DataFrame,
    mat_info: dict,
):

    aligned_volume = mat_info["aligned_volume"]
    table_name = mat_info["annotation_table_name"]
    segmentation_source = mat_info["pcg_table_name"]
    db = dynamic_annotation_cache.get_db(aligned_volume)
    schema = db.database.get_table_schema(table_name)
    engine = sqlalchemy_cache.get_engine(aligned_volume)

    SegmentationModel = db.schema.create_segmentation_model(
        table_name, schema, segmentation_source
    )
    seg_columns = SegmentationModel.__table__.columns.keys()
    segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
    data_df = pd.DataFrame(data, dtype=object)

    # find the common columns between the two dataframes
    common_cols = segmentation_dataframe.columns.intersection(data_df.columns)
    df_cols = segmentation_dataframe.columns.difference(data_df.columns)

    # merge the dataframes and fill the missing values with -1, data might get updated in the next chunk lookup
    df = pd.merge(
        segmentation_dataframe[common_cols], data_df[common_cols], how="right"
    ).fillna(0)

    # reorder the columns
    df = df[segmentation_dataframe.columns]

    # convert the dataframe to a list of dictionaries
    data = df.to_dict(orient="records")

    # create the insert statement with on conflict do update clause
    # to update the data if it already exists in the table
    # if the new value is not 0 then update the value, otherwise keep the old (0) value
    stmt = insert(SegmentationModel).values(data)
    do_update_stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            column.name: case(
                [(stmt.excluded[column.name] != 0, stmt.excluded[column.name])],
                else_=column,
            )
            for column in SegmentationModel.__table__.columns
            if column.name != "id"
        },
    )

    # insert the data or update if it already exists
    with engine.begin() as connection:
        connection.execute(do_update_stmt)
    return True
