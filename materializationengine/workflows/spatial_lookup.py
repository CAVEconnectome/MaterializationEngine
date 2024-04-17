import collections
import datetime
import itertools
import random
import time
from itertools import islice
from typing import List

import numpy as np
import pandas as pd
from celery.utils.log import get_task_logger
from geoalchemy2 import Geometry
from sqlalchemy import case, func, literal, select, union_all
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import func

from materializationengine.celery_init import celery
from materializationengine.cloudvolume_gateway import cloudvolume_cache
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.shared_tasks import (
    get_materialization_info,
    workflow_complete,
)
from materializationengine.utils import get_geom_from_wkb
from materializationengine.throttle import throttle_celery
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_new_root_ids,
)
from materializationengine.utils import (
    create_segmentation_model,
    create_annotation_model,
)

celery_logger = get_task_logger(__name__)


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
    chunk_scale_factor: int = 8,
    get_root_ids: bool = True,
    upload_to_database: bool = True,
    task_group_chunk_size: int = 100,
):
    """Run the spatial lookup workflow.

    Args:
        self (celery.task): Celery task
        datastack_info (dict): Datastack info
        table_name (str): Annotation table name
        chunk_scale_factor (int, optional): Scale factor for chunk size. Defaults to 10.

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
    celery_logger.info(f"Table info: {table_info}")
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

        chunks = chunk_tasks(
            chunk_generator(
                min_enclosing_bbox[0],
                min_enclosing_bbox[1],
                cv_info,
                mat_info,
                chunk_scale_factor,
            ),
            task_group_chunk_size,
        )
        for i, chunk_data in enumerate(chunks):
            celery_logger.debug(f"Chunk {i}")
            for j, chunk_info in enumerate(chunk_data):
                min_corner, max_corner, chunk_index, num_chunks = chunk_info
                celery_logger.debug(
                    f"Sub-Chunk {chunk_index}: {min_corner} to {max_corner}"
                )

                task = process_spatially_chunked_svids.si(
                    min_corner=min_corner.tolist(),
                    max_corner=max_corner.tolist(),
                    mat_info=mat_info,
                    get_root_ids=get_root_ids,
                    upload_to_database=upload_to_database
                )
                result = task.apply_async()

                if mat_info.get("throttle_queues"):
                    throttle_celery.wait_if_queue_full(queue_name="process")

    return workflow_complete.si("Spatial Lookup Workflow")


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
    segmentation_source = mat_info["pcg_table_name"]

    annotation_table_bounding_boxes = get_table_bounding_boxes(
        aligned_volume=aligned_volume,
        table_name=table_name,
        segmentation_source=segmentation_source,
    )
    min_enclosing_bbox, outside_volume = calc_min_enclosing_and_sub_volumes(
        annotation_table_bounding_boxes,
        cv_info["bbox"],
        cv_info["chunk_size"],
        cv_resolution=cv_info["resolution"],
        coord_resolution=coord_resolution,
    )
    return min_enclosing_bbox, outside_volume


@celery.task(
    name="process:process_spatially_chunked_svids",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=10,
)
def process_spatially_chunked_svids(
    self, min_corner, max_corner, mat_info, get_root_ids: bool = True, upload_to_database: bool = True
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
        start_time = time.time()
        pts_df = get_pts_from_bbox(np.array(min_corner), np.array(max_corner), mat_info)
        celery_logger.info(f"Time to get points from bbox: {time.time() - start_time}")
        if pts_df is None:
            return None
        data = get_svids_from_df(pts_df, mat_info)
        # time to get svids
        celery_logger.info(f"Time to get svids: {time.time() - start_time}")
        celery_logger.info(f"Number of svids: {len(data['id'])}")
        if get_root_ids:
            # get time for root ids
            start_time = time.time()
            data = get_new_root_ids(data, mat_info)
            celery_logger.info(f"Time to get root ids: {time.time() - start_time}")
        if upload_to_database:
            # time to insert data
            start_time = time.time()
            is_inserted = insert_segmentation_data(data, mat_info)
            celery_logger.info(f"Time to insert data: {time.time() - start_time}")

            celery_logger.info(
                f"Data inserted: {is_inserted}, Number of rows: {len(data)}"
            )
    except Exception as e:
        celery_logger.error(e)
        self.retry(exc=e, countdown=int(random.uniform(2, 6) ** self.request.retries))


def get_pts_from_bbox(min_corner, max_corner, mat_info):
    stmt = select(
        [select_all_points_in_bbox(min_corner, max_corner, mat_info)]
    ).compile(compile_kwargs={"literal_binds": True})

    engine = sqlalchemy_cache.get_engine(aligned_volume=mat_info["aligned_volume"])
    with engine.connect() as connection:
        df = pd.read_sql(stmt, connection)
        # if the dataframe is empty then there are no points in the bounding box
        # so we can skip the rest of the workflow
        if df.empty:
            # self.request.chain = None
            return None
        # convert the point column to a numpy array
        df["pt_position"] = df["pt_position"].apply(lambda pt: get_geom_from_wkb(pt))

        return df


def match_point_and_get_value(point, points_map):
    point_tuple = tuple(point)  # Convert list to tuple
    return points_map.get(point_tuple, 0)


def normalize_positions(point, scale_factor):
    scaled_point = np.floor(np.array(point) / scale_factor).astype(int)
    return tuple(scaled_point)


def get_svids_from_df(df, mat_info: dict) -> pd.DataFrame:
    """Get the supervoxel ids from a dataframe of points.

    Args:
        df (pd.DataFrame): Dataframe containing annotation points
        mat_info (dict): Materialization info for a given table

    Returns:
        pd.DataFrame: Dataframe of points with supervoxel ids
    """
    start_time = time.time()
    segmentation_source = mat_info["segmentation_source"]
    coord_resolution = mat_info["coord_resolution"]
    cv = cloudvolume_cache.get_cv(segmentation_source)
    scale_factor = cv.resolution / coord_resolution

    # Scale the points to the resolution of the cloudvolume
    df["pt_position_scaled"] = df["pt_position"].apply(
        lambda x: normalize_positions(x, scale_factor)
    )

    sv_id_data = cv.scattered_points(
        df["pt_position"], coord_resolution=coord_resolution
    )

    # Match points to svids using the scaled coordinates
    df["svids"] = df["pt_position_scaled"].apply(
        lambda x: match_point_and_get_value(x, sv_id_data)
    )

    # Drop the temporary scaled coordinates column
    df.drop(columns=["pt_position_scaled"], inplace=True)

    # Add the supervoxel id column to the type column name
    if df["type"].str.contains("pt").all():
        df["type"] = df["type"].apply(lambda x: f"{x}_supervoxel_id")
    else:
        df["type"] = df["type"].apply(lambda x: f"{x}_pt_supervoxel_id")

    # custom pivot to preserve uint64 dtype values since they are normally converted to float64
    # add we loose precision when converting back to uint64
    svid_dict = _safe_pivot_svid_df_to_dict(df)

    celery_logger.info(f"Time to get svids from df: {time.time() - start_time}")
    return svid_dict


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


def get_table_bounding_boxes(
    aligned_volume: str, table_name: str, segmentation_source: str
) -> dict:
    """Get the bounding boxes of the annotation table.

    Args:
        aligned_volume (str): Name of the aligned volume to use as the database
        table_name (str): Name of the annotation table
        segmentation_source (str): Name of the PCG table

    Returns:
        dict: Dictionary of bounding boxes for each column in the annotation table
    """
    db = dynamic_annotation_cache.get_db(aligned_volume)
    schema = db.database.get_table_schema(table_name)
    mat_metadata = {
        "annotation_table_name": table_name,
        "schema": schema,
        "pcg_table_name": segmentation_source,
    }
    AnnotationModel = create_annotation_model(mat_metadata)
    SegmentationModel = create_segmentation_model(mat_metadata)

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
                        [
                            list(map(lambda x: int(round(float(x))), coord.split()))
                            for coord in coords
                        ]
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
    cv_info: List,
    mat_info: dict,
    chunk_scale_factor: int = 1,
) -> tuple:
    """Generate chunks of a given size that cover a given bounding box.

    Args:
        min_coord (np.array): Min corner of the bounding box
        max_coord (np.array): Max corner of the bounding box
        cv_info (List): cloud volume info
        chunk_scale_factor (int, optional): Scale factor for chunk size. Defaults to 1.

    Yields:
        Iterator[tuple]: chunk min corner, chunk max corner, chunk index, total number of chunks
    """
    # Determine the number of chunks in each dimension
    anno_resolution = mat_info["coord_resolution"]
    cv_chunk_size = cv_info["chunk_size"]
    cv_resolution = cv_info["resolution"]
    scale_factor = np.array(cv_resolution) / np.array(anno_resolution)
    scaled_chunks = cv_chunk_size * scale_factor

    chunk_size = scaled_chunks * chunk_scale_factor
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
    if np.any(min_corner == np.inf) or np.any(max_corner == -np.inf):
        return None, []  # No input bounding boxes were within the global bounding box

    aligned_bbox = np.array([min_corner, max_corner], dtype=int)
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
    try:
        chunk = load_chunk(cv, chunk_start, chunk_end)
    except IndexError:
        return points, np.zeros(len(points), dtype=np.uint64)

    return points, chunk[indices[:, 0], indices[:, 1], indices[:, 2]]


def process_chunks(chunk_map_dict, cv, coord_resolution):
    for k, v in chunk_map_dict.items():
        yield get_svids_in_chunk(
            cv, chunk_map_key=k, chunk_data=v, coord_resolution=coord_resolution
        )


# Create a group for the tasks
def chunk_tasks(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    iterable = iter(iterable)
    while True:
        chunk = list(islice(iterable, n))
        if not chunk:
            return
        yield chunk


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
    mat_info: dict,
):
    """Inserts the segmentation data into the database.

    Args:
        data (pd.DataFrame): Dataframe containing the segmentation data
        mat_info (dict): Materialization info for a given table

    Returns:
        bool: True if the data is inserted, False otherwise
    """

    start_time = time.time()
    aligned_volume = mat_info["aligned_volume"]
    table_name = mat_info["annotation_table_name"]
    # pcg_table_name = mat_info["pcg_table_name"]
    db = dynamic_annotation_cache.get_db(aligned_volume)
    schema = db.database.get_table_schema(table_name)
    engine = sqlalchemy_cache.get_engine(aligned_volume)
    mat_info["schema"] = schema
    SegmentationModel = create_segmentation_model(mat_info)
    seg_columns = SegmentationModel.__table__.columns.keys()
    segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
    data_df = pd.DataFrame(data, dtype=object)
    supervoxel_id_cols = [
        col for col in data_df.columns if col.endswith("_supervoxel_id")
    ]

    for col in supervoxel_id_cols:
        data_df[col] = data_df[col].apply(convert_array_to_int)

    # find the common columns between the two dataframes
    common_cols = segmentation_dataframe.columns.intersection(data_df.columns)

    # merge the dataframes and fill the missing values with 0, data might get updated in the next chunk lookup
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
    celery_logger.info(f"Insertion time: {time.time() - start_time} seconds")
    return True


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
