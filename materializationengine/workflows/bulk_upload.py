import datetime
import json
import time
from typing import List

import gcsfs
import numpy as np
import pandas as pd
from celery import chain, chord, group
from celery.utils.log import get_task_logger
from dynamicannotationdb.models import AnnoMetadata, SegmentationMetadata
from dynamicannotationdb.schema import DynamicSchemaClient

from materializationengine.celery_init import celery
from materializationengine.database import db_manager
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import add_index, fin
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
)

celery_logger = get_task_logger(__name__)


@celery.task(name="workflow:gcs_bulk_upload_workflow", bind=True, acks_late=True)
def gcs_bulk_upload_workflow(self, bulk_upload_params: dict):
    """Bulk insert of npy file from a google cloud storage
    bucket.

    Args:
        bulk_upload_params (dict): column mapping info and
        metadata for uploading data.
    """
    upload_creation_time = datetime.datetime.utcnow()

    bulk_upload_info = get_gcs_file_info(upload_creation_time, bulk_upload_params)
    bulk_upload_chunks = create_chunks(bulk_upload_info[0])

    bulk_upload_workflow = chain(
        create_tables.si(bulk_upload_info[0]),
        chord(
            [
                group(bulk_upload_task.si(bulk_upload_info, chunk))
                for chunk in bulk_upload_chunks
            ],
            fin.si(),
        ),  # return here is required for chords
        add_table_indices.si(bulk_upload_info[0]),
        find_missing_chunks_by_ids.s(bulk_upload_info),
    )

    bulk_upload_workflow.apply_async()


@celery.task(name="workflow:gcs_insert_missing_data_workflow", bind=True, acks_late=True)
def gcs_insert_missing_data(self, bulk_upload_params: dict):

    bulk_upload_chunks = bulk_upload_params["chunks"]
    upload_creation_time = datetime.datetime.utcnow()

    bulk_upload_info = get_gcs_file_info(upload_creation_time, bulk_upload_params)

    bulk_upload_workflow = group(
        bulk_upload_task.si(bulk_upload_info, chunk) for chunk in bulk_upload_chunks
    )
    bulk_upload_workflow.apply_async()


def get_gcs_file_info(
    upload_creation_time: datetime.datetime.utcnow, bulk_upload_params: dict
) -> dict:
    project_path = bulk_upload_params["project"]
    file_path = bulk_upload_params["file_path"]
    column_mapping = bulk_upload_params["column_mapping"]
    annotation_table_name = bulk_upload_params["annotation_table_name"]
    segmentation_source = bulk_upload_params["segmentation_source"]

    # convert unix epoch time to datetime object
    materialized_ts = bulk_upload_params.get("materialized_ts", None)
    if materialized_ts:
        last_updated_ts = datetime.datetime.utcfromtimestamp(materialized_ts).strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )
    else:
        last_updated_ts = None

    fs = gcsfs.GCSFileSystem(project=project_path)
    files = fs.ls(f"{project_path}/{file_path}")
    bulk_upload_info = []
    try:
        for file in files:
            if file.endswith(".npy"):
                mapped_file_name = file.split("/")[-1].split(".")[0]
                if mapped_file_name in column_mapping:
                    with fs.open(file, "rb") as fhandle:
                        major, minor = np.lib.format.read_magic(fhandle)
                        shape, fortran, dtype = np.lib.format.read_array_header_1_0(
                            fhandle
                        )
                        file_info = {
                            "filename": file,
                            "project": bulk_upload_params["project"],
                            "file_path": bulk_upload_params["file_path"],
                            "schema": bulk_upload_params["schema"],
                            "description": bulk_upload_params["description"],
                            "annotation_table_name": annotation_table_name,
                            "seg_table_name": f"{annotation_table_name}__{segmentation_source}",
                            "aligned_volume": bulk_upload_params["aligned_volume"][
                                "name"
                            ],
                            "pcg_table_name": segmentation_source,
                            "upload_creation_time": upload_creation_time,
                            "num_rows": int(shape[0]),
                            "data_type": mapped_file_name,
                            "fortran": fortran,
                            "column_mapping": column_mapping,
                            "last_updated": last_updated_ts,
                        }

                    bulk_upload_info.append(file_info.copy())
    except Exception as e:
        raise e

    return bulk_upload_info


def create_chunks(bulk_upload_info: dict) -> List:
    num_rows = bulk_upload_info["num_rows"]
    chunk_size = bulk_upload_info.get("chunk_size", 100_000)
    chunks = []
    if chunk_size <= 1:
        raise ValueError(f"Chunk size of {chunk_size}, must be larger than 1.")
    for chunk_start in range(0, num_rows, chunk_size):
        chunk_end = chunk_start + chunk_size
        if chunk_end > num_rows:
            chunk_end = num_rows
        chunks.append([chunk_start, chunk_end - chunk_start])
    return chunks


@celery.task(name="workflow:create_tables", acks_late=True, bind=True)
def create_tables(self, bulk_upload_params: dict):
    table_name = bulk_upload_params["annotation_table_name"]
    aligned_volume = bulk_upload_params["aligned_volume"]
    pcg_table_name = bulk_upload_params["pcg_table_name"]
    last_updated = bulk_upload_params["last_updated"]
    seg_table_name = bulk_upload_params["seg_table_name"]
    upload_creation_time = bulk_upload_params["upload_creation_time"]
    

    engine = db_manager.get_engine(aligned_volume)

    with db_manager.session_scope(aligned_volume) as session:
        if not session.query(AnnoMetadata).filter(AnnoMetadata.table_name == table_name).scalar():
            AnnotationModel = create_annotation_model(bulk_upload_params)
            AnnotationModel.__table__.create(bind=engine, checkfirst=True)
            anno_metadata_dict = {
                "table_name": table_name,
                "schema_type": bulk_upload_params.get("schema"),
                "valid": True,
                "created": upload_creation_time,
                "user_id": bulk_upload_params.get("user_id"),
                "description": bulk_upload_params["description"],
                "reference_table": bulk_upload_params.get("reference_table"),
                "flat_segmentation_source": bulk_upload_params.get("flat_segmentation_source"),
            }
            anno_metadata = AnnoMetadata(**anno_metadata_dict)
            session.add(anno_metadata)

        session.add(anno_metadata)

        if (
            not session.query(SegmentationMetadata)
            .filter(SegmentationMetadata.table_name == table_name)
            .scalar()
        ):
            SegmentationModel = create_segmentation_model(bulk_upload_params)
            SegmentationModel.__table__.create(bind=engine, checkfirst=True)
            seg_metadata_dict = {
                "annotation_table": table_name,
                "schema_type": bulk_upload_params.get("schema"),
                "table_name": seg_table_name,
                "valid": True,
                "created": upload_creation_time,
                "pcg_table_name": pcg_table_name,
                "last_updated": last_updated,
            }

            seg_metadata = SegmentationMetadata(**seg_metadata_dict)


            session.flush()
            session.add(seg_metadata)


        drop_seg_indexes = index_cache.drop_table_indices(
            SegmentationModel.__table__.name, engine
        )
        time.sleep(10)
        drop_anno_indexes = index_cache.drop_table_indices(
            AnnotationModel.__table__.name, engine
        )
        celery_logger.info(
            f"Table {AnnotationModel.__table__.name} indices have been dropped {drop_anno_indexes}."
        )
        celery_logger.info(
            f"Table {SegmentationModel.__table__.name} indices have been dropped {drop_seg_indexes}."
        )

    return f"Tables {table_name}, {seg_table_name} created."


@celery.task(name="workflow:bulk_upload_task", bind=True, acks_late=True, max_retries=3)
def bulk_upload_task(self, bulk_upload_info: dict, chunk: List):
    try:
        file_data = []
        for file_metadata in bulk_upload_info:
            celery_logger.info(file_metadata)

            data = gcs_read_npy_chunk(file_metadata, chunk)
            parsed_data = parse_data(data, file_metadata)
            file_data.append(parsed_data)

        formatted_data = format_data(file_data, file_metadata)
        return self.replace(upload_data.s(formatted_data, file_metadata))
    except Exception as e:
        celery_logger.error(e)
        raise self.retry(exc=Exception, countdown=3)


def gcs_read_npy_chunk(bulk_upload_info: dict, chunk: List):
    filename = bulk_upload_info["filename"]
    project = bulk_upload_info["project"]
    start_row = chunk[0]
    num_rows = chunk[1]
    if start_row < 0 or num_rows <= 0:
        raise ValueError()

    fs = gcsfs.GCSFileSystem(project=project)
    with fs.open(filename, "rb") as fhandle:
        major, minor = np.lib.format.read_magic(fhandle)
        shape, fortran, dtype = np.lib.format.read_array_header_1_0(fhandle)
        offset = fhandle.tell()

        try:
            col_shape = shape[1]
        except IndexError:
            col_shape = 1

        if start_row > shape[0]:
            raise ValueError()
        if start_row + num_rows > shape[0]:
            raise ValueError()

        total_size = np.prod(shape[:])
        row_size = int(np.prod(shape[1:]))

        length = num_rows * dtype.itemsize
        start_byte = start_row * dtype.itemsize
        index_row_byte = (total_size // row_size) * dtype.itemsize
        if fortran:
            data_bytes = [
                (start_byte + (index_row_byte * i)) for i in range(0, col_shape)
            ]
        else:
            fhandle.seek(start_byte)
            data_bytes = [start_byte]

        array = np.zeros([num_rows, row_size], dtype=dtype)

        for i, index in enumerate(data_bytes):
            data = fs.read_block(filename, offset=index + offset, length=length)
            if col_shape == 1:
                array = np.frombuffer(data, dtype=dtype)
            else:
                array[:, i] = np.frombuffer(data, dtype=dtype)
    return array.tolist()


def parse_data(data: List, bulk_upload_info: dict):
    data_type = bulk_upload_info["data_type"]
    column_mapping = bulk_upload_info["column_mapping"]
    celery_logger.info(f"data_type: {data_type} |  column_mapping:{column_mapping} ")
    data_columns = column_mapping[data_type]

    if not isinstance(data_columns, list):
        data = {data_columns: data}
        formatted_data = pd.DataFrame(data)
    else:
        formatted_data = pd.DataFrame(data, columns=data_columns)
    return formatted_data.to_dict("records")


def format_data(data: List, bulk_upload_info: dict):
    schema = bulk_upload_info["schema"]
    upload_creation_time = bulk_upload_info["upload_creation_time"]
    base_df = pd.DataFrame(data[0])
    for data in data[1:]:
        temp_df = pd.DataFrame(data)
        base_df = pd.concat([base_df, temp_df], axis=1)

    records = base_df.to_dict("records")
    schema_client = DynamicSchemaClient()
    schema = schema_client.get_schema(schema)
    FlattendSchema = schema_client.get_flattened_schema(schema)

    (
        flat_annotation_schema,
        flat_segmentation_schema,
    ) = schema_client._split_flattened_schema(FlattendSchema)
    anno_data = split_annotation_data(
        records, flat_annotation_schema, upload_creation_time
    )
    seg_data = split_annotation_data(
        records, flat_segmentation_schema, upload_creation_time
    )
    return [anno_data, seg_data]


def split_annotation_data(serialized_data, schema, upload_creation_time):
    split_data = []
    for data in serialized_data:
        matched_data = {}
        for key, value in schema._declared_fields.items():
            if key in data:
                if "position" in key:
                    matched_data[
                        key
                    ] = f"POINTZ({data[key][0]} {data[key][1]} {data[key][2]})"
                    matched_data.update(
                        {"valid": True, "created": str(upload_creation_time)}
                    )
                else:
                    matched_data[key] = data[key]

        matched_data.update(
            {
                "id": data["id"],
            }
        )
        split_data.append(matched_data)
    return split_data


@celery.task(name="workflow:add_table_indices", acks_late=True, max_retries=3, bind=True)
def upload_data(self, data: List, bulk_upload_info: dict):

    aligned_volume = bulk_upload_info["aligned_volume"]

    model_data = {
        "annotation_table_name": bulk_upload_info["annotation_table_name"],
        "schema": bulk_upload_info["schema"],
        "pcg_table_name": bulk_upload_info["pcg_table_name"],
    }

    AnnotationModel = create_annotation_model(model_data)
    SegmentationModel = create_segmentation_model(model_data)

    engine = db_manager.get_engine(aligned_volume)

    try:
        with engine.begin() as connection:
            connection.execute(AnnotationModel.__table__.insert(), data[0])
            connection.execute(SegmentationModel.__table__.insert(), data[1])
    except Exception as e:
        celery_logger.error(f"ERROR: {e}")
        raise self.retry(exc=Exception, countdown=3)
    finally:
        engine.dispose()
    return True


@celery.task(name="workflow:add_table_indices", bind=True, acks_late=True)
def add_table_indices(self, bulk_upload_info: dict):
    aligned_volume = bulk_upload_info["aligned_volume"]
    annotation_table_name = bulk_upload_info["annotation_table_name"]
    seg_table_name = bulk_upload_info["seg_table_name"]
    segmentation_source = bulk_upload_info["pcg_table_name"]
    schema = bulk_upload_info["schema"]

    engine = db_manager.get_engine(aligned_volume)
    schema_client = DynamicSchemaClient()
    anno_model = schema_client.create_annotation_model(annotation_table_name, schema)
    seg_model = schema_client.create_segmentation_model(
        annotation_table_name, schema, segmentation_source
    )

    # add annotation indexes
    anno_indices = index_cache.add_indices_sql_commands(
        table_name=annotation_table_name, model=anno_model, engine=engine
    )

    # add segmentation table indexes
    seg_indices = index_cache.add_indices_sql_commands(
        table_name=seg_table_name, model=seg_model, engine=engine
    )
    add_index_tasks = []
    add_anno_table_index_tasks = [
        add_index.si(aligned_volume, command) for command in anno_indices
    ]
    add_index_tasks.append(add_anno_table_index_tasks)

    add_seg_table_index_tasks = [
        add_index.si(aligned_volume, command) for command in seg_indices
    ]
    add_index_tasks.append(add_seg_table_index_tasks)

    return self.replace(chain(add_index_tasks))


@celery.task(name="workflow:find_missing_chunks_by_ids", bind=True, acks_late=True)
def find_missing_chunks_by_ids(self, bulk_upload_info: dict, chunk_size: int = 100_000):
    """Find missing chunks that failed to insert during bulk uploading.
    It will compare the .npy files in the bucket to the database. If
    missing chunks of data are found this method will return a celery
    workflow to attempt to re-insert the data.

    Args:
        bulk_upload_info (dict): bulk upload metadata
        chunk_size (int, optional): size of chunk to query. Defaults to 100_000.

    Returns:
        celery workflow or message
    """
    filename = bulk_upload_info["filename"]
    file_path = bulk_upload_info["file_path"]
    table_name = bulk_upload_info["annotation_table_name"]

    project = bulk_upload_info["project"]
    aligned_volume = bulk_upload_info["aligned_volume"]

    engine = db_manager.get_engine(aligned_volume)

    fs = gcsfs.GCSFileSystem(project=project)
    with fs.open(filename, "rb") as fhandle:
        ids = np.load(file_path)
        start_ids = ids[::chunk_size]
        valstr = ",".join([str(s) for s in start_ids])

        found_ids = pd.read_sql(
            f"select id from {table_name} where id in ({valstr})", engine
        )
        chunk_ids = np.where(~np.isin(start_ids, found_ids.id.values))[0]

        chunks = chunk_ids * chunk_size
        c_list = chunks.tolist()

        data = [[itm, chunk_size] for itm in c_list]
        lost_chunks = json.dumps(data)

    if lost_chunks:
        celery_logger.warning(
            f"Some chunks of data failed to be inserted {lost_chunks}"
        )
        bulk_upload_info.update({"chunks": lost_chunks})
        celery_logger.info("Will attempt to re-insert missing data...")
        return self.replace(gcs_insert_missing_data.s(bulk_upload_info))
    return "No missing chunks found"
