import os
from dynamicannotationdb.schema import DynamicSchemaClient
from geoalchemy2.shape import to_shape
from flask import current_app, abort, g
from middle_auth_client.decorators import users_share_common_group
from celery.utils.log import get_task_logger
from typing import Any
from cachetools import TTLCache, cached, LRUCache
celery_logger = get_task_logger(__name__)


def get_app_base_path():
    return os.path.dirname(os.path.realpath(__file__))


def get_instance_folder_path():
    return os.path.join(get_app_base_path(), "instance")


def make_root_id_column_name(column_name: str):
    pos = column_name.split("_")[0]
    pos_type = column_name.split("_")[1]
    return f"{pos}_{pos_type}_root_id"


def build_materialized_table_id(aligned_volume: str, table_name: str) -> str:
    return f"mat__{aligned_volume}__{table_name}"


def get_query_columns_by_suffix(AnnotationModel, SegmentationModel, suffix):
    seg_columns = [column.name for column in SegmentationModel.__table__.columns]
    anno_columns = [column.name for column in AnnotationModel.__table__.columns]

    matched_columns = set()
    for column in seg_columns:
        prefix = column.split("_")[0]
        for anno_col in anno_columns:
            if anno_col.startswith(prefix):
                matched_columns.add(anno_col)
    matched_columns.remove("id")

    supervoxel_columns = [
        f"{col.rsplit('_', 1)[0]}_{suffix}"
        for col in matched_columns
        if col != "annotation_id"
    ]
    # # create model columns for querying
    anno_model_cols = [getattr(AnnotationModel, name) for name in matched_columns]
    anno_model_cols.append(AnnotationModel.id)
    seg_model_cols = [getattr(SegmentationModel, name) for name in supervoxel_columns]

    # add id columns to lookup
    seg_model_cols.extend([SegmentationModel.id])
    return anno_model_cols, seg_model_cols, supervoxel_columns


def get_geom_from_wkb(wkb):
    wkb_element = to_shape(wkb)
    if wkb_element.has_z:
        return [
            int(wkb_element.xy[0][0]),
            int(wkb_element.xy[1][0]),
            int(wkb_element.z),
        ]


def create_segmentation_model(mat_metadata, reset_cache: bool = False):
    annotation_table_name = mat_metadata.get("annotation_table_name")
    schema_type = mat_metadata.get("schema")
    table_metadata = {"reference_table": mat_metadata.get("reference_table")}
    pcg_table_name = mat_metadata.get("pcg_table_name")
    schema_client = DynamicSchemaClient()

    SegmentationModel = schema_client.create_segmentation_model(
        table_name=annotation_table_name,
        schema_type=schema_type,
        segmentation_source=pcg_table_name,
        table_metadata=table_metadata,
        reset_cache=reset_cache,
    )
    celery_logger.debug(
        f"SEGMENTATION----------------------- {SegmentationModel.__table__.columns}"
    )
    return SegmentationModel


def create_annotation_model(
    mat_metadata, with_crud_columns: bool = True, reset_cache: bool = False
):
    annotation_table_name = mat_metadata.get("annotation_table_name")
    schema_type = mat_metadata.get("schema")
    table_metadata = {"reference_table": mat_metadata.get("reference_table")}
    schema_client = DynamicSchemaClient()

    AnnotationModel = schema_client.create_annotation_model(
        table_name=annotation_table_name,
        schema_type=schema_type,
        table_metadata=table_metadata,
        with_crud_columns=with_crud_columns,
        reset_cache=reset_cache,
    )

    celery_logger.debug(
        f"ANNOTATION----------------------- {AnnotationModel.__table__.columns}"
    )
    return AnnotationModel


def get_config_param(config_param: str, default: Any = None):
    try:
        return current_app.config[config_param]
    except (KeyError, LookupError, RuntimeError):
        return os.environ.get(config_param, default)

@cached(cache=TTLCache(maxsize=100, ttl=600))
def check_write_permission(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["user_id"] != str(g.auth_user["id"]):
        if metadata["write_permission"] == "GROUP":
            if not users_share_common_group(metadata["user_id"]):
                abort(
                    401,
                    "Unauthorized: You cannot write because you do not share a common group with the creator of this table.",
                )
            else:
                abort(401, "Unauthorized: The table can only be written to by owner")
    return metadata

@cached(cache=TTLCache(maxsize=100, ttl=600))
def check_read_permission(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["read_permission"] == "GROUP":
        if not users_share_common_group(metadata["user_id"]):
            abort(
                401,
                "Unauthorized: You cannot read this table because you do not share a common group with the creator of this table.",
            )
    elif metadata["read_permission"] == "PRIVATE":
        if metadata["user_id"] != str(g.auth_user["id"]):
            abort(401, "Unauthorized: The table can only be read by its owner")
    return metadata


def check_ownership(db, table_name):
    metadata = db.database.get_table_metadata(table_name)
    if metadata["user_id"] != str(g.auth_user["id"]):
        abort(401, "You cannot do this because you are not the owner of this table")
    return metadata
