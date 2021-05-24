import os
from emannotationschemas import models as em_models
from geoalchemy2.shape import to_shape
from flask import current_app


def get_app_base_path():
    return os.path.dirname(os.path.realpath(__file__))


def get_instance_folder_path():
    return os.path.join(get_app_base_path(), 'instance')


def make_root_id_column_name(column_name: str):
    pos = column_name.split("_")[0]
    pos_type = column_name.split("_")[1]
    return f"{pos}_{pos_type}_root_id"


def build_materialized_table_id(aligned_volume: str, table_name: str) -> str:
    return f"mat__{aligned_volume}__{table_name}"


def get_query_columns_by_suffix(AnnotationModel, SegmentationModel, suffix):
    seg_columns = [
        column.name for column in SegmentationModel.__table__.columns]
    anno_columns = [
        column.name for column in AnnotationModel.__table__.columns]

    matched_columns = set()
    for column in seg_columns:
        prefix = (column.split("_")[0])
        for anno_col in anno_columns:
            if anno_col.startswith(prefix):
                matched_columns.add(anno_col)
    matched_columns.remove('id')

    supervoxel_columns = [
        f"{col.rsplit('_', 1)[0]}_{suffix}" for col in matched_columns if col != 'annotation_id']
    # # create model columns for querying
    anno_model_cols = [getattr(AnnotationModel, name)
                       for name in matched_columns]
    anno_model_cols.append(AnnotationModel.id)
    seg_model_cols = [getattr(SegmentationModel, name)
                      for name in supervoxel_columns]

    # add id columns to lookup
    seg_model_cols.extend([SegmentationModel.id])
    return anno_model_cols, seg_model_cols, supervoxel_columns


def get_geom_from_wkb(wkb):
    wkb_element = to_shape(wkb)
    if wkb_element.has_z:
        return [int(wkb_element.xy[0][0]), int(wkb_element.xy[1][0]), int(wkb_element.z)]


def create_segmentation_model(mat_metadata):
    annotation_table_name = mat_metadata.get('annotation_table_name')
    schema_type = mat_metadata.get("schema")
    pcg_table_name = mat_metadata.get("pcg_table_name")

    SegmentationModel = em_models.make_segmentation_model(
        annotation_table_name, schema_type, pcg_table_name)
    return SegmentationModel


def create_annotation_model(mat_metadata, with_crud_columns: bool = True):
    annotation_table_name = mat_metadata.get('annotation_table_name')
    schema_type = mat_metadata.get("schema")

    AnnotationModel = em_models.make_annotation_model(
        annotation_table_name, schema_type, with_crud_columns)
    return AnnotationModel


def get_config_param(config_param: str):
    try:
        return current_app.config[config_param]
    except Exception:
        return os.environ[config_param]
