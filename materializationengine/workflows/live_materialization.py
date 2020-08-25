import datetime
import logging
import numpy as np
from flask import current_app
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.sql import or_
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError
from celery.utils.log import get_task_logger

import cloudvolume
from celery import group, chain, chord, subtask, chunks, Task
from dynamicannotationdb.key_utils import (
    build_table_id,
    build_segmentation_table_id,
    get_table_name_from_table_id,
)
from dynamicannotationdb.models import SegmentationMetadata
from geoalchemy2.shape import to_shape
from emannotationschemas import models as em_models
from materializationengine.celery_worker import celery
from materializationengine.database import get_db
from materializationengine.extensions import create_session
from materializationengine.errors import AnnotationParseFailure, TaskFailure, WrongModelType
from materializationengine.chunkedgraph_gateway import ChunkedGraphGateway
from materializationengine.utils import make_root_id_column_name
from typing import List
from copy import deepcopy
import pandas as pd
from functools import lru_cache
celery_logger = get_task_logger(__name__)

BIGTABLE = current_app.config["BIGTABLE_CONFIG"]
PROJECT_ID = BIGTABLE["project_id"]
CG_INSTANCE_ID = BIGTABLE["instance_id"]
AMDB_INSTANCE_ID = BIGTABLE["amdb_instance_id"]
CHUNKGRAPH_TABLE_ID = current_app.config["CHUNKGRAPH_TABLE_ID"]
INFOSERVICE_ENDPOINT = current_app.config["INFOSERVICE_ENDPOINT"]
ANNO_ADDRESS = current_app.config["ANNO_ENDPOINT"]
SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]


def start_materialization(aligned_volume_name: str, pcg_table_name: str, aligned_volume_info: dict):
    """Base live materialization

    Workflow paths:
        check if supervoxel column is empty:
            if last_updated is NULL:
                -> workflow : find missing supervoxels > cloudvolume lookup supervoxels > get root ids > 
                            find missing root_ids > lookup supervoxel ids from sql > get root_ids > merge root_ids list > insert root_ids
            else:
                -> find missing supervoxels > cloudvolume lookup |
                    - > find new root_ids between time stamps  ---> merge root_ids list > upsert root_ids

    Parameters
    ----------
    aligned_volume_name : str
        [description]
    aligned_volume_info : dict
        [description]
    """

    result = get_materialization_info.s(aligned_volume_name, pcg_table_name, aligned_volume_info).delay()
    mat_info = result.get()
    for mat_metadata in mat_info:
        result = chunk_supervoxel_ids_task.s(mat_metadata).delay()
        supervoxel_chunks = result.get()

        process_chunks_workflow = chain(
            create_missing_segmentation_tables.s(mat_metadata),
            chord([
                chain(
                    get_annotations_with_missing_supervoxel_ids.s(chunk),
                    get_cloudvolume_supervoxel_ids.s(mat_metadata),
                    get_root_ids.s(mat_metadata),
                    ) for chunk in supervoxel_chunks],
                    fin.si()), # return here is required for chords
                    fin.si() # final task which will process a return status/timing etc...
                )

        process_chunks_workflow.apply_async()

class SqlAlchemyCache:

    def __init__(self):
        self._engine = None
        self._sessions = {}

    @property
    def engine(self):
        return self._engine

    def get(self, aligned_volume):
        if aligned_volume not in self._sessions:
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
            self._engine = create_engine(sql_uri, pool_recycle=3600,
                                                  pool_size=20,
                                                  max_overflow=50)
            Session = scoped_session(sessionmaker(bind=self._engine))
            self._sessions[aligned_volume] = Session
        return self._sessions[aligned_volume]

sqlalchemy_cache = SqlAlchemyCache()

@celery.task(name="process:get_materialization_info", bind=True)
def get_materialization_info(self, aligned_volume: str,
                                   pcg_table_name: str,
                                   aligned_volume_info: dict) -> List[dict]:
    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization.

    Parameters
    ----------
    aligned_volume : str
        name of aligned volume
    pcg_table_name: str
        cg_table_name
    aligned_volume_info:
        infoservice data
    Returns
    -------
    List[dict]
        list of dicts containing metadata for each table
    """
    db = get_db(aligned_volume)
    annotation_table_ids = db._get_existing_table_ids()
    metadata = []
    for annotation_table_id in annotation_table_ids:
        table_name = annotation_table_id.split("__")[-1]
        segmentation_table_id = f"{annotation_table_id}__{pcg_table_name}"
        segmentation_metadata = db.get_segmentation_table_metadata(aligned_volume, table_name, pcg_table_name)

        table_metadata = {
            'aligned_volume': str(aligned_volume),
            'schema': db.get_table_schema(aligned_volume, table_name),
            'max_id': int(db.get_max_id_value(annotation_table_id)),
            'segmentation_table_id': segmentation_table_id,
            'annotation_table_id': annotation_table_id,
            'pcg_table_name': pcg_table_name,
            'table_name': table_name,
            'segmentation_source': aligned_volume_info.get('segmentation_source'),
            'last_updated_time_stamp': segmentation_metadata.get('last_updated')
        }
        metadata.append(table_metadata.copy())
    return metadata


@celery.task(name='process:create_missing_segmentation_tables',
             bind=True)
def create_missing_segmentation_tables(self, mat_metadata: dict) -> dict:
    """Create missing segmentation tables associated with an annotation table if it 
    does not already exist.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns:
        dict: Materialization metadata
    """
    segmentation_table_id = mat_metadata.get('segmentation_table_id')
    aligned_volume = mat_metadata.get('aligned_volume')

    SegmentationModel = create_segmentation_model(mat_metadata)
 
    session = sqlalchemy_cache.get(aligned_volume)
    engine = sqlalchemy_cache.engine
    
    if not session.query(SegmentationMetadata).filter(SegmentationMetadata.table_id==segmentation_table_id).scalar():
        SegmentationModel.create(bind=engine, checkfirst=True)
        creation_time = datetime.datetime.utcnow()
        metadata_dict = {
            'annotation_table': mat_metadata.get('annotation_table_id'),
            'schema_type': mat_metadata.get('schema'),
            'table_id': segmentation_table_id,
            'valid': True,
            'created': creation_time,
            'pcg_table_name': mat_metadata.get('pcg_table_name')
        }

        seg_metadata = SegmentationMetadata(**metadata_dict)
        try:
            session.add(seg_metadata)
            session.commit()
        except Exception as e:
            celery_logger.error(f"SQL ERROR: {e}")
            session.rollback()
    else:
        session.close()
    return mat_metadata


@celery.task(name="process:chunk_supervoxel_ids_task", bind=True)
def chunk_supervoxel_ids_task(self, mat_metadata: dict, block_size: int = 10000) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialziation.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata
    block_size : int, optional
        [description], by default 2500

    Returns
    -------
    List[List]
        list of list containg start and end indices
    """
    max_id = mat_metadata["max_id"]
    n_parts = int(max(1, max_id / block_size))
    n_parts += 1
    id_chunks = np.linspace(1, max_id, num=n_parts, dtype=np.int64).tolist()
    chunk_ends = []
    for i_id_chunk in range(len(id_chunks) - 1):
        chunk_ends.append([id_chunks[i_id_chunk], id_chunks[i_id_chunk + 1]])
    return chunk_ends


@celery.task(name="process:get_annotations_with_missing_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_annotations_with_missing_supervoxel_ids(self, mat_metadata: dict,
                                                      chunk: List[int]) -> dict:
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
    
    aligned_volume = mat_metadata.get("aligned_volume")
    SegmentationModel = create_segmentation_model(mat_metadata)
    AnnotationModel = create_annotation_model(mat_metadata)
    
    session = sqlalchemy_cache.get(aligned_volume)
    anno_model_cols, seg_model_cols, supervoxel_columns = get_query_columns_by_suffix(
        AnnotationModel, SegmentationModel, 'supervoxel_id')
    try:
        annotation_data = [
            anno_id for anno_id in session.query(*anno_model_cols).\
            filter(AnnotationModel.id.between(chunk[0], chunk[1])).filter(AnnotationModel.valid==True)
        ]
        anno_ids = [x[-1] for x in annotation_data]
        supervoxel_data = [data for data in session.query(*seg_model_cols).\
            filter(or_(SegmentationModel.annotation_id.in_(anno_ids)))]  
        session.close()
    except SQLAlchemyError as e:
        session.rollback()
        raise self.retry(exc=e, countdown=3)

    if not anno_ids:
        self.request.callbacks = None

    annotation_dataframe = pd.DataFrame(annotation_data, dtype=object)

    wkb_data = annotation_dataframe.loc[:, annotation_dataframe.columns.str.endswith("position")]


    annotation_dict = {}
    for column, wkb_points in wkb_data.items():
        annotation_dict[column] = [get_geom_from_wkb(wkb_point) for wkb_point in wkb_points]
    for key, value in annotation_dict.items():
        annotation_dataframe.loc[:, key] = value

    if supervoxel_data:
        segmatation_col_list = ['segmentation_id' if col == "id" else col for col in supervoxel_data[0].keys()]
        segmentation_dataframe = pd.DataFrame(supervoxel_data, columns=segmatation_col_list, dtype=object).fillna(value=np.nan)
        merged_dataframe = pd.merge(segmentation_dataframe, annotation_dataframe, how='outer', left_on='annotation_id', right_on='id')
    else:
        supervoxel_columns.extend(['annotation_id', 'segmentation_id'])
        segmentation_dataframe = pd.DataFrame(columns=supervoxel_columns, dtype=object)
        segmentation_dataframe = segmentation_dataframe.fillna(value=np.nan)
        merged_dataframe = pd.concat((segmentation_dataframe, annotation_dataframe), axis=1)

    return merged_dataframe.to_dict(orient='list')

@celery.task(name="process:get_cloudvolume_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_cloudvolume_supervoxel_ids(self, materialization_data: dict, mat_metadata: dict) -> dict:
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
    cv = cloudvolume.CloudVolume(segmentation_source, mip=0)

    position_data = mat_df.loc[:, mat_df.columns.str.endswith("position")]
    for k, row in mat_df.iterrows():
        for col, data in position_data.items():
            suprvoxel_column = f"{col.rsplit('_', 1)[0]}_supervoxel_id"
            if np.isnan(mat_df.loc[k, suprvoxel_column]):
                svid = np.squeeze(cv.download_point(data, size=1))
                mat_df.loc[k, 'annotation_id'] = row.id
                mat_df.loc[k, suprvoxel_column] = svid
    mat_df.loc[:, mat_df.columns.str.endswith("supervoxel_id")] = mat_df.loc[:, mat_df.columns.str.endswith("supervoxel_id")].astype('uint64')
    return mat_df.to_dict(orient='list')


@celery.task(name="process:get_sql_supervoxel_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_sql_supervoxel_ids(self, chunks: List[int], mat_metadata: dict) -> List[int]:
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
    session = sqlalchemy_cache.get(aligned_volume)
    try:
        columns = [column.name for column in SegmentationModel.__table__.columns]
        supervoxel_id_columns = [column for column in columns if "supervoxel_id" in column]
        supervoxel_id_data = {}
        for supervoxel_id_column in supervoxel_id_columns:
            supervoxel_id_data[supervoxel_id_column] = [
                data
                for data in session.query(
                    SegmentationModel.id, getattr(SegmentationModel, supervoxel_id_column)
                ).filter(
                    or_(SegmentationModel.annotation_id).between(int(chunks[0]), int(chunks[1]))
                )
            ]
        session.close()
        return supervoxel_id_data
    except Exception as e:
        raise self.retry(exc=e, countdown=3)


@celery.task(name="process:get_root_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_root_ids(self, materialization_data: dict, mat_metadata: dict) -> dict:
    pcg_table_name = mat_metadata.get("pcg_table_name")
    aligned_volume = mat_metadata.get("aligned_volume")
    last_updated_time_stamp = mat_metadata.get("last_updated_time_stamp")
    
    mat_df = pd.DataFrame(materialization_data, dtype=object)
    
    AnnotationModel = create_annotation_model(mat_metadata)
    SegmentationModel = create_segmentation_model(mat_metadata)
    
    session = sqlalchemy_cache.get(aligned_volume)

    __, seg_model_cols, __ = get_query_columns_by_suffix(AnnotationModel, SegmentationModel, 'root_id')

    anno_ids = mat_df['annotation_id'].to_list()

    # get current root ids from database
    try:
        current_root_ids =  [data for data in session.query(*seg_model_cols).\
                filter(or_(SegmentationModel.annotation_id.in_(anno_ids)))]   
        session.close()
    except Exception as e:
        raise self.retry(exc=e, countdown=3)

    root_ids_df = pd.DataFrame(current_root_ids, dtype=object)
    del root_ids_df['id']

    if not last_updated_time_stamp:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    cg = ChunkedGraphGateway(pcg_table_name)
    old_roots, new_roots = cg.get_proofread_root_ids(last_updated_time_stamp, time_stamp)
    
    root_id_map = dict(zip(old_roots, new_roots))

    for col in root_ids_df:
        mask = root_ids_df[col].isin(root_id_map.keys())
        root_ids_df.loc[mask, col] = root_ids_df.loc[mask, col].map(root_id_map)
    
    # merge with existing data
    dataframe_with_root_ids = pd.merge(mat_df, root_ids_df, how='outer', left_on='annotation_id', right_on='annotation_id')
    dataframe_with_root_ids.sort_values(by='segmentation_id', inplace=True)
    # get data by column type
    supervoxel_data = dataframe_with_root_ids.loc[:, dataframe_with_root_ids.columns.str.endswith("supervoxel_id")]
    
    root_id_data = dataframe_with_root_ids.loc[:, dataframe_with_root_ids.columns.str.endswith("root_id")]

    # get mask of missing root_ids for lookup
    missing_values = root_id_data.isna()
    
    missing_roots = dataframe_with_root_ids[missing_values.any(axis=1)]

    supervoxels_array = supervoxel_data.to_numpy(dtype=np.uint64)
    supervoxel_columns = [column for column in dataframe_with_root_ids.columns if 'supervoxel_id' in column]
    
    root_ids_dict = {}
    
    for col_name, supervoxels_ids in supervoxel_data.items():
        root_id_col_name = make_root_id_column_name(col_name)
        supervoxels_array =  np.array(supervoxels_ids, dtype=np.uint64)
        root_id_array = cg.get_roots(supervoxels_array, time_stamp=last_updated_time_stamp)
        root_ids_dict[root_id_col_name] = root_id_array
        
    dataframe_with_root_ids.update(root_ids_dict)
    return dataframe_with_root_ids.to_dict(orient='list')

@celery.task(name="process:get_root_ids_from_supervoxels",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_root_ids_from_supervoxels(self, materialization_data: dict, mat_metadata: dict) -> dict:
    """Lookup root ids. First checks if root id columns exist. If there are existing
    root_ids present in the table, update them from last timestap if it is not None.
    Otherwise look up the list of root_ids from the supervoxel data present in the
    database.

    Parameters
    ----------
    materialization_data : dict
        dict of annotation and segmentation data
    mat_metadata : dict
        Materialization metadata


    Returns
    -------
    dict
        dict of annotation and with updated supervoxel and root id data
    """
    pcg_table_name = mat_metadata.get("pcg_table_name")
    cg = ChunkedGraphGateway(pcg_table_name)
    

    time_stamp = None
    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)

    frames = []
    columns = []

    for column_name, supervoxel_data in materialization_data.items():
        columns.append(column_name)
        supervoxel_df = pd.DataFrame(supervoxel_data,  columns=['id', 'supervoxel_id'])
        frames.append(supervoxel_df)
    df = pd.concat(frames, keys=columns)

    root_id_frames = []
    for column in columns:
        root_id_df = pd.DataFrame()

        col = make_root_id_column_name(column)

        column_ids = df.loc[column]['id']
        supervoxels_ids = df.loc[column]['supervoxel_id']
        supervoxels_array = np.array(supervoxels_ids.values.tolist(), dtype=np.uint64)
        root_ids = cg.get_roots(supervoxels_array, time_stamp=time_stamp)
        root_id_df['id'] = column_ids
        root_id_df[col] = root_ids.tolist()
        root_id_frames.append(root_id_df)
    data = pd.concat(root_id_frames)

    return data.apply(lambda x: [x.dropna().to_dict()], axis=1).sum()


@celery.task(name="process:get_expired_root_ids", 
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def get_expired_root_ids(self, mat_metadata):
    """deprecated

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    dict
        root_id data
    """
    if time_stamp is None:
        time_stamp = datetime.datetime.utcnow() - datetime.timedelta(minutes=5)
    last_updated_time_stamp = mat_metadata.get('last_updated_time_stamp')
    root_id_dict = {}
    root_column_names = []

    pcg_table_name = mat_metadata.get("pcg_table_name")
    cg = ChunkedGraphGateway(pcg_table_name)
    old_roots, new_roots = cg.get_proofread_root_ids(last_updated_time_stamp, time_stamp)
    for columns, values in supervoxel_id_data.items():
        col = make_root_id_column_name(columns)
        root_column_names.append(col)
        supervoxel_data = np.asarray(values, dtype=np.uint64)
        pk = supervoxel_data[:, 0]

        root_ids = cg.get_roots(supervoxel_data[:, 1], time_stamp=time_stamp)

        root_id_dict["id"] = pk
        root_id_dict[col] = root_ids

    root_column_names.append("id")
    df = pd.DataFrame(root_id_dict)
    data = df.to_dict("records")
    return data


@celery.task(name="process:update_root_ids",
             bind=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_root_ids(self, materialization_data: dict, mat_metadata: dict) -> bool:
    """Update sql databse with updated root ids

    Parameters
    ----------
    materialization_data : dict
        dict of annotation and segmentation data
    metadata : dict
        Materialization metadata

    Returns
    -------
    bool
        True is update was successful
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    try:
        self.session.bulk_update_mappings(SegmentationModel, root_id_data)
        self.session.commit()
    except Exception as e:
        self.cached_session.rollback()
        celery_logger.error(f"SQL Error: {e}")
    finally:
        self.session.close()
    return True


@celery.task(name="process:update_supervoxel_rows",
             bind=True,
             acks_late=True)
def update_supervoxel_rows(self, materialization_data: dict, mat_metadata: dict) -> bool:
    """Update supervoxel ids in database

    Parameters
    ----------
    materialization_data : dict
        dict of annotation and segmentation data
    metadata : dict
        Materialization metadata


    Returns
    -------
    bool
        [description]
    """
    SegmentationModel = create_segmentation_model(mat_metadata)
    schema_name = mat_metadata.get('schema')
    segmentations = []
    for k, v in materialization_data.items():
        for row in v:
            row[k] = row.pop('supervoxel_id')
            segmentations.append(row)

    schema_type = self.get_schema(schema_name)

    __, flat_segmentation_schema = em_models.split_annotation_schema(schema_type)

    for segmentation in segmentations:
        flat_data = [
            data[key]
            for key, value in flat_segmentation_schema._declared_fields.items() if key in data]

    try:
        for data in flat_data:
            self.session.bulk_update_mappings(SegmentationModel, data)
            self.session.flush()
        self.session.commit()
        return True
    except Exception as e:
        self.Session.rollback()
        celery_logger.error(f"SQL ERROR {e}")
        return False
    finally:
        self.session.close()


@celery.task(name="process:fin", acks_late=True)
def fin(*args, **kwargs):
    return True


@celery.task(name="process:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs


def create_segmentation_model(mat_metadata):
    annotation_table_id = mat_metadata.get('annotation_table_id')
    schema_type = mat_metadata.get("schema")
    pcg_table_name = mat_metadata.get("pcg_table_name")

    SegmentationModel = em_models.make_segmentation_model(annotation_table_id, schema_type, pcg_table_name)
    return SegmentationModel

def create_annotation_model(mat_metadata):
    annotation_table_id = mat_metadata.get('annotation_table_id')
    schema_type = mat_metadata.get("schema")

    AnnotationModel = em_models.make_annotation_model(annotation_table_id, schema_type)
    return AnnotationModel

def get_geom_from_wkb(wkb):
    wkb_element = to_shape(wkb)
    if wkb_element.has_z:
        return [int(wkb_element.xy[0][0]), int(wkb_element.xy[1][0]), int(wkb_element.z)]

def get_query_columns_by_suffix(AnnotationModel, SegmentationModel, suffix):
    seg_columns = [column.name for column in SegmentationModel.__table__.columns]
    anno_columns = [column.name for column in AnnotationModel.__table__.columns]

    matched_columns = set()
    for column in seg_columns:
        prefix = (column.split("_")[0])
        for anno_col in anno_columns:
            if anno_col.startswith(prefix):
                matched_columns.add(anno_col)
    matched_columns.remove('id')

    supervoxel_columns =  [f"{col.rsplit('_', 1)[0]}_{suffix}" for col in matched_columns if col != 'annotation_id']
    # # create model columns for querying
    anno_model_cols = [getattr(AnnotationModel, name) for name in matched_columns]
    anno_model_cols.append(AnnotationModel.id)
    seg_model_cols = [getattr(SegmentationModel, name) for name in supervoxel_columns]

    # add id columns to lookup
    seg_model_cols.extend([SegmentationModel.annotation_id, SegmentationModel.id])
    return anno_model_cols, seg_model_cols, supervoxel_columns