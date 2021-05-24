import datetime
import os
from typing import List

from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from dynamicannotationdb.models import AnnoMetadata, SegmentationMetadata
from flask import current_app
from sqlalchemy import and_, func, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.expression import except_

from materializationengine.celery_init import celery
from materializationengine.database import (dynamic_annotation_cache,
                                            sqlalchemy_cache)
from materializationengine.utils import (create_annotation_model,
                                         get_config_param)

celery_logger = get_task_logger(__name__)



def chunk_annotation_ids(mat_metadata: dict) -> List[List]:
    """Creates list of chunks with start:end index for chunking queries for materialziation.

    Parameters
    ----------
    mat_metadata : dict
        Materialization metadata

    Returns
    -------
    List[List]
        list of list containg start and end indices
    """
    celery_logger.info("Chunking supervoxel ids")
    AnnotationModel = create_annotation_model(mat_metadata)
    chunk_size = mat_metadata.get('chunk_size', None)
    
    if not chunk_size:
        ROW_CHUNK_SIZE = get_config_param("MATERIALIZATION_ROW_CHUNK_SIZE")
        chunk_size = ROW_CHUNK_SIZE

    chunked_ids = chunk_ids(mat_metadata, AnnotationModel.id, chunk_size)

    return [chunk for chunk in chunked_ids]

@celery.task(name="process:fin", acks_late=True, bind=True)
def fin(self, *args, **kwargs):
    return True

def get_materialization_info(datastack_info: dict,
                             analysis_version: int=None,
                             materialization_time_stamp: datetime.datetime.utcnow=None,
                             skip_table:bool=False,
                             row_size:int=1_000_000) -> List[dict]:
    """Initialize materialization by an aligned volume name. Iterates thorugh all
    tables in a aligned volume database and gathers metadata for each table. The list
    of tables are passed to workers for materialization. 

    Args:
        datastack_info (dict): Datastack info
        analysis_version (int, optional): Analysis version to use for frozen materialization. Defaults to None.
        skip_table (bool, optional): Triggers row count for skipping tables larger than row_size arg. Defaults to False.
        row_size (int, optional): Row size number to check. Defaults to 1_000_000.

    Returns:
        List[dict]: [description]
    """

    aligned_volume_name = datastack_info['aligned_volume']['name']
    pcg_table_name = datastack_info['segmentation_source'].split("/")[-1]
    segmentation_source = datastack_info.get('segmentation_source')
    
    if not materialization_time_stamp:
        materialization_time_stamp = datetime.datetime.utcnow()
    
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    
    try:
        annotation_tables = db.get_valid_table_names()
        metadata = []
        for annotation_table in annotation_tables:
            row_count = db._get_table_row_count(annotation_table, filter_valid=True)
            if row_count >= row_size and skip_table:
                continue
            else:
                max_id = db.get_max_id_value(annotation_table)
                min_id = db.get_min_id_value(annotation_table)
                if max_id:
                    segmentation_table_name = build_segmentation_table_name(
                        annotation_table, pcg_table_name)
                    try:
                        segmentation_metadata = db.get_segmentation_table_metadata(annotation_table,
                                                                                   pcg_table_name)
                        create_segmentation_table = False
                    except AttributeError as e:
                        celery_logger.warning(f"SEGMENTATION TABLE DOES NOT EXIST: {e}")
                        segmentation_metadata = {'last_updated': None}
                        create_segmentation_table = True
                    last_updated_time_stamp = segmentation_metadata.get('last_updated', None)

                    if not last_updated_time_stamp:
                        last_updated_time_stamp = None
                    else:
                        last_updated_time_stamp = str(last_updated_time_stamp)


                    table_metadata = {
                        'datastack': datastack_info['datastack'],
                        'aligned_volume': str(aligned_volume_name),
                        'schema': db.get_table_schema(annotation_table),
                        'create_segmentation_table': create_segmentation_table,
                        'max_id': int(max_id),
                        'min_id': int(min_id),
                        'row_count': row_count,
                        'add_indices': True,
                        'segmentation_table_name': segmentation_table_name,
                        'annotation_table_name': annotation_table,
                        'temp_mat_table_name': f"temp__{annotation_table}",
                        'pcg_table_name': pcg_table_name,
                        'segmentation_source': segmentation_source,
                        'coord_resolution': [4,4,40],
                        'materialization_time_stamp': str(materialization_time_stamp),
                        'last_updated_time_stamp': last_updated_time_stamp,
                        'chunk_size': 100000,
                        'table_count': len(annotation_tables),
                        'find_all_expired_roots': datastack_info.get('find_all_expired_roots', False)
                    }
                    if analysis_version:
                        table_metadata['analysis_version'] = analysis_version
                        table_metadata['analysis_database'] = f"{datastack_info['datastack']}__mat{analysis_version}"

                    metadata.append(table_metadata.copy())
        db.cached_session.close()
        return metadata
    except Exception as e:
        db.cached_session.rollback()
        celery_logger.error(e)


@celery.task(name="process:collect_data", acks_late=True)
def collect_data(*args, **kwargs):
    return args, kwargs

def query_id_range(column, start_id: int, end_id: int):
    if end_id:
        return and_(column >= start_id, column < end_id)
    else:
        return column >= start_id


def chunk_ids(mat_metadata, model, chunk_size: int):
    aligned_volume = mat_metadata.get('aligned_volume')
    session = sqlalchemy_cache.get(aligned_volume)

    q = session.query(
        model, func.row_number().over(order_by=model).label("row_count")
    ).from_self(model)

    if chunk_size > 1:
        q = q.filter(text("row_count %% %d=1" % chunk_size))

    chunks = [id for id, in q]

    while chunks:
        chunk_start = chunks.pop(0)
        if chunks:
            chunk_end = chunks[0]
        else:
            chunk_end = None
        yield [chunk_start, chunk_end]


@celery.task(name="process:update_metadata",
             bind=True,
             acks_late=True,
             autoretry_for=(Exception,),
             max_retries=3)
def update_metadata(self, mat_metadata: dict):
    """Update 'last_updated' column in the segmentation 
    metadata table for a given segmentation table.
    

    Args:
        mat_metadata (dict): materialziation metadata

    Returns:
        str: description of table that was updated
    """
    aligned_volume = mat_metadata['aligned_volume']
    segmentation_table_name = mat_metadata['segmentation_table_name']

    session = sqlalchemy_cache.get(aligned_volume)
    
    materialization_time_stamp = mat_metadata['materialization_time_stamp']
    try:
        last_updated_time_stamp = datetime.datetime.strptime(materialization_time_stamp, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        last_updated_time_stamp = datetime.datetime.strptime(materialization_time_stamp, '%Y-%m-%dT%H:%M:%S.%f')

    try:
        seg_metadata = session.query(SegmentationMetadata).filter(
            SegmentationMetadata.table_name == segmentation_table_name).one()
        seg_metadata.last_updated = last_updated_time_stamp
        session.commit()
    except Exception as e:
        celery_logger.error(f"SQL ERROR: {e}")
        session.rollback()
    finally:
        session.close()
    return {f"Table: {segmentation_table_name}": f"Time stamp {materialization_time_stamp}"}

@celery.task(name="process:add_index",
             bind=True,
             acks_late=True,
             task_reject_on_worker_lost=True,
             autoretry_for=(Exception,),
             max_retries=3)
def add_index(self, database: dict, command: str):
    """Add an index or a contrainst to a table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice
        command (str): sql command to create an index or constraint

    Raises:
        self.retry: retries task when an error creating an index occurs

    Returns:
        str: String of SQL command
    """
    engine = sqlalchemy_cache.get_engine(database)

    # increase maintenance memory to improve index creation speeds,
    # reset to default after index is created
    ADD_INDEX_SQL = f"""
        SET maintenance_work_mem to '1GB';
        {command}
        SET maintenance_work_mem to '64MB';
    """

    try:
        with engine.begin() as conn:
            celery_logger.info(f"Adding index: {command}")
            result = conn.execute(ADD_INDEX_SQL)
    except ProgrammingError as index_error:
        celery_logger.error(index_error)
        return "Index already exists"
    except Exception as e:
        celery_logger.error(f"Index creation failed: {e}")
        raise self.retry(exc=e, countdown=3)        
    
    return f"Index {command} added to table"
