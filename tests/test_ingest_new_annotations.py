
# Mock pychunkgraph imports and cloudvolume before importing
# the tasks
import sys
import logging
from unittest import mock
sys.modules['materializationengine.chunkedgraph_gateway'] = mock.MagicMock()
sys.modules['cloudvolume'] = mock.MagicMock()

import numpy as np
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    get_annotations_with_missing_supervoxel_ids,
    get_cloudvolume_supervoxel_ids, get_new_root_ids, get_sql_supervoxel_ids,
    insert_segmentation_data)
from numpy import nan


missing_segmentation_data = {'post_pt_supervoxel_id': [nan],
                              'pre_pt_supervoxel_id': [nan],
                              'post_pt_position': [
                                  [73000, 83000, 93000],
                                  ],
                              'pre_pt_position': [
                                  [13000, 23000, 33000],
                                  ],
                              'id': [4]}


mocked_supervoxel_data = {'post_pt_supervoxel_id': [10000000],
                          'pre_pt_supervoxel_id': [10000000],
                          'post_pt_position': [
                              [73000, 83000, 93000],
                              ],
                          'pre_pt_position': [
                              [13000, 23000, 33000],
                              ],
                          'id': [4]}


mocked_root_id_data = [{'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 1,
                        'post_pt_root_id': 20000000000000000,
                        'pre_pt_root_id': 10000000000000000},
                       {'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 2, 
                        'post_pt_root_id': 40000000000000000,
                        'pre_pt_root_id': 30000000000000000},
                       {'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'id': 3,
                        'post_pt_root_id': 60000000000000000,
                        'pre_pt_root_id': 50000000000000000}
                       ]


def test_create_missing_segmentation_table(mat_metadata, db_client):
    table_metadata = create_missing_segmentation_table.s(mat_metadata).apply()
    
    __, engine= db_client

    seg_table_exists= engine.dialect.has_table(
        engine.connect(), "test_synapse_table__test_pcg")
    assert table_metadata.get() == mat_metadata
    assert seg_table_exists == True


def test_get_annotations_with_missing_supervoxel_ids(mat_metadata):
    id_chunk_range = [1, 5]
    annotations = get_annotations_with_missing_supervoxel_ids(
        mat_metadata, id_chunk_range)
    assert annotations == missing_segmentation_data

@mock.patch('materializationengine.workflows.ingest_new_annotations.cloudvolume.CloudVolume')
def test_get_cloudvolume_supervoxel_ids(mock_cv, mat_metadata):
    mock_cv.return_value = True

    with mock.patch("materializationengine.workflows.ingest_new_annotations.get_sv_id") as mock_get_sv_id:
        mock_get_sv_id.return_value = np.ndarray((1,), buffer=np.array([10000000]), dtype=int)
        supervoxel_data = get_cloudvolume_supervoxel_ids(
            missing_segmentation_data, mat_metadata)
    assert supervoxel_data == mocked_supervoxel_data

@mock.patch('materializationengine.workflows.ingest_new_annotations.chunkedgraph_cache.init_pcg')
def test_get_new_root_ids(mock_chunkgraph, mat_metadata, annotation_data):
    mock_chunkgraph.return_value = True

    with mock.patch("materializationengine.workflows.ingest_new_annotations.get_root_ids") as mock_get_roots:
        mock_get_roots.return_value = np.ndarray((1,), buffer=np.array([20000000]), dtype=int)
        root_ids = get_new_root_ids(mocked_supervoxel_data, mat_metadata)
    assert root_ids == [{'post_pt_supervoxel_id': 10000000,
                        'pre_pt_supervoxel_id': 10000000,
                        'post_pt_root_id': 20000000,
                        'pre_pt_root_id': 20000000,
                        'id': 4}]

def test_insert_segmentation_data(test_app, annotation_data, mat_metadata):
    segmentation_data = annotation_data['new_segmentation_data']
    num_of_rows = insert_segmentation_data(segmentation_data, mat_metadata)
    assert num_of_rows == {'Segmentation data inserted': 1} 


def test_get_sql_supervoxel_ids(test_app, mat_metadata):
    id_chunk_range = [1, 4]
    supervoxel_ids = get_sql_supervoxel_ids(id_chunk_range, mat_metadata)
    logging.info(supervoxel_ids)
    assert True
