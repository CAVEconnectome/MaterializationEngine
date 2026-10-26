"""process_chunk_points does a whole spatial chunk in-process, as the Ray upload runs it."""

from unittest import mock

import numpy as np
import pandas as pd
import pytest

from materializationengine.workflows import spatial_lookup

MAT_INFO = {
    "segmentation_source": "graphene://fake",
    "coord_resolution": [4, 4, 40],
    "materialization_time_stamp": "2026-10-25 20:37:42.000001",
}


def _points_df():
    return pd.DataFrame(
        [
            {"id": 1, "type": "pre_pt", "pt_position": [10, 10, 1]},
            {"id": 1, "type": "post_pt", "pt_position": [11, 10, 1]},
            {"id": 2, "type": "pre_pt", "pt_position": [500, 10, 1]},
            {"id": 2, "type": "post_pt", "pt_position": [501, 10, 1]},
        ]
    )


@pytest.fixture
def patched():
    def split(points, cv, resolution, sub_batch_size):
        return [points[i : i + sub_batch_size] for i in range(0, len(points), sub_batch_size)]

    def lookup(cv, pts, resolution):
        return np.array([100 + p[0] for p in pts], dtype=np.uint64)

    def roots(data, mat_info):
        return [dict(d, pre_pt_root_id=1, post_pt_root_id=2) for d in data]

    inserted = []

    def insert(data, mat_info):
        inserted.append(data)
        return len(data)

    with mock.patch.object(spatial_lookup, "get_pts_from_bbox", return_value=_points_df()), \
            mock.patch.object(spatial_lookup.cloudvolume_cache, "get_cv", return_value=object()), \
            mock.patch.object(spatial_lookup, "split_points_by_storage_chunk", side_effect=split), \
            mock.patch.object(spatial_lookup, "lookup_supervoxel_ids", side_effect=lookup), \
            mock.patch.object(spatial_lookup, "get_root_ids_from_supervoxels", side_effect=roots), \
            mock.patch.object(spatial_lookup, "insert_segmentation_data", side_effect=insert):
        yield inserted


class TestProcessChunkPoints:
    def test_chunk_is_looked_up_and_upserted_in_sub_batches(self, patched):
        result = spatial_lookup.process_chunk_points("staging", MAT_INFO, [0, 0, 0], [1000, 1000, 10], sub_batch_size=2)
        assert result == {"points": 4, "rows": 2, "sub_batches": 2}
        assert patched == [
            [{"id": 1, "pre_pt_supervoxel_id": 110, "post_pt_supervoxel_id": 111, "pre_pt_root_id": 1, "post_pt_root_id": 2}],
            [{"id": 2, "pre_pt_supervoxel_id": 600, "post_pt_supervoxel_id": 601, "pre_pt_root_id": 1, "post_pt_root_id": 2}],
        ]

    def test_empty_chunk_does_nothing(self, patched):
        with mock.patch.object(spatial_lookup, "get_pts_from_bbox", return_value=pd.DataFrame()):
            result = spatial_lookup.process_chunk_points("staging", MAT_INFO, [0, 0, 0], [1, 1, 1])
        assert result == {"points": 0, "rows": 0, "sub_batches": 0}
        assert patched == []

    def test_should_stop_is_checked_between_sub_batches(self, patched):
        calls = iter([False, True])
        with pytest.raises(InterruptedError):
            spatial_lookup.process_chunk_points(
                "staging", MAT_INFO, [0, 0, 0], [1000, 1000, 10], sub_batch_size=2, should_stop=lambda: next(calls)
            )
        assert len(patched) == 1

    def test_rebuild_segmentation_indices_runs_each_command(self):
        mat_metadata = {"segmentation_table_name": "t__seg", "aligned_volume": "av"}
        with mock.patch.object(spatial_lookup.db_manager, "get_engine"), \
                mock.patch.object(spatial_lookup, "create_segmentation_model"), \
                mock.patch.object(spatial_lookup.index_cache, "drop_table_indices") as drop, \
                mock.patch.object(spatial_lookup.index_cache, "add_indices_sql_commands", return_value=["A", "B"]), \
                mock.patch.object(spatial_lookup.add_index, "run") as add, \
                mock.patch.object(spatial_lookup.update_metadata, "run") as update:
            assert spatial_lookup.rebuild_segmentation_indices(mat_metadata, "staging") == ["A", "B"]
        drop.assert_called_once()
        assert [c.args for c in add.call_args_list] == [("staging", "A"), ("staging", "B")]
        update.assert_called_once_with(mat_metadata)
