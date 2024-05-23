import numpy as np
import pandas as pd
import pytest
from materializationengine.workflows.spatial_lookup import (
    calc_min_enclosing_and_sub_volumes,
)


def create_dataframes(seg_columns, data):
    segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
    data_df = pd.DataFrame(data, dtype=object)
    print(f"Segmentation dataframe: {segmentation_dataframe}")
    print(f"Data dataframe: {data_df}")
    return segmentation_dataframe, data_df


def merge_dataframes(segmentation_dataframe, data_df):
    common_cols = segmentation_dataframe.columns.intersection(data_df.columns)
    print(f"Common columns: {common_cols}")
    df = pd.merge(
        segmentation_dataframe[common_cols], data_df[common_cols], how="right"
    )
    df = df.infer_objects().fillna(0)
    df = df.reindex(columns=segmentation_dataframe.columns, fill_value=0)
    print(f"Merged dataframe: {df}")
    return df


class TestSpatialLookup:

    def test_dataframe_merging_basic(self):
        seg_columns = ["id", "column1", "column2"]
        data = {
            "id": [1, 2, 3],
            "column1": [10, 20, 30],
            "column3": [100, 200, 300],
        }
        segmentation_dataframe, data_df = create_dataframes(seg_columns, data)
        df = merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [10, 20, 30]
        assert df["column2"].tolist() == [0, 0, 0]


    def test_dataframe_merging_fewer_columns(self):
        seg_columns = ["id", "column1", "column2", "column3"]
        data = {
            "id": [1, 2, 3],
            "column1": [10, 20, 30],
        }
        segmentation_dataframe, data_df = create_dataframes(seg_columns, data)
        df = merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [10, 20, 30]
        assert df["column2"].tolist() == [0, 0, 0]
        assert df["column3"].tolist() == [0, 0, 0]


    def test_dataframe_merging_uint64(self):
        seg_columns = ["id", "column1", "column2"]
        data = {
            "id": [1, 2, 3],
            "column1": [np.uint64(2**63), np.uint64(2**63 + 1), np.uint64(2**63 + 2)],
            "column2": [np.uint64(2**64 - 3), np.uint64(2**64 - 2), np.uint64(2**64 - 1)],
        }
        segmentation_dataframe, data_df = create_dataframes(seg_columns, data)
        df = merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [
            np.uint64(2**63),
            np.uint64(2**63 + 1),
            np.uint64(2**63 + 2),
        ]
        assert df["column2"].tolist() == [
            np.uint64(2**64 - 3),
            np.uint64(2**64 - 2),
            np.uint64(2**64 - 1),
        ]


    def test_dataframe_merging_int64(self):
        seg_columns = ["id", "column1", "column2"]
        data = {
            "id": [1, 2, 3],
            "column1": [np.int64(2**62), np.int64(2**62 + 1), np.int64(2**62 + 2)],
            "column2": [np.int64(-(2**63)), np.int64(-(2**63) + 1), np.int64(-(2**63) + 2)],
        }
        segmentation_dataframe, data_df = create_dataframes(seg_columns, data)
        df = merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [
            np.int64(2**62),
            np.int64(2**62 + 1),
            np.int64(2**62 + 2),
        ]
        assert df["column2"].tolist() == [
            np.int64(-(2**63)),
            np.int64(-(2**63) + 1),
            np.int64(-(2**63) + 2),
        ]


    # Test cases for the function calc_min_enclosing_and_sub_volumes


    @pytest.mark.parametrize(
        "test_id, input_bboxes, global_bbox, chunk_size, cv_resolution, coord_resolution, expected_enclosing_bbox, expected_sub_volumes",
        [
            # Happy path test: Single bbox fully within global_bbox
            (
                "HP-1",
                [[np.array([10, 10, 10]), np.array([20, 20, 20])]],
                [np.array([0, 0, 0]), np.array([30, 30, 30])],
                [5, 5, 5],
                [1, 1, 1],
                [1, 1, 1],
                np.array([[10, 10, 10], [20, 20, 20]]),
                [],
            ),
            # Happy path test: Multiple bboxes, some partially outside global_bbox
            (
                "HP-2",
                [
                    [np.array([10, 10, 10]), np.array([40, 40, 40])],
                    [np.array([-10, -10, -10]), np.array([5, 5, 5])],
                ],
                [np.array([0, 0, 0]), np.array([30, 30, 30])],
                [10, 10, 10],
                [1, 1, 1],
                [1, 1, 1],
                np.array([[0, 0, 0], [30, 30, 30]]),
                [np.array([[31, 0, 0], [40, 40, 40]]), 
                 np.array([[0, 31, 0], [40, 40, 40]]),
                 np.array([[0, 0, 31], [40, 40, 40]]),
                 np.array([[-10, -10, -10], [-1, 30, 30]]),
                 np.array([[-10, -10, -10], [30, -1, 30]]),
                 np.array([[-10, -10, -10], [30, 30, -1]])],
            ),
            # Edge case: No bboxes provided
            (
                "EC-1",
                [],
                [np.array([0, 0, 0]), np.array([30, 30, 30])],
                [10, 10, 10],
                [1, 1, 1],
                [1, 1, 1],
                None,
                [],
            ),
        ],
    )
    def test_calc_min_enclosing_and_sub_volumes(
        self,
        test_id,
        input_bboxes,
        global_bbox,
        chunk_size,
        cv_resolution,
        coord_resolution,
        expected_enclosing_bbox,
        expected_sub_volumes,
    ):
        # Arrange
        # Parameters are set up by pytest's parametrize, so no additional arrangement is necessary.

        # Act
        result_enclosing_bbox, result_sub_volumes = calc_min_enclosing_and_sub_volumes(
            input_bboxes, global_bbox, chunk_size, cv_resolution, coord_resolution
        )

        # Assert
        if expected_enclosing_bbox is None:
            assert result_enclosing_bbox is None
        else:
            np.testing.assert_array_equal(result_enclosing_bbox, expected_enclosing_bbox)

        assert len(result_sub_volumes) == len(expected_sub_volumes)
        for result_sub, expected_sub in zip(result_sub_volumes, expected_sub_volumes):
            np.testing.assert_array_equal(result_sub, expected_sub)


