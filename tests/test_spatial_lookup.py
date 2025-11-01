import numpy as np
import pandas as pd


class TestDataFrameMerging:
    """Test class for dataframe merging operations"""
    
    def create_dataframes(self, seg_columns, data):
        segmentation_dataframe = pd.DataFrame(columns=seg_columns, dtype=object)
        data_df = pd.DataFrame(data, dtype=object)
        print(f"Segmentation dataframe: {segmentation_dataframe}")
        print(f"Data dataframe: {data_df}")
        return segmentation_dataframe, data_df
    
    def merge_dataframes(self, segmentation_dataframe, data_df):
        common_cols = segmentation_dataframe.columns.intersection(data_df.columns)
        print(f"Common columns: {common_cols}")
        df = pd.merge(segmentation_dataframe[common_cols], data_df[common_cols], how="right")
        df = df.infer_objects().fillna(0)
        df = df.reindex(columns=segmentation_dataframe.columns, fill_value=0)
        print(f"Merged dataframe: {df}")
        return df
    
    def test_dataframe_merging_basic(self):
        seg_columns = ["id", "column1", "column2"]
        data = {
            "id": [1, 2, 3],
            "column1": [10, 20, 30],
            "column3": [100, 200, 300],
        }
        segmentation_dataframe, data_df = self.create_dataframes(seg_columns, data)
        df = self.merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [10, 20, 30]
        assert df["column2"].tolist() == [0, 0, 0]
    
    def test_dataframe_merging_fewer_columns(self):
        seg_columns = ["id", "column1", "column2", "column3"]
        data = {
            "id": [1, 2, 3],
            "column1": [10, 20, 30],
        }
        segmentation_dataframe, data_df = self.create_dataframes(seg_columns, data)
        df = self.merge_dataframes(segmentation_dataframe, data_df)
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
        segmentation_dataframe, data_df = self.create_dataframes(seg_columns, data)
        df = self.merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [np.uint64(2**63), np.uint64(2**63 + 1), np.uint64(2**63 + 2)]
        assert df["column2"].tolist() == [np.uint64(2**64 - 3), np.uint64(2**64 - 2), np.uint64(2**64 - 1)]
    
    def test_dataframe_merging_int64(self):
        seg_columns = ["id", "column1", "column2"]
        data = {
            "id": [1, 2, 3],
            "column1": [np.int64(2**62), np.int64(2**62 + 1), np.int64(2**62 + 2)],
            "column2": [np.int64(-2**63), np.int64(-2**63 + 1), np.int64(-2**63 + 2)],
        }
        segmentation_dataframe, data_df = self.create_dataframes(seg_columns, data)
        df = self.merge_dataframes(segmentation_dataframe, data_df)
        assert df["id"].tolist() == [1, 2, 3]
        assert df["column1"].tolist() == [np.int64(2**62), np.int64(2**62 + 1), np.int64(2**62 + 2)]
        assert df["column2"].tolist() == [np.int64(-2**63), np.int64(-2**63 + 1), np.int64(-2**63 + 2)]