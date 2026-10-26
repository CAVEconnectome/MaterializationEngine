import io
from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from materializationengine.blueprints.upload.processor import (
    SchemaProcessor,
    encode_points_wkb_hex,
)

SEPARATE_MAPPING = {
    f"{point}_position_{axis}": f"{point.split('_')[0]}_{axis}"
    for point in ["pre_pt", "post_pt", "ctr_pt"]
    for axis in "xyz"
}
SEPARATE_MAPPING.update({"id": "id", "size": "size"})


def per_row(processor, chunk, coordinate_cols):
    """The old per-row path, as text."""
    out = []
    for _, row in chunk.iterrows():
        point = processor.process_spatial_point(row, coordinate_cols)
        out.append(None if point is None else str(point))
    return out


def as_text(values):
    return [None if v is None else str(v) for v in values]


class TestUploadProcessor:
    def test_encode_points_matches_shapely_wkb(self):
        rng = np.random.default_rng(0)
        xyz = np.vstack(
            [
                rng.integers(0, 2**20, size=(50, 3)).astype(float),
                rng.normal(scale=1e6, size=(50, 3)),
                [[0.0, -0.0, 1.5], [-1.0, 2**53, 1e-300], [np.inf, -np.inf, 3.0]],
            ]
        )
        from shapely.geometry import Point

        expected = [Point(row).wkb_hex.lower() for row in xyz]
        assert list(encode_points_wkb_hex(xyz)) == expected


    def test_encode_points_nan_rows_are_none(self):
        xyz = np.array([[1.0, 2.0, 3.0], [np.nan, 2.0, 3.0], [1.0, 2.0, np.nan]])
        out = encode_points_wkb_hex(xyz)
        assert out[0] is not None
        assert out[1] is None and out[2] is None


    def test_encode_points_empty(self):
        assert len(encode_points_wkb_hex(np.empty((0, 3)))) == 0


    def test_separate_columns_match_per_row(self):
        processor = SchemaProcessor("synapse", column_mapping=SEPARATE_MAPPING)
        chunk = pd.DataFrame(
            {
                "pre_x": [1, 2.5, -3, np.nan, "7", "abc", None, 2**40],
                "pre_y": [4, 5.5, -6, 1, "8", 1, 1, 2**41],
                "pre_z": [7, 8.5, -9, 1, "9", 1, 1, 2**42],
            },
            dtype=object,
        )
        cols = ["pre_x", "pre_y", "pre_z"]
        assert as_text(processor._spatial_column(chunk, cols)) == per_row(
            processor, chunk, cols
        )


    @pytest.mark.parametrize(
        "value",
        [
            "[1,2,3]",
            "[1.5, -2.25, 3e5]",
            "(4, 5, 6)",
            " [7,8,9] ",
            "[1,2]",
            "[1,2,3,4]",
            "[a,b,c]",
            "",
            np.nan,
            None,
        ],
    )
    def test_combined_column_matches_per_row(self, value):
        mapping = {"pre_pt_position": "pre_xyz"}
        processor = SchemaProcessor("synapse", column_mapping=mapping)
        chunk = pd.DataFrame({"pre_xyz": [value]}, dtype=object)
        assert as_text(processor._spatial_column(chunk, ["pre_xyz"])) == per_row(
            processor, chunk, ["pre_xyz"]
        )


    def test_combined_column_accepts_lists(self):
        processor = SchemaProcessor(
            "synapse", column_mapping={"pre_pt_position": "pre_xyz"}
        )
        chunk = pd.DataFrame({"pre_xyz": [[1, 2, 3], np.array([4.0, 5.0, 6.0])]})
        out = processor._spatial_column(chunk, ["pre_xyz"])
        assert list(out) == list(encode_points_wkb_hex(np.array([[1, 2, 3], [4, 5, 6]])))


    def test_process_chunk_csv_unchanged(self):
        """A whole chunk gives the same CSV text as the per-row geometry did."""
        rng = np.random.default_rng(1)
        n = 500
        data = {"id": np.arange(1, n + 1), "size": rng.integers(1, 10000, n)}
        for point in ["pre", "post", "ctr"]:
            for axis in "xyz":
                data[f"{point}_{axis}"] = rng.integers(0, 300000, n)
        chunk = pd.DataFrame(data)
        chunk.loc[[3, 40], "pre_x"] = np.nan
        chunk.loc[77, "ctr_z"] = np.nan
        timestamp = datetime(2026, 10, 24)

        fast = SchemaProcessor("synapse", column_mapping=SEPARATE_MAPPING)
        slow = SchemaProcessor("synapse", column_mapping=SEPARATE_MAPPING)
        slow._spatial_column = lambda c, cols: [
            slow.process_spatial_point(row, cols) for _, row in c.iterrows()
        ]

        def to_text(df):
            buf = io.StringIO()
            df.to_csv(buf, index=False, header=False)
            return buf.getvalue()

        fast_out = fast.process_chunk(chunk, timestamp)
        assert len(fast_out) == n - 3
        assert to_text(fast_out) == to_text(slow.process_chunk(chunk, timestamp))
