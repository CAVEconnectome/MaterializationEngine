"""Unit tests for materializationengine.workflows.deltalake_export

Tasks 4.1–4.5: output spec derivation, partition count heuristic,
bucket boundary computation, and bucket assignment strategies.
"""

from unittest.mock import MagicMock, patch

import numpy as np
import polars as pl
import pyarrow as pa
import pytest
import shapely

from materializationengine.workflows.deltalake_export import (
    _DEFAULT_BYTES_PER_ROW,
    DeltaLakeOutputSpec,
    _flush_buffer,
    assign_hash_bucket,
    assign_percentile_range_bucket,
    assign_uniform_range_bucket,
    compute_bucket_boundaries,
    decode_geometry_columns,
    discover_default_output_specs,
    estimate_bytes_per_row,
    export_table_to_deltalake,
    morton_encode_3d,
    optimize_deltalake,
    resolve_n_partitions,
)

# ---------------------------------------------------------------------------
# 4.1  discover_default_output_specs
# ---------------------------------------------------------------------------


class TestDiscoverDefaultOutputSpecs:
    """discover_default_output_specs should return one spec per
    non-spatial indexed column."""

    def _make_engine(self, indexes):
        """Return a mock engine whose inspector returns *indexes*."""
        engine = MagicMock()
        inspector = MagicMock()
        inspector.get_indexes.return_value = indexes
        with patch(
            "materializationengine.workflows.deltalake_export.inspect",
            return_value=inspector,
        ):
            return engine, inspector

    def test_single_btree_index(self):
        indexes = [
            {
                "name": "ix_synapse_pre_pt_root_id",
                "column_names": ["pre_pt_root_id"],
                "unique": False,
                "dialect_options": {},
            }
        ]
        engine, inspector = self._make_engine(indexes)
        with patch(
            "materializationengine.workflows.deltalake_export.inspect",
            return_value=inspector,
        ):
            specs = discover_default_output_specs("synapse", engine)

        assert len(specs) == 1
        assert specs[0].partition_by == "pre_pt_root_id"
        assert specs[0].partition_strategy == "percentile_range"
        assert specs[0].n_partitions == "auto"
        assert specs[0].zorder_columns == ["pre_pt_root_id"]
        assert specs[0].bloom_filter_columns == []
        assert specs[0].source_geometry_column is None

    def test_spatial_index_produces_morton_spec(self):
        indexes = [
            {
                "name": "idx_synapse_pt_position",
                "column_names": ["pt_position"],
                "unique": False,
                "dialect_options": {"postgresql_using": "gist"},
            }
        ]
        engine, inspector = self._make_engine(indexes)
        with patch(
            "materializationengine.workflows.deltalake_export.inspect",
            return_value=inspector,
        ):
            specs = discover_default_output_specs("synapse", engine)

        assert len(specs) == 1
        assert specs[0].partition_by == "pt_position_morton"
        assert specs[0].partition_strategy == "percentile_range"
        assert specs[0].zorder_columns == [
            "pt_position_x",
            "pt_position_y",
            "pt_position_z",
        ]
        assert specs[0].source_geometry_column == "pt_position"

    def test_multiple_indexes(self):
        indexes = [
            {
                "name": "ix_pre",
                "column_names": ["pre_pt_root_id"],
                "unique": False,
                "dialect_options": {},
            },
            {
                "name": "ix_post",
                "column_names": ["post_pt_root_id"],
                "unique": False,
                "dialect_options": {},
            },
            {
                "name": "idx_spatial",
                "column_names": ["pt_position"],
                "unique": False,
                "dialect_options": {"postgresql_using": "gist"},
            },
        ]
        engine, inspector = self._make_engine(indexes)
        with patch(
            "materializationengine.workflows.deltalake_export.inspect",
            return_value=inspector,
        ):
            specs = discover_default_output_specs("synapse", engine)

        assert len(specs) == 3
        assert {s.partition_by for s in specs} == {
            "pre_pt_root_id",
            "post_pt_root_id",
            "pt_position_morton",
        }

    def test_no_indexes(self):
        engine, inspector = self._make_engine([])
        with patch(
            "materializationengine.workflows.deltalake_export.inspect",
            return_value=inspector,
        ):
            specs = discover_default_output_specs("empty_table", engine)

        assert specs == []


# ---------------------------------------------------------------------------
# estimate_bytes_per_row
# ---------------------------------------------------------------------------


class TestEstimateBytesPerRow:
    """estimate_bytes_per_row should query pg_class for table stats."""

    @patch("materializationengine.workflows.deltalake_export.pg_dbapi")
    def test_normal_table(self, mock_dbapi):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # 1000 pages, 100_000 rows → 1000 * 8192 / 100_000 ≈ 81 bytes/row
        mock_cur.fetchone.return_value = (1000, 100_000.0)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_dbapi.connect.return_value.__enter__ = lambda s: mock_conn
        mock_dbapi.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = estimate_bytes_per_row("postgresql://localhost/test", "my_table")
        assert result == 81

    @patch("materializationengine.workflows.deltalake_export.pg_dbapi")
    def test_no_stats_returns_default(self, mock_dbapi):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = None
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_dbapi.connect.return_value.__enter__ = lambda s: mock_conn
        mock_dbapi.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = estimate_bytes_per_row("postgresql://localhost/test", "missing")
        assert result == _DEFAULT_BYTES_PER_ROW

    @patch("materializationengine.workflows.deltalake_export.pg_dbapi")
    def test_zero_rows_returns_default(self, mock_dbapi):
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = (0, 0.0)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_dbapi.connect.return_value.__enter__ = lambda s: mock_conn
        mock_dbapi.connect.return_value.__exit__ = MagicMock(return_value=False)

        result = estimate_bytes_per_row("postgresql://localhost/test", "empty")
        assert result == _DEFAULT_BYTES_PER_ROW


# ---------------------------------------------------------------------------
# 4.2  resolve_n_partitions
# ---------------------------------------------------------------------------


class TestResolveNPartitions:
    """resolve_n_partitions: auto heuristic and explicit pass-through."""

    def test_explicit_value(self):
        assert resolve_n_partitions(64, row_count=1_000_000) == 64

    def test_auto_small_table(self):
        # 50 000 rows * 200 bytes ≈ 10 MB → 1 partition
        assert resolve_n_partitions("auto", row_count=50_000) == 1

    def test_auto_large_table(self):
        # 500_000_000 rows * 200 bytes = 100 GB → 100 GB / 256 MB ≈ 373
        n = resolve_n_partitions("auto", row_count=500_000_000)
        assert n > 300
        assert n < 500  # sanity bound

    def test_auto_medium_table(self):
        # 5_000_000 rows * 200 bytes = 1 GB → 1 GB / 256 MB ≈ 4
        n = resolve_n_partitions("auto", row_count=5_000_000)
        assert n >= 3
        assert n <= 5

    def test_auto_minimum_is_one(self):
        assert resolve_n_partitions("auto", row_count=1) == 1

    def test_custom_target_size(self):
        # 10_000_000 rows * 200 bytes = 2 GB
        # target 512 MB → 2 GB / 512 MB = 4
        n = resolve_n_partitions("auto", row_count=10_000_000, target_file_size_mb=512)
        assert n == 4


# ---------------------------------------------------------------------------
# 4.3  compute_bucket_boundaries
# ---------------------------------------------------------------------------


class TestComputeBucketBoundaries:
    """compute_bucket_boundaries queries Postgres for percentiles."""

    def test_single_partition_returns_empty(self):
        # No DB call needed for n_partitions <= 1
        assert compute_bucket_boundaries("unused", "t", "c", 1) == []

    @patch("materializationengine.workflows.deltalake_export.pg_dbapi")
    def test_four_partitions(self, mock_dbapi):
        # Simulate Postgres returning 3 boundary values for 4 partitions
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_cur.fetchone.return_value = ([10, 20, 30],)
        mock_conn.cursor.return_value.__enter__ = lambda s: mock_cur
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_dbapi.connect.return_value.__enter__ = lambda s: mock_conn
        mock_dbapi.connect.return_value.__exit__ = MagicMock(return_value=False)

        boundaries = compute_bucket_boundaries(
            "postgresql://localhost/test", "my_table", "root_id", 4
        )
        assert boundaries == [10, 20, 30]

        # Verify the SQL was correct
        call_args = mock_cur.execute.call_args[0][0]
        assert "percentile_disc" in call_args
        assert '"root_id"' in call_args
        assert '"my_table"' in call_args


# ---------------------------------------------------------------------------
# 4.4  assign_percentile_range_bucket
# ---------------------------------------------------------------------------


class TestAssignPercentileRangeBucket:
    """Rows should be split into roughly equal-count buckets with
    contiguous value ranges."""

    def test_basic_distribution(self):
        # 100 rows with values 0..99, boundaries at 25, 50, 75 → 4 buckets
        df = pl.DataFrame({"val": list(range(100))})
        result = assign_percentile_range_bucket(df, "val", [25, 50, 75])

        assert "val_partition" in result.columns
        counts = result.group_by("val_partition").len().sort("val_partition")
        # Each bucket should have ~25 rows (exact depends on boundary inclusion)
        for row in counts.iter_rows():
            assert row[1] >= 20  # at least 20 (allowing boundary effects)
            assert row[1] <= 30

    def test_contiguous_ranges(self):
        # Verify that values within each bucket are contiguous
        df = pl.DataFrame({"val": list(range(100))})
        result = assign_percentile_range_bucket(df, "val", [25, 50, 75])

        for bucket in result["val_partition"].unique().sort().to_list():
            bucket_vals = (
                result.filter(pl.col("val_partition") == bucket)["val"].sort().to_list()
            )
            # Values should be contiguous: max - min + 1 == count
            assert bucket_vals[-1] - bucket_vals[0] + 1 == len(bucket_vals)

    def test_single_boundary(self):
        df = pl.DataFrame({"val": list(range(10))})
        result = assign_percentile_range_bucket(df, "val", [5])
        assert result["val_partition"].unique().sort().to_list() == [0, 1]


# ---------------------------------------------------------------------------
# 4.5  assign_uniform_range_bucket
# ---------------------------------------------------------------------------


class TestAssignUniformRangeBucket:
    """Equal-width buckets spanning [min_val, max_val]."""

    def test_basic_uniform(self):
        df = pl.DataFrame({"val": list(range(100))})
        result = assign_uniform_range_bucket(df, "val", 0, 99, 4)

        assert "val_partition" in result.columns
        partitions = result["val_partition"].unique().sort().to_list()
        assert partitions == [0, 1, 2, 3]

    def test_min_equals_max(self):
        df = pl.DataFrame({"val": [5, 5, 5]})
        result = assign_uniform_range_bucket(df, "val", 5, 5, 4)
        assert result["val_partition"].unique().to_list() == [0]

    def test_single_partition(self):
        df = pl.DataFrame({"val": list(range(10))})
        result = assign_uniform_range_bucket(df, "val", 0, 9, 1)
        assert result["val_partition"].unique().to_list() == [0]

    def test_boundary_values_clipped(self):
        # Values at the max boundary should get the last bucket, not overflow
        df = pl.DataFrame({"val": [0.0, 50.0, 100.0]})
        result = assign_uniform_range_bucket(df, "val", 0, 100, 2)
        partitions = result["val_partition"].to_list()
        assert partitions[0] == 0
        assert partitions[2] == 1  # 100.0 should be clipped to bucket 1


# ---------------------------------------------------------------------------
# assign_hash_bucket (supplementary — not in task list but included for
# completeness since the function was implemented)
# ---------------------------------------------------------------------------


class TestAssignHashBucket:
    """Hash bucketing: every row gets a bucket in [0, n_partitions)."""

    def test_all_buckets_assigned(self):
        # With enough distinct values, all buckets should be populated
        df = pl.DataFrame({"val": list(range(1000))})
        result = assign_hash_bucket(df, "val", 4)
        partitions = result["val_partition"].unique().sort().to_list()
        assert partitions == [0, 1, 2, 3]

    def test_single_partition(self):
        df = pl.DataFrame({"val": list(range(10))})
        result = assign_hash_bucket(df, "val", 1)
        assert result["val_partition"].unique().to_list() == [0]


# ---------------------------------------------------------------------------
# Cross-buffer partition consistency
# ---------------------------------------------------------------------------


class TestCrossBufferPartitionConsistency:
    """Verify that non-overlapping batches get consistent partitions
    when flushed separately with the same global boundaries/ranges."""

    def test_percentile_range_consistent_across_buffers(self):
        """Two non-overlapping value ranges should get distinct buckets
        when using the same global boundaries."""
        boundaries = [25, 50, 75]

        # Buffer 1: values 0–20 (all below first boundary → bucket 0)
        df1 = pl.DataFrame({"val": list(range(0, 21))})
        result1 = assign_percentile_range_bucket(df1, "val", boundaries)

        # Buffer 2: values 80–99 (all above last boundary → bucket 3)
        df2 = pl.DataFrame({"val": list(range(80, 100))})
        result2 = assign_percentile_range_bucket(df2, "val", boundaries)

        assert result1["val_partition"].unique().to_list() == [0]
        assert result2["val_partition"].unique().to_list() == [3]

    def test_uniform_range_consistent_across_buffers(self):
        """Two non-overlapping value ranges should get distinct buckets
        when using the same global (min, max)."""
        global_min, global_max, n = 0.0, 100.0, 4

        # Buffer 1: values 0–10 (all in first bin → bucket 0)
        df1 = pl.DataFrame({"val": list(range(0, 11))})
        result1 = assign_uniform_range_bucket(df1, "val", global_min, global_max, n)

        # Buffer 2: values 80–99 (all in last bin → bucket 3)
        df2 = pl.DataFrame({"val": list(range(80, 100))})
        result2 = assign_uniform_range_bucket(df2, "val", global_min, global_max, n)

        assert result1["val_partition"].unique().to_list() == [0]
        assert result2["val_partition"].unique().to_list() == [3]

    def test_uniform_range_per_buffer_would_fail(self):
        """With global ranges, a buffer spanning only the top quarter
        lands entirely in bucket 3 — per-buffer min/max would scatter
        across all 4 buckets."""
        global_min, global_max, n = 0.0, 100.0, 4

        df = pl.DataFrame({"val": list(range(76, 100))})
        result = assign_uniform_range_bucket(df, "val", global_min, global_max, n)
        assert result["val_partition"].unique().to_list() == [3]


# ---------------------------------------------------------------------------
# Cross-buffer partition consistency (_flush_buffer level)
# ---------------------------------------------------------------------------


class TestFlushBufferPartitionConsistency:
    """Verify that _flush_buffer produces consistent partitions across
    multiple flushes with non-overlapping data."""

    @patch("deltalake.write_deltalake")
    def test_percentile_range_across_flushes(self, mock_write):
        """Two flushes with non-overlapping values should land in
        distinct partitions when using shared global boundaries."""
        boundaries = {"val": [25, 50, 75]}
        spec = DeltaLakeOutputSpec(
            partition_by="val",
            partition_strategy="percentile_range",
            n_partitions=4,
        )

        batch1 = pa.RecordBatch.from_pydict({"val": list(range(0, 21))})
        batch2 = pa.RecordBatch.from_pydict({"val": list(range(80, 100))})

        _flush_buffer([batch1], [spec], "/tmp/test", [], [], boundaries, {})
        _flush_buffer([batch2], [spec], "/tmp/test", [], [], boundaries, {})

        assert mock_write.call_count == 2
        written1 = pl.from_arrow(mock_write.call_args_list[0][0][1])
        written2 = pl.from_arrow(mock_write.call_args_list[1][0][1])
        assert written1["val_partition"].unique().to_list() == [0]
        assert written2["val_partition"].unique().to_list() == [3]

    @patch("deltalake.write_deltalake")
    def test_uniform_range_across_flushes(self, mock_write):
        """Two flushes with non-overlapping values should land in
        distinct partitions when using shared global uniform ranges."""
        uniform_ranges = {"val": (0.0, 100.0)}
        spec = DeltaLakeOutputSpec(
            partition_by="val",
            partition_strategy="uniform_range",
            n_partitions=4,
        )

        batch1 = pa.RecordBatch.from_pydict({"val": list(range(0, 11))})
        batch2 = pa.RecordBatch.from_pydict({"val": list(range(80, 100))})

        _flush_buffer([batch1], [spec], "/tmp/test", [], [], {}, uniform_ranges)
        _flush_buffer([batch2], [spec], "/tmp/test", [], [], {}, uniform_ranges)

        assert mock_write.call_count == 2
        written1 = pl.from_arrow(mock_write.call_args_list[0][0][1])
        written2 = pl.from_arrow(mock_write.call_args_list[1][0][1])
        assert written1["val_partition"].unique().to_list() == [0]
        assert written2["val_partition"].unique().to_list() == [3]


# ===========================================================================
# Section 6 — Tests: Core Streaming Writer
# ===========================================================================


def _make_wkb_series(coords: list[tuple[float, float, float]], name: str) -> pl.Series:
    """Create a Polars Binary series of WKB-encoded 3D points."""
    points = [shapely.Point(x, y, z) for x, y, z in coords]
    wkb_bytes = [shapely.to_wkb(p) for p in points]
    return pl.Series(name, wkb_bytes, dtype=pl.Binary)


# ---------------------------------------------------------------------------
# 6.1  decode_geometry_columns
# ---------------------------------------------------------------------------


class TestDecodeGeometryColumns:
    """decode_geometry_columns should replace WKB binary columns with
    decoded x, y, z (and optionally Morton code) columns."""

    def test_basic_coordinate_extraction(self):
        coords = [(10, 20, 30), (40, 50, 60), (70, 80, 90)]
        geom = _make_wkb_series(coords, "pt")
        df = pl.DataFrame({"id": [1, 2, 3], "pt": geom})

        result = decode_geometry_columns(df, ["pt"])

        assert "pt" not in result.columns
        assert "pt_x" in result.columns
        assert "pt_y" in result.columns
        assert "pt_z" in result.columns
        assert "pt_morton" not in result.columns

        assert result["pt_x"].to_list() == [10, 40, 70]
        assert result["pt_y"].to_list() == [20, 50, 80]
        assert result["pt_z"].to_list() == [30, 60, 90]

    def test_morton_code_produced_when_requested(self):
        coords = [(100, 200, 300)]
        geom = _make_wkb_series(coords, "pt")
        df = pl.DataFrame({"pt": geom})

        result = decode_geometry_columns(df, ["pt"], morton_columns=["pt"])

        assert "pt_morton" in result.columns
        assert result["pt_morton"].dtype == pl.Int64
        # Verify Morton code matches manual computation
        expected = morton_encode_3d(
            np.array([100], dtype=np.uint64),
            np.array([200], dtype=np.uint64),
            np.array([300], dtype=np.uint64),
        )
        assert result["pt_morton"].to_list() == expected.tolist()

    def test_multiple_geometry_columns(self):
        coords_a = [(1, 2, 3)]
        coords_b = [(4, 5, 6)]
        df = pl.DataFrame(
            {
                "id": [1],
                "pre_pt": _make_wkb_series(coords_a, "pre_pt"),
                "post_pt": _make_wkb_series(coords_b, "post_pt"),
            }
        )

        result = decode_geometry_columns(
            df, ["pre_pt", "post_pt"], morton_columns=["pre_pt"]
        )

        # pre_pt decoded with Morton
        assert "pre_pt_x" in result.columns
        assert "pre_pt_morton" in result.columns
        # post_pt decoded without Morton
        assert "post_pt_x" in result.columns
        assert "post_pt_morton" not in result.columns
        # Originals dropped
        assert "pre_pt" not in result.columns
        assert "post_pt" not in result.columns

    def test_preserves_other_columns(self):
        coords = [(10, 20, 30)]
        geom = _make_wkb_series(coords, "pt")
        df = pl.DataFrame({"id": [42], "name": ["synapse"], "pt": geom})

        result = decode_geometry_columns(df, ["pt"])

        assert result["id"].to_list() == [42]
        assert result["name"].to_list() == ["synapse"]


# ---------------------------------------------------------------------------
# 6.4  Morton code locality
# ---------------------------------------------------------------------------


class TestMortonCodeLocality:
    """Morton codes should preserve 3D spatial locality: nearby points
    in 3D space should have nearby Morton codes."""

    def test_adjacent_points_have_close_codes(self):
        """Points differing by 1 in one axis should have closer Morton
        codes than points far apart."""
        base = np.array([1000], dtype=np.uint64)
        m_base = morton_encode_3d(base, base, base)[0]
        m_near = morton_encode_3d(base + 1, base, base)[0]
        m_far = morton_encode_3d(base + 1000, base, base)[0]

        assert abs(m_near - m_base) < abs(m_far - m_base)

    def test_bit_interleaving_pattern(self):
        """For small values, verify the exact bit-interleaving pattern.

        x=1, y=0, z=0 → bit 0 set → Morton code = 1
        x=0, y=1, z=0 → bit 1 set → Morton code = 2
        x=0, y=0, z=1 → bit 2 set → Morton code = 4
        """
        one = np.array([1], dtype=np.uint64)
        zero = np.array([0], dtype=np.uint64)

        assert morton_encode_3d(one, zero, zero)[0] == 1
        assert morton_encode_3d(zero, one, zero)[0] == 2
        assert morton_encode_3d(zero, zero, one)[0] == 4

    def test_origin_is_zero(self):
        zero = np.array([0], dtype=np.uint64)
        assert morton_encode_3d(zero, zero, zero)[0] == 0

    def test_vectorized_matches_scalar(self):
        """Batch encoding should match element-wise encoding."""
        xs = np.array([10, 100, 1000, 50000], dtype=np.uint64)
        ys = np.array([20, 200, 2000, 60000], dtype=np.uint64)
        zs = np.array([30, 300, 3000, 70000], dtype=np.uint64)

        batch_result = morton_encode_3d(xs, ys, zs)
        for i in range(len(xs)):
            scalar_result = morton_encode_3d(
                xs[i : i + 1], ys[i : i + 1], zs[i : i + 1]
            )
            assert batch_result[i] == scalar_result[0]

    def test_spatial_clustering(self):
        """Points in a tight 3D cluster should have a smaller Morton code
        range than points spread across the volume."""
        rng = np.random.default_rng(42)

        # Tight cluster: 100 points in a 10×10×10 box
        tight_x = rng.integers(500, 510, size=100).astype(np.uint64)
        tight_y = rng.integers(500, 510, size=100).astype(np.uint64)
        tight_z = rng.integers(500, 510, size=100).astype(np.uint64)
        tight_codes = morton_encode_3d(tight_x, tight_y, tight_z)
        tight_range = tight_codes.max() - tight_codes.min()

        # Spread: 100 points in a 1000×1000×1000 volume
        spread_x = rng.integers(0, 1000, size=100).astype(np.uint64)
        spread_y = rng.integers(0, 1000, size=100).astype(np.uint64)
        spread_z = rng.integers(0, 1000, size=100).astype(np.uint64)
        spread_codes = morton_encode_3d(spread_x, spread_y, spread_z)
        spread_range = spread_codes.max() - spread_codes.min()

        assert tight_range < spread_range


# ---------------------------------------------------------------------------
# 6.2 / 6.3  End-to-end export with mocked ADBC
# ---------------------------------------------------------------------------


class TestExportTableToDeltalake:
    """End-to-end test of export_table_to_deltalake with mocked DB."""

    @patch("deltalake.DeltaTable")
    @patch("deltalake.write_deltalake")
    @patch("materializationengine.workflows.deltalake_export.stream_table_to_arrow")
    def test_end_to_end_export(self, mock_stream, mock_write, _mock_dt):
        """Full pipeline: stream → buffer → flush → write."""
        # Simulate two small Arrow batches
        batch1 = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3],
                "root_id": [100, 200, 300],
            }
        )
        batch2 = pa.RecordBatch.from_pydict(
            {
                "id": [4, 5, 6],
                "root_id": [400, 500, 600],
            }
        )
        mock_stream.return_value = iter([batch1, batch2])

        spec = DeltaLakeOutputSpec(
            partition_by="root_id",
            partition_strategy="percentile_range",
            n_partitions=2,
        )
        boundaries = {"root_id": [350]}

        export_table_to_deltalake(
            connection_string="unused",
            annotation_table_name="synapse",
            output_specs=[spec],
            output_uri_base="gs://bucket/test",
            boundaries=boundaries,
            flush_threshold_bytes=10 * 1024 * 1024 * 1024,  # huge → single flush
        )

        # Should have flushed once with all 6 rows
        assert mock_write.call_count == 1
        written = pl.from_arrow(mock_write.call_args_list[0][0][1])
        assert len(written) == 6
        assert "root_id_partition" in written.columns
        # Values 100,200,300 ≤ 350 → bucket 0; 400,500,600 > 350 → bucket 1
        partitions = written.sort("id")["root_id_partition"].to_list()
        assert partitions == [0, 0, 0, 1, 1, 1]

    @patch("deltalake.DeltaTable")
    @patch("deltalake.write_deltalake")
    @patch("materializationengine.workflows.deltalake_export.stream_table_to_arrow")
    def test_explicit_specs_override_defaults(self, mock_stream, mock_write, _mock_dt):
        """Passing explicit output_specs should use those, not index-derived ones."""
        batch = pa.RecordBatch.from_pydict(
            {
                "id": [1, 2, 3, 4],
                "root_id": [10, 20, 30, 40],
            }
        )
        mock_stream.return_value = iter([batch])

        # Explicit spec: hash partition with 2 buckets (not percentile_range)
        spec = DeltaLakeOutputSpec(
            partition_by="root_id",
            partition_strategy="hash",
            n_partitions=2,
        )

        export_table_to_deltalake(
            connection_string="unused",
            annotation_table_name="test_table",
            output_specs=[spec],
            output_uri_base="gs://bucket/test",
            flush_threshold_bytes=10 * 1024 * 1024 * 1024,
        )

        assert mock_write.call_count == 1
        written = pl.from_arrow(mock_write.call_args_list[0][0][1])
        assert "root_id_partition" in written.columns
        # Hash produces buckets 0 or 1
        assert set(written["root_id_partition"].to_list()).issubset({0, 1})

    @patch("deltalake.DeltaTable")
    @patch("deltalake.write_deltalake")
    @patch("materializationengine.workflows.deltalake_export.stream_table_to_arrow")
    def test_multiple_flushes(self, mock_stream, mock_write, _mock_dt):
        """With a low flush threshold, data should be written in multiple flushes."""
        batch1 = pa.RecordBatch.from_pydict({"val": list(range(0, 50))})
        batch2 = pa.RecordBatch.from_pydict({"val": list(range(50, 100))})
        mock_stream.return_value = iter([batch1, batch2])

        spec = DeltaLakeOutputSpec(
            partition_by="val",
            partition_strategy="percentile_range",
            n_partitions=2,
        )

        export_table_to_deltalake(
            connection_string="unused",
            annotation_table_name="t",
            output_specs=[spec],
            output_uri_base="/tmp/test",
            boundaries={"val": [50]},
            flush_threshold_bytes=1,  # tiny → flush after every batch
        )

        # Two batches, each exceeds 1-byte threshold → 2 flushes
        assert mock_write.call_count == 2


# ===========================================================================
# Section 7 — Tests: Delta Lake Optimization
# ===========================================================================


class TestOptimizeDeltalake:
    """optimize_deltalake should z-order, apply bloom filters, and vacuum."""

    @patch("deltalake.DeltaTable")
    def test_z_order_with_bloom_filters(self, MockDeltaTable):
        """With both z-order and bloom filter columns, should call
        z_order with WriterProperties and then vacuum."""
        mock_dt = MagicMock()
        MockDeltaTable.return_value = mock_dt

        optimize_deltalake(
            "gs://bucket/test/root_id",
            zorder_columns=["root_id"],
            bloom_filter_columns=["id", "root_id"],
            fpp=0.01,
        )

        mock_dt.optimize.z_order.assert_called_once()
        call_kwargs = mock_dt.optimize.z_order.call_args
        assert call_kwargs[1]["columns"] == ["root_id"]
        assert call_kwargs[1]["writer_properties"] is not None
        mock_dt.vacuum.assert_called_once_with(
            dry_run=False,
            retention_hours=0,
            enforce_retention_duration=False,
            full=True,
        )

    @patch("deltalake.DeltaTable")
    def test_z_order_without_bloom(self, MockDeltaTable):
        """With z-order columns but no bloom filters, writer_properties
        should be None."""
        mock_dt = MagicMock()
        MockDeltaTable.return_value = mock_dt

        optimize_deltalake(
            "/tmp/test",
            zorder_columns=["col_a", "col_b"],
        )

        call_kwargs = mock_dt.optimize.z_order.call_args
        assert call_kwargs[1]["writer_properties"] is None
        mock_dt.optimize.compact.assert_not_called()
        mock_dt.vacuum.assert_called_once()

    @patch("deltalake.DeltaTable")
    def test_compact_when_no_zorder_columns(self, MockDeltaTable):
        """Without z-order columns, should call compact instead."""
        mock_dt = MagicMock()
        MockDeltaTable.return_value = mock_dt

        optimize_deltalake("/tmp/test")

        mock_dt.optimize.compact.assert_called_once()
        mock_dt.optimize.z_order.assert_not_called()
        mock_dt.vacuum.assert_called_once()

    @patch("deltalake.write_deltalake")
    @patch("deltalake.DeltaTable")
    @patch("materializationengine.workflows.deltalake_export.stream_table_to_arrow")
    def test_export_calls_optimize(self, mock_stream, MockDeltaTable, mock_write):
        """export_table_to_deltalake should call optimize_deltalake for
        each output spec after writing."""
        mock_dt = MagicMock()
        MockDeltaTable.return_value = mock_dt

        batch = pa.RecordBatch.from_pydict({"id": [1, 2], "val": [10, 20]})
        mock_stream.return_value = iter([batch])

        spec = DeltaLakeOutputSpec(
            partition_by="val",
            partition_strategy="hash",
            n_partitions=2,
            zorder_columns=["val"],
            bloom_filter_columns=["id"],
        )

        export_table_to_deltalake(
            connection_string="unused",
            annotation_table_name="t",
            output_specs=[spec],
            output_uri_base="/tmp/test",
        )

        # Should have opened the DeltaTable at the correct URI
        MockDeltaTable.assert_called_once_with("/tmp/test/val")
        # Should have called z_order (not compact) since zorder_columns is set
        mock_dt.optimize.z_order.assert_called_once()
        mock_dt.vacuum.assert_called_once()
