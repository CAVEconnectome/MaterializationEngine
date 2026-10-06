"""The grid strategy sizes its chunks for about target_rows_per_chunk rows each."""

from types import SimpleNamespace
from unittest import mock

import numpy as np
import pytest

from materializationengine.workflows.chunking import (
    DEFAULT_TARGET_ROWS_PER_CHUNK,
    ChunkingStrategy,
)


def _strategy(rows, spans, base_chunk_size=2048, target=DEFAULT_TARGET_ROWS_PER_CHUNK):
    strategy = ChunkingStrategy(
        engine=None,
        table_name="synapses",
        database="staging",
        base_chunk_size=base_chunk_size,
        target_rows_per_chunk=target,
    )
    strategy.min_coords = np.array([0.0, 0.0, 0.0])
    strategy.max_coords = np.array(spans, dtype=float)
    strategy.estimated_rows = rows
    return strategy


class TestGridChunkSizeForTargetRows:
    def test_test7_sized_table_gets_about_target_rows_per_chunk(self):
        # ltv7 test7: 1.02M rows in a box that a fixed 2048 grid cut into 150,332 chunks
        spans = [2048 * 106, 2048 * 74, 2048 * 19.2]
        strategy = _strategy(1_020_000, spans)
        assert strategy.select_strategy() == "grid"

        assert 1_020_000 / strategy.total_chunks == pytest.approx(500, rel=0.15)
        assert strategy.actual_chunk_size > 4 * 2048

    def test_bigger_table_in_the_same_box_gets_more_chunks(self):
        spans = [2048 * 106, 2048 * 74, 2048 * 19.2]
        # A smaller minimum, so it does not cap the 100M-row table's chunk size
        small, large = _strategy(1_020_000, spans, 1024), _strategy(100_000_000, spans, 1024)
        small.select_strategy(), large.select_strategy()

        assert large.total_chunks > 50 * small.total_chunks
        assert 100_000_000 / large.total_chunks == pytest.approx(500, rel=0.15)

    def test_chunk_size_never_below_base(self):
        # Dense table: the target would ask for chunks smaller than the minimum
        strategy = _strategy(5_000_000, [4096, 4096, 4096], base_chunk_size=2048)
        strategy.select_strategy()
        assert strategy.actual_chunk_size == 2048
        assert strategy.total_chunks == 8

    @pytest.mark.parametrize("rows", [0, None], ids=["zero", "unknown"])
    def test_unknown_row_count_keeps_base_size(self, rows):
        strategy = _strategy(rows, [2048 * 10] * 3)
        assert strategy._grid_chunk_size_for_target_rows() == 2048

    def test_flat_bounding_box_does_not_divide_by_zero(self):
        strategy = _strategy(2_000_000, [2048 * 100, 2048 * 100, 0])
        assert strategy._grid_chunk_size_for_target_rows() >= 2048


class TestEstimateRowCount:
    def _engine(self, reltuples, exact):
        connection = mock.MagicMock()
        connection.execute.side_effect = [
            mock.MagicMock(fetchone=lambda: SimpleNamespace(est_rows=reltuples, table_size_bytes=0)),
            mock.MagicMock(scalar=lambda: exact),
        ]
        engine = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = connection
        return engine, connection

    def test_never_analyzed_table_is_counted(self):
        # reltuples is -1 until the first VACUUM/ANALYZE
        engine, connection = self._engine(reltuples=-1, exact=1_020_000)
        strategy = ChunkingStrategy(engine, "synapses", "staging")
        assert strategy.estimate_row_count() == 1_020_000
        assert "count(*)" in str(connection.execute.call_args_list[1].args[0])

    def test_analyzed_table_uses_the_estimate(self):
        engine, connection = self._engine(reltuples=1_000_500, exact=None)
        strategy = ChunkingStrategy(engine, "synapses", "staging")
        assert strategy.estimate_row_count() == 1_000_500
        assert connection.execute.call_count == 1
