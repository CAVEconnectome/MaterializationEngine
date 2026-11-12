"""lake_export.sizing: how a table's export is divided, from its size (pure)."""

import pytest

from materializationengine.workflows.lake_export.sizing import (
    GiB,
    MiB,
    PartitionStat,
    SizingConfig,
    max_flush_bytes,
    plan_optimize,
    size_table,
)


class TestSizeTable:
    def test_small_table_keeps_one_range(self):
        sizing = size_table(1_000_000, 200)  # ~190 MiB
        assert sizing.ranges == 1 and not sizing.split
        # one flush for the whole table when it fits
        assert sizing.flush_bytes >= sizing.estimated_bytes

    def test_split_starts_at_split_min_bytes(self):
        cfg = SizingConfig()
        just_under = size_table(cfg.split_min_bytes - 1, 1, cfg)
        at = size_table(cfg.split_min_bytes, 1, cfg)
        assert just_under.ranges == 1
        assert at.ranges == 2

    def test_synapse_table_spreads(self):
        # minnie's synapses_pni_2: 337M rows; ~200 Postgres bytes per row
        sizing = size_table(337_312_429, 200)
        assert sizing.ranges == -(-337_312_429 * 200 // GiB)
        assert sizing.flush_bytes == max_flush_bytes(SizingConfig())

    def test_flush_is_capped_by_the_memory_budget(self):
        cfg = SizingConfig(worker_memory_budget=9 * GiB, streams_per_worker=2)
        # (9 GiB - 2 x 0.5 GiB) / (2 streams x 5) = 819 MiB -> 768 MiB in 64 MiB steps
        assert max_flush_bytes(cfg) == 768 * MiB
        assert max_flush_bytes(SizingConfig(worker_memory_budget=GiB, streams_per_worker=4)) == cfg.min_flush_bytes

    def test_flush_fits_one_range_when_it_can(self):
        cfg = SizingConfig(range_bytes=256 * MiB, split_min_bytes=512 * MiB)
        sizing = size_table(10 * GiB // 100, 100, cfg)
        assert sizing.ranges == 40
        # a range's bytes plus a quarter, rounded up to 64 MiB
        assert sizing.flush_bytes == 320 * MiB

    def test_zero_or_unknown_rows(self):
        assert size_table(0, 200).ranges == 1
        assert size_table(None, None).ranges == 1

    def test_from_config(self):
        cfg = SizingConfig.from_config({"DELTALAKE_EXPORT_RANGE_BYTES": str(GiB // 2),
                                        "DELTALAKE_EXPORT_STREAMS_PER_WORKER": 3,
                                        "DELTALAKE_EXPORT_SPLIT_MIN_BYTES": ""})
        assert cfg.range_bytes == GiB // 2 and cfg.streams_per_worker == 3
        assert cfg.split_min_bytes == SizingConfig().split_min_bytes


class TestPlanOptimize:
    cfg = SizingConfig(optimize_group_bytes=100, whole_lake_max_bytes=50)

    def test_unpartitioned_or_small_lake_is_one_task(self):
        stats = [PartitionStat(None, 3, 400)]
        assert [t.values for t in plan_optimize(stats, partitioned=False, cfg=self.cfg)] == [None]
        small = [PartitionStat("0", 2, 20), PartitionStat("1", 2, 20)]
        assert [t.values for t in plan_optimize(small, partitioned=True, cfg=self.cfg)] == [None]
        assert plan_optimize([], partitioned=True, cfg=self.cfg) == []

    def test_groups_by_bytes_largest_first(self):
        stats = [PartitionStat(str(i), 2, b) for i, b in enumerate([60, 30, 30, 150, 10, 40])]
        tasks = plan_optimize(stats, partitioned=True, cfg=self.cfg)
        # every partition exactly once
        assert sorted(v for t in tasks for v in t.values) == sorted(s.value for s in stats)
        # a partition over the group size is a task alone; the rest pack to <= 100
        assert tasks[0].values == ("3",) and tasks[0].bytes == 150
        assert all(t.bytes <= 100 for t in tasks[1:])
        assert [t.bytes for t in tasks] == sorted((t.bytes for t in tasks), reverse=True)

    def test_null_partition_is_a_task_alone(self):
        stats = [PartitionStat(None, 5, 10), PartitionStat("0", 1, 30), PartitionStat("1", 1, 30)]
        tasks = plan_optimize(stats, partitioned=True, cfg=self.cfg)
        assert (None,) in [t.values for t in tasks]
        assert all(None not in t.values for t in tasks if t.values != (None,))


class TestMemoryBudget:
    @pytest.mark.parametrize("budget,streams", [(9 * GiB, 2), (11 * GiB, 1), (6 * GiB, 4)])
    def test_budget_holds_at_the_measured_peak(self, budget, streams):
        cfg = SizingConfig(worker_memory_budget=budget, streams_per_worker=streams)
        peak = streams * (cfg.process_base_bytes + cfg.stream_peak_factor * max_flush_bytes(cfg))
        assert peak <= budget or max_flush_bytes(cfg) == cfg.min_flush_bytes
