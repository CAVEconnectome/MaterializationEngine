"""lake_export.optimize: a few partitions per task, each in its own commit, deleting the
files it replaced -- on small local lakes."""

from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from materializationengine.workflows import deltalake_export as dx
from materializationengine.workflows.lake_export import optimize as opt
from materializationengine.workflows.lake_export import storage
from materializationengine.workflows.lake_export.sizing import SizingConfig, plan_optimize

SPEC = dx.DeltaLakeOutputSpec(name="root", partition_by="root", partition_strategy="percentile_range",
                              zorder_columns=["root"], max_row_group_size=100)


def make_lake(path, flushes=6, rows=500, partitioned=True, nulls=True):
    """A lake written as many small appends, like a streamed export before optimize."""
    from deltalake import write_deltalake

    rng = np.random.default_rng(0)
    for _ in range(flushes):
        root = rng.integers(0, 8000, rows)
        part = (root // 1000).astype("int32")
        mask = np.zeros(rows, bool)
        if nulls:
            mask[:20] = True
        table = pa.table({"root": pa.array(root, mask=mask),
                          "x": pa.array(rng.random(rows)),
                          "root_partition": pa.array(part, mask=mask)})
        write_deltalake(path, table, mode="append", partition_by=["root_partition"] if partitioned else None)
    return path


def files_by_partition(path):
    return {s.value: s.files for s in opt.partition_stats(path, "root_partition")}


class TestPartitionStats:
    def test_counts_files_and_bytes(self, tmp_path):
        path = make_lake(str(tmp_path / "lake"))
        stats = {s.value: s for s in opt.partition_stats(path, "root_partition")}
        assert set(stats) == {None} | {str(i) for i in range(8)}
        assert all(s.files == 6 and s.bytes > 0 for s in stats.values())

    def test_unpartitioned(self, tmp_path):
        path = make_lake(str(tmp_path / "lake"), partitioned=False, nulls=False)
        (stat,) = opt.partition_stats(path, None)
        assert stat.value is None and stat.files == 6


class TestOptimizePartitions:
    def test_only_its_partitions_and_deletes_what_it_replaced(self, tmp_path):
        from deltalake import DeltaTable

        path = make_lake(str(tmp_path / "lake"))
        on_disk = len([p for p in storage.list_paths(path) if p.endswith(".parquet")])
        result = opt.optimize_partitions(path, SPEC, "root_partition", ["2", "5"])
        files = files_by_partition(path)
        assert files["2"] == files["5"] == 1
        assert all(n == 6 for v, n in files.items() if v not in ("2", "5"))
        assert result["deleted"] == 12 and result["files_removed"] == 12
        assert len([p for p in storage.list_paths(path) if p.endswith(".parquet")]) == on_disk - 12 + 2
        assert DeltaTable(path).to_pyarrow_table().num_rows == 6 * 500

    def test_null_partition(self, tmp_path):
        path = make_lake(str(tmp_path / "lake"))
        opt.optimize_partitions(path, SPEC, "root_partition", [None])
        assert files_by_partition(path)[None] == 1
        with pytest.raises(ValueError, match="null partition"):
            opt.optimize_partitions(path, SPEC, "root_partition", [None, "1"])

    def test_writer_settings_and_order(self, tmp_path):
        from deltalake import DeltaTable

        path = make_lake(str(tmp_path / "lake"), rows=2000)
        opt.optimize_partitions(path, SPEC, "root_partition", ["3"])
        (uri,) = [u for u in DeltaTable(path).file_uris() if "root_partition=3" in u]
        meta = pq.ParquetFile(uri).metadata
        assert meta.num_row_groups > 1
        assert max(meta.row_group(i).num_rows for i in range(meta.num_row_groups)) <= 100
        roots = pq.read_table(uri)["root"].to_numpy()
        assert (np.diff(roots) >= 0).all()  # z-order on one column is a sort

    def test_concurrent_tasks_of_one_lake(self, tmp_path):
        from deltalake import DeltaTable

        path = make_lake(str(tmp_path / "lake"))
        tasks = plan_optimize(opt.partition_stats(path, "root_partition"), partitioned=True,
                              cfg=SizingConfig(optimize_group_bytes=1, whole_lake_max_bytes=0))
        assert len(tasks) == 9  # every partition alone, the null one included
        with ThreadPoolExecutor(len(tasks)) as pool:
            list(pool.map(lambda t: opt.optimize_partitions(path, SPEC, "root_partition", list(t.values)), tasks))
        assert set(files_by_partition(path).values()) == {1}
        assert DeltaTable(path).to_pyarrow_table().num_rows == 6 * 500
        assert len([p for p in storage.list_paths(path) if p.endswith(".parquet")]) == 9

    def test_whole_lake(self, tmp_path):
        from deltalake import DeltaTable

        path = make_lake(str(tmp_path / "lake"), partitioned=False, nulls=False)
        spec = dx.DeltaLakeOutputSpec(name="flat")  # no z-order: compacted
        result = opt.optimize_partitions(path, spec, None, None)
        assert len(DeltaTable(path).file_uris()) == 1 and result["deleted"] == 6

    def test_sweep_removes_unreferenced_files(self, tmp_path):
        path = make_lake(str(tmp_path / "lake"))
        storage_path = tmp_path / "lake" / "staging" / "r00000-dead" / "root_partition=1"
        storage_path.mkdir(parents=True)
        (storage_path / "part-0.parquet").write_bytes(b"orphan")
        opt.sweep(path)
        assert storage.list_paths(path, "staging") == []
