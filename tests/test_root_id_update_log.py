"""The root ID update log: one append-only Delta table per annotation table, one commit per run."""

import json
import shutil
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest
from deltalake import DeltaTable

from materializationengine.workflows import root_id_update_log as log

RUN_TS = "2026-10-07 15:01:02.123456"
RUN_ID = "20261007T150102.123456Z"
LATER_TS = "2026-10-07 16:01:02.000000"
LATER_ID = "20261007T160102.000000Z"
# celery task ids, which name the chunk files
TASK = {k: f"{i:08x}-0000-4000-8000-000000000000" for i, k in enumerate("abcd")}


def metadata(table="synapses", **extra):
    return {
        "datastack": "minnie65_phase3_v1",
        "aligned_volume": "minnie65_phase3",
        "annotation_table_name": table,
        "segmentation_table_name": f"{table}__minnie3_v1",
        "pcg_table_name": "minnie3_v1",
        "materialization_time_stamp": RUN_TS,
        "last_updated_time_stamp": "2026-10-07 14:01:00.000000",
        **extra,
    }


@pytest.fixture
def bucket(tmp_path):
    config = {"MATERIALIZATION_DUMP_BUCKET": str(tmp_path), "ROOT_ID_UPDATE_LOG": True}
    with mock.patch.object(log, "get_config_param", side_effect=lambda k, d=None: config.get(k, d)):
        yield tmp_path, config


def chunk(ids, old, new, side="pre_pt"):
    """A get_new_root_ids frame after the lookup, and the roots it had before."""
    df = pd.DataFrame(
        {"id": ids, f"{side}_root_id": new, f"{side}_supervoxel_id": [i * 10 for i in ids]}, dtype=object
    )
    return df, pd.Series(old, dtype=object)


def write(md, ids, task, side="pre_pt"):
    df, old = chunk(ids, [i * 10 for i in ids], [i * 10 + 1 for i in ids], side)
    log.record_chunk(md, df, old, f"{side}_root_id", f"{side}_supervoxel_id", task)


def table_dir(tmp_path, table="synapses"):
    return tmp_path / "root_id_updates" / "minnie65_phase3_v1" / table


def staging_dir(tmp_path, run=RUN_ID, table="synapses"):
    return tmp_path / "root_id_updates" / "_staging" / "minnie65_phase3_v1" / table / run


class TestPaths:
    def test_one_table_per_annotation_table_and_staging_outside_it(self, bucket):
        tmp_path, _ = bucket
        assert log.table_uri(metadata()) == str(table_dir(tmp_path))
        assert log.staging_uri(metadata()) == str(staging_dir(tmp_path))
        # the ISO form of the same timestamp names the same run
        assert log.staging_uri(metadata(materialization_time_stamp="2026-10-07T15:01:02.123456")) == str(staging_dir(tmp_path))
        assert not str(staging_dir(tmp_path)).startswith(str(table_dir(tmp_path)))

    def test_no_bucket_no_paths(self, bucket):
        _, config = bucket
        del config["MATERIALIZATION_DUMP_BUCKET"]
        assert log.table_uri(metadata()) is None and log.staging_uri(metadata()) is None

    @pytest.mark.parametrize("value, expected", [(True, True), (False, False), ("True", True), ("false", False), ("1", True), (None, False)])
    def test_enabled_reads_bools_and_env_strings(self, bucket, value, expected):
        _, config = bucket
        config["ROOT_ID_UPDATE_LOG"] = value
        assert log.enabled(metadata()) is expected

    @pytest.mark.parametrize("value", [
        {"minnie65_phase3_v1": ["synapses"]},
        {"minnie65_phase3_v1": "synapses"},
        '{"minnie65_phase3_v1": ["synapses"]}',  # from the environment
    ])
    def test_enabled_for_chosen_tables_of_chosen_datastacks(self, bucket, value):
        _, config = bucket
        config["ROOT_ID_UPDATE_LOG"] = value
        assert log.enabled(metadata("synapses"))
        assert not log.enabled(metadata("cells"))
        assert not log.enabled({**metadata("synapses"), "datastack": "zheng_ca3"})

    @pytest.mark.parametrize("tables", ["*", ["*"]])
    def test_star_means_every_table_of_that_datastack(self, bucket, tables):
        _, config = bucket
        config["ROOT_ID_UPDATE_LOG"] = {"minnie65_phase3_v1": tables}
        assert log.enabled(metadata("synapses")) and log.enabled(metadata("cells"))
        assert not log.enabled({**metadata("synapses"), "datastack": "zheng_ca3"})

    def test_bad_json_is_off(self, bucket):
        _, config = bucket
        config["ROOT_ID_UPDATE_LOG"] = '{"minnie65_phase3_v1": [synapses]}'
        assert not log.enabled(metadata())


class TestRows:
    def test_rows_hold_only_what_varies_with_the_old_root_from_before_the_lookup(self):
        df, old = chunk([1, 2, 3], [100, None, 300], [101, 202, 301])
        table = log.updates_frame(df, old, "pre_pt_root_id", "pre_pt_supervoxel_id")
        assert table.schema == log.SCHEMA
        assert table.column_names == ["id", "root_column", "supervoxel_id", "old_root_id", "new_root_id"]
        assert table.to_pylist()[1] == {"id": 2, "root_column": "pre_pt", "supervoxel_id": 20, "old_root_id": None, "new_root_id": 202}


class TestRuns:
    def test_each_run_is_one_commit_in_its_partition_with_its_details(self, bucket):
        tmp_path, _ = bucket
        md = metadata(lookup_all_root_ids=False)
        log.start(md)
        assert json.loads((staging_dir(tmp_path) / "_run.json").read_text())["state"] == "running"
        write(md, [1, 2], TASK["a"])
        write(md, [1, 2], TASK["b"], side="post_pt")
        write(md, [2], TASK["c"])  # a retried batch under a new task id: duplicate rows
        log.finalize(md)

        table = DeltaTable(str(table_dir(tmp_path)))
        rows = table.to_pyarrow_table()
        assert rows.num_rows == 4  # 5 written, 1 exact duplicate removed
        assert set(rows.column("run_id").to_pylist()) == {RUN_ID}
        assert {t.isoformat() for t in rows.column("end_time_stamp").to_pylist()} == {"2026-10-07T15:01:02.123456+00:00"}
        assert all(p.startswith(f"run_id={RUN_ID}/") for p in table.get_add_actions(flatten=True).column("path").to_pylist())
        (run,) = log.runs(str(table_dir(tmp_path)))
        assert run["run_id"] == RUN_ID and run["rows"] == "4" and run["chunk_files"] == "3"
        assert run["duplicate_rows_removed"] == "1" and run["lookup_all_root_ids"] == "false"
        assert run["start_time_stamp"].startswith("2026-10-07T14:01:00") and run["end_time_stamp"].startswith("2026-10-07T15:01:02.123456")
        assert run["segmentation_table"] == "synapses__minnie3_v1"
        assert not staging_dir(tmp_path).exists()

    def test_finalize_again_adds_no_commit(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        write(md, [1], TASK["a"])
        log.finalize(md)
        write(md, [1], TASK["a"])  # the same run's chunk shows up again
        log.finalize(md)
        assert len(log.runs(str(table_dir(tmp_path)))) == 1
        assert DeltaTable(str(table_dir(tmp_path))).to_pyarrow_table().num_rows == 1

    def test_a_run_without_updates_makes_no_commit(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        log.start(md)
        log.finalize(md)
        assert not table_dir(tmp_path).exists() and not staging_dir(tmp_path).exists()

    def test_the_table_is_append_only_and_keeps_its_log(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        write(md, [1], TASK["a"])
        log.finalize(md)
        table = DeltaTable(str(table_dir(tmp_path)))
        assert table.metadata().configuration["delta.appendOnly"] == "true"
        assert table.metadata().configuration["delta.enableExpiredLogCleanup"] == "false"
        with pytest.raises(Exception, match="append-only"):
            table.delete("id = 1")

    def test_a_mirror_needs_only_the_new_commits_files(self, bucket, tmp_path):
        tmp, _ = bucket
        first, second = metadata(), metadata(materialization_time_stamp=LATER_TS, last_updated_time_stamp=RUN_TS)
        write(first, [1, 2], TASK["a"])
        log.finalize(first)
        mirror = tmp_path / "mirror"
        shutil.copytree(table_dir(tmp), mirror)
        version = DeltaTable(str(mirror)).version()

        write(second, [3], TASK["b"])
        log.finalize(second)
        new = log.added_files(str(table_dir(tmp)), after_version=version)
        assert new and all(p.startswith(f"run_id={LATER_ID}/") for p in new)
        # bring the mirror up to date with just those files and the new log entries
        for p in new:
            (mirror / p).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(table_dir(tmp) / p, mirror / p)
        for entry in (table_dir(tmp) / "_delta_log").iterdir():
            if not (mirror / "_delta_log" / entry.name).exists():
                shutil.copy(entry, mirror / "_delta_log" / entry.name)
        assert sorted(DeltaTable(str(mirror)).to_pyarrow_table().column("id").to_pylist()) == [1, 2, 3]
        assert [r["run_id"] for r in log.runs(str(mirror))] == [RUN_ID, LATER_ID]
        # windows chain: the second run starts where the first ended
        runs = log.runs(str(mirror))
        assert runs[1]["start_time_stamp"] == runs[0]["end_time_stamp"]

    def test_a_large_run_is_streamed_into_one_commit(self, bucket, monkeypatch):
        tmp_path, _ = bucket
        monkeypatch.setattr(log, "STREAM_GROUP_FILES", 2)
        md = metadata()
        for i, t in enumerate("abcd"):
            write(md, [i * 10 + 1, i * 10 + 2], TASK[t])
        log.finalize(md)
        (run,) = log.runs(str(table_dir(tmp_path)))
        assert run["rows"] == "8" and run["duplicate_rows_removed"] == "" and run["chunk_files"] == "4"
        assert DeltaTable(str(table_dir(tmp_path))).to_pyarrow_table().num_rows == 8

    def test_failures_are_logged_not_raised(self, bucket):
        md = metadata()
        with mock.patch.object(log.pq, "write_table", side_effect=OSError("bucket down")), \
                mock.patch.object(log.celery_logger, "warning") as warning:
            assert log.record_chunk(md, *chunk([1], [10], [11]), "pre_pt_root_id", "pre_pt_supervoxel_id", TASK["a"]) is None
        assert "bucket down" in warning.call_args[0][0]
        with mock.patch.object(log, "updates_frame", side_effect=ValueError("bad row")):
            assert log.record_chunk(md, *chunk([1], [10], [11]), "pre_pt_root_id", "pre_pt_supervoxel_id", TASK["a"]) is None
        with mock.patch.object(log, "_filesystem", side_effect=OSError("bucket down")):
            log.start(md)
            log.finalize(md)


class TestMigrateRunsToTable:
    def per_run_folder(self, tmp_path, run, ts, rows, dups=0):
        """A run as the previous layout stored it: {table}/{run_id}/ Delta table + _run.json."""
        from deltalake import write_deltalake

        path = table_dir(tmp_path) / run
        write_deltalake(str(path), pa.Table.from_pylist(rows, schema=log.SCHEMA))
        (path / "_run.json").write_text(json.dumps({
            "state": "done", "rows": len(rows), "materialization_time_stamp": ts,
            "last_updated_time_stamp": "2026-10-07 14:01:00.000000", "duplicate_rows_removed": dups,
            "datastack": "minnie65_phase3_v1", "annotation_table": "synapses"}))
        return path

    def test_one_commit_per_run_oldest_first_idempotent_and_sources_kept(self, tmp_path):
        row = lambda i: {"id": i, "root_column": "pre_pt", "supervoxel_id": i, "old_root_id": 10, "new_root_id": 11}
        later = self.per_run_folder(tmp_path, LATER_ID, LATER_TS, [row(3)])
        first = self.per_run_folder(tmp_path, RUN_ID, RUN_TS, [row(1), row(2)], dups=5)
        uri = str(table_dir(tmp_path))

        plan = log.migrate_runs_to_table(uri)
        assert [(p["run_id"], p["result"]) for p in plan] == [(RUN_ID, "would commit"), (LATER_ID, "would commit")]
        assert not DeltaTable.is_deltatable(uri)

        report = log.migrate_runs_to_table(uri, dry_run=False)
        assert [p["result"] for p in report] == ["committed", "committed"]
        runs = log.runs(uri)
        assert [(r["run_id"], r["rows"], r["duplicate_rows_removed"]) for r in runs] == [(RUN_ID, "2", "5"), (LATER_ID, "1", "0")]
        assert DeltaTable(uri).to_pyarrow_table().num_rows == 3
        assert first.exists() and later.exists()  # the previous layout is left in place

        again = log.migrate_runs_to_table(uri, dry_run=False)
        assert {p["result"] for p in again} == {"already committed"} and len(log.runs(uri)) == 2
