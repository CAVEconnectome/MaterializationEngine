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


def manifest_rows(tmp_path, table="synapses"):
    return log.manifest(str(table_dir(tmp_path, table))).to_pylist()


class TestRuns:
    def test_each_run_is_one_commit_with_lean_rows_and_a_manifest_row(self, bucket):
        tmp_path, _ = bucket
        md = metadata(lookup_all_root_ids=False)
        log.start(md)
        assert json.loads((staging_dir(tmp_path) / "_run.json").read_text())["state"] == "running"
        write(md, [1, 2], TASK["a"])
        write(md, [1, 2], TASK["b"], side="post_pt")
        write(md, [2], TASK["c"])  # a retried batch under a new task id: duplicate rows
        log.finalize(md, succeeded=True)

        table = DeltaTable(str(table_dir(tmp_path)))
        rows = table.to_pyarrow_table()
        assert rows.num_rows == 4  # 5 written, 1 exact duplicate removed
        assert rows.column_names == ["id", "root_column", "supervoxel_id", "old_root_id", "new_root_id", "run_id"]
        assert set(rows.column("run_id").to_pylist()) == {RUN_ID}
        assert all(p.startswith(f"run_id={RUN_ID}/") for p in table.get_add_actions(flatten=True).column("path").to_pylist())
        (row,) = manifest_rows(tmp_path)
        assert row["run_id"] == RUN_ID and row["rows"] == 4 and row["chunk_files"] == 3
        assert row["duplicate_rows_removed"] == 1 and row["table_version"] == 0 and row["succeeded"] is True
        assert row["start_time_stamp"].isoformat().startswith("2026-10-07T14:01:00")
        assert row["end_time_stamp"].isoformat().startswith("2026-10-07T15:01:02.123456")
        assert row["segmentation_table"] == "synapses__minnie3_v1" and row["lookup_all_root_ids"] is False
        # the run's details stay in the commit metadata as well
        (commit,) = log.runs(str(table_dir(tmp_path)))
        assert commit["run_id"] == RUN_ID and commit["rows"] == "4"
        assert not staging_dir(tmp_path).exists()

    def test_finalize_again_adds_neither_a_commit_nor_a_manifest_row(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        write(md, [1], TASK["a"])
        log.finalize(md)
        write(md, [1], TASK["a"])  # the same run's chunk shows up again
        log.finalize(md)
        assert len(log.runs(str(table_dir(tmp_path)))) == 1 and len(manifest_rows(tmp_path)) == 1
        assert DeltaTable(str(table_dir(tmp_path))).to_pyarrow_table().num_rows == 1

    def test_runs_that_changed_nothing_or_failed_are_listed_so_windows_chain(self, bucket):
        tmp_path, _ = bucket
        first = metadata()
        write(first, [1], TASK["a"])
        log.finalize(first)
        quiet = metadata(materialization_time_stamp=LATER_TS, last_updated_time_stamp=RUN_TS)
        log.finalize(quiet)  # nothing to update: no data commit, still a manifest row
        failed = metadata(materialization_time_stamp="2026-10-07 17:01:02.000000", last_updated_time_stamp=LATER_TS)
        write(failed, [2], TASK["b"])
        log.finalize(failed, succeeded=False)
        rows = manifest_rows(tmp_path)
        assert [(r["run_id"], r["rows"], r["table_version"], r["succeeded"]) for r in rows] == [
            (RUN_ID, 1, 0, True), (LATER_ID, 0, None, True), ("20261007T170102.000000Z", 1, 1, False)]
        assert all(b["start_time_stamp"] == a["end_time_stamp"] for a, b in zip(rows, rows[1:]))
        assert len(log.runs(str(table_dir(tmp_path)))) == 2  # data commits only for runs with updates

    def test_repair_lists_a_data_commit_the_manifest_missed(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        write(md, [1], TASK["a"])
        with mock.patch.object(log, "append_manifest_row", side_effect=OSError("bucket down")):
            log.finalize(md)
        assert manifest_rows(tmp_path) == [] and len(log.runs(str(table_dir(tmp_path)))) == 1
        assert log.repair_manifest(str(table_dir(tmp_path))) == [RUN_ID]
        (row,) = manifest_rows(tmp_path)
        assert row["rows"] == 1 and row["table_version"] == 0 and row["succeeded"] is None
        assert log.repair_manifest(str(table_dir(tmp_path))) == []

    def test_the_tables_are_append_only_with_default_log_retention(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        write(md, [1], TASK["a"])
        log.finalize(md)
        for uri in (str(table_dir(tmp_path)), str(table_dir(tmp_path) / "_manifest")):
            config = DeltaTable(uri).metadata().configuration
            assert config == {"delta.appendOnly": "true"}  # default log cleanup and retention
        with pytest.raises(Exception, match="append-only"):
            DeltaTable(str(table_dir(tmp_path))).delete("id = 1")

    def test_files_are_zstd_compressed(self, bucket):
        import pyarrow.parquet as pq

        tmp_path, _ = bucket
        md = metadata()
        write(md, [1, 2], TASK["a"])
        log.finalize(md)
        for uri in (str(table_dir(tmp_path)), str(table_dir(tmp_path) / "_manifest")):
            for f in DeltaTable(uri).file_uris():
                path = f.replace("file://", "")
                assert path.endswith(".zstd.parquet")
                assert pq.ParquetFile(path).metadata.row_group(0).column(0).compression == "ZSTD"

    def test_a_mirror_needs_only_the_new_files(self, bucket, tmp_path):
        tmp, _ = bucket
        first, second = metadata(), metadata(materialization_time_stamp=LATER_TS, last_updated_time_stamp=RUN_TS)
        write(first, [1, 2], TASK["a"])
        log.finalize(first)
        mirror = tmp_path / "mirror"
        shutil.copytree(table_dir(tmp), mirror)
        have = {p.relative_to(mirror) for p in mirror.rglob("*") if p.is_file()}

        write(second, [3], TASK["b"])
        log.finalize(second)
        # an rsync: copy only files the mirror does not have (files are never rewritten)
        new = [p.relative_to(table_dir(tmp)) for p in table_dir(tmp).rglob("*") if p.is_file()]
        new = [p for p in new if p not in have]
        for p in new:
            (mirror / p).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(table_dir(tmp) / p, mirror / p)
        assert any(str(p).startswith(f"run_id={LATER_ID}/") for p in new)
        assert sorted(DeltaTable(str(mirror)).to_pyarrow_table().column("id").to_pylist()) == [1, 2, 3]
        assert [r["run_id"] for r in log.manifest(str(mirror)).to_pylist()] == [RUN_ID, LATER_ID]
        assert log.added_files(str(table_dir(tmp)), after_version=0) == sorted(
            str(p) for p in new if str(p).startswith("run_id="))

    def test_a_large_run_is_streamed_into_one_commit(self, bucket, monkeypatch):
        tmp_path, _ = bucket
        monkeypatch.setattr(log, "STREAM_GROUP_FILES", 2)
        md = metadata()
        for i, t in enumerate("abcd"):
            write(md, [i * 10 + 1, i * 10 + 2], TASK[t])
        log.finalize(md)
        (row,) = manifest_rows(tmp_path)
        assert row["rows"] == 8 and row["duplicate_rows_removed"] is None and row["chunk_files"] == 4
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


class TestRebuildTable:
    def test_copies_an_earlier_table_without_end_time_stamp_and_with_a_manifest(self, tmp_path):
        """The 2026-10-09 tables had an end_time_stamp column and no manifest."""
        from deltalake import CommitProperties, write_deltalake

        source = tmp_path / "old"
        old_schema = pa.schema(list(log.SCHEMA) + [("end_time_stamp", log.TS), ("run_id", pa.string())])
        for run, ts, ids in ((RUN_ID, RUN_TS, [1, 2]), (LATER_ID, LATER_TS, [3])):
            end = log._utc(ts)
            rows = pa.Table.from_pylist([{"id": i, "root_column": "pre_pt", "supervoxel_id": i, "old_root_id": 10,
                                          "new_root_id": 11, "end_time_stamp": end, "run_id": run} for i in ids],
                                        schema=old_schema)
            write_deltalake(str(source), rows, mode="append", partition_by=["run_id"], commit_properties=CommitProperties(
                custom_metadata={"run_id": run, "end_time_stamp": end.isoformat(), "rows": str(len(ids)),
                                 "start_time_stamp": "", "duplicate_rows_removed": "2", "annotation_table": "synapses"}))
        target = str(tmp_path / "new")
        assert [r["result"] for r in log.rebuild_table(str(source), target)] == ["would copy", "would copy"]
        assert not DeltaTable.is_deltatable(target)

        report = log.rebuild_table(str(source), target, dry_run=False)
        assert [(r["run_id"], r["rows"], r["target_version"]) for r in report] == [(RUN_ID, 2, 0), (LATER_ID, 1, 1)]
        assert DeltaTable(target).to_pyarrow_table().column_names == list(log.TABLE_SCHEMA.names)
        rows = log.manifest(target).to_pylist()
        assert [(r["run_id"], r["rows"], r["table_version"], r["duplicate_rows_removed"]) for r in rows] == [
            (RUN_ID, 2, 0, 2), (LATER_ID, 1, 1, 2)]
        assert DeltaTable(str(source)).version() == 1  # untouched
