"""The record of root ID updates written for each update_root_ids run (Parquet + Delta Lake)."""

import json
from unittest import mock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable

from materializationengine.workflows import root_id_update_log as log

RUN_TS = "2026-10-07 15:01:02.123456"
# celery task ids, which name the chunk files
TASK = {k: f"{i:08x}-0000-4000-8000-000000000000" for i, k in enumerate("abc")}


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


RUN_ID = "20261007T150102.123456Z"


def table_dir(tmp_path, table="synapses"):
    return tmp_path / "root_id_updates" / "minnie65_phase3_v1" / table


def run_dir(tmp_path, table="synapses"):
    return table_dir(tmp_path, table) / RUN_ID


def manifest_rows(tmp_path, table="synapses"):
    return DeltaTable(str(table_dir(tmp_path, table) / "_manifest")).to_pyarrow_table().to_pylist()


class TestPaths:
    def test_folders_are_by_table_then_run(self, bucket):
        tmp_path, _ = bucket
        assert log.table_uri(metadata()) == str(table_dir(tmp_path))
        assert log.run_uri(metadata()) == str(run_dir(tmp_path))
        assert log.run_uri(metadata()) == log.run_uri(metadata())
        # the ISO form of the same timestamp names the same run
        assert log.run_uri(metadata(materialization_time_stamp="2026-10-07T15:01:02.123456")) == str(run_dir(tmp_path))
        # another run of the table sits in the same table folder
        later = log.run_uri(metadata(materialization_time_stamp="2026-10-07 16:01:02.000000"))
        assert later == str(table_dir(tmp_path) / "20261007T160102.000000Z")

    def test_no_bucket_no_folder(self, bucket):
        _, config = bucket
        del config["MATERIALIZATION_DUMP_BUCKET"]
        assert log.run_uri(metadata()) is None

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


class TestRecord:
    def write(self, md, ids, old, new, task_id, side="pre_pt"):
        df, old_roots = chunk(ids, old, new, side)
        return log.record_chunk(md, df, old_roots, f"{side}_root_id", f"{side}_supervoxel_id", task_id)

    def test_run_is_one_delta_table_per_annotation_table_with_metadata_beside_it(self, bucket):
        tmp_path, _ = bucket
        md = metadata(lookup_all_root_ids=False)
        log.start(md)
        assert json.loads((run_dir(tmp_path) / "_run.json").read_text())["state"] == "running"
        self.write(md, [1, 2], [10, 20], [11, 21], TASK["a"])
        self.write(md, [1, 2], [10, 20], [12, 22], TASK["b"], side="post_pt")
        self.write(md, [3], [30], [31], TASK["c"])
        log.finalize(md)

        rows = DeltaTable(str(run_dir(tmp_path))).to_pyarrow_table()
        assert rows.num_rows == 5 and rows.schema == log.SCHEMA
        manifest = json.loads((run_dir(tmp_path) / "_run.json").read_text())
        assert manifest["state"] == "done" and manifest["rows"] == 5 and manifest["chunk_files"] == 3
        # the three chunks were compacted into one file of the table
        assert manifest["files"] == 1 and manifest["compaction"] == {"numFilesAdded": 1, "numFilesRemoved": 3}
        assert len(DeltaTable(str(run_dir(tmp_path))).file_uris()) == 1
        # ...and the chunk files deleted
        assert manifest["vacuumed_files"] == 3 and not list(run_dir(tmp_path).glob("part-pre_pt-*"))
        assert len(list(run_dir(tmp_path).glob("*.parquet"))) == 1
        assert manifest["root_columns"] == ["post_pt", "pre_pt"]
        assert manifest["annotation_table"] == "synapses" and manifest["segmentation_table"] == "synapses__minnie3_v1"
        assert manifest["materialization_time_stamp"] == RUN_TS and manifest["lookup_all_root_ids"] is False

    def test_a_retried_task_overwrites_its_own_file(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        self.write(md, [1, 2], [10, 20], [11, 21], TASK["a"])
        self.write(md, [1, 2], [10, 20], [11, 21], TASK["a"])
        assert len(list(run_dir(tmp_path).glob("*.parquet"))) == 1
        assert pq.read_table(next(run_dir(tmp_path).glob("*.parquet"))).num_rows == 2

    def test_nothing_updated_writes_nothing(self, bucket):
        tmp_path, _ = bucket
        log.finalize(metadata())
        assert not (tmp_path / "root_id_updates").exists()

    def test_finalize_twice_keeps_one_table(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        self.write(md, [1], [10], [11], TASK["a"])
        self.write(md, [2], [20], [21], TASK["b"])
        log.finalize(md)
        log.finalize(md)
        assert DeltaTable(str(run_dir(tmp_path))).to_pyarrow_table().num_rows == 2
        manifest = json.loads((run_dir(tmp_path) / "_run.json").read_text())
        # the second run finds no chunk files (deleted) and keeps what the first recorded
        assert manifest["rows"] == 2 and manifest["chunk_files"] == 2 and manifest["root_columns"] == ["pre_pt"]
        assert manifest["files"] == 1 and manifest["vacuumed_files"] == 2
        assert manifest["compaction"] == {"numFilesAdded": 1, "numFilesRemoved": 2}

    def test_failures_are_logged_not_raised(self, bucket):
        md = metadata()
        with mock.patch.object(log.pq, "write_table", side_effect=OSError("bucket down")), \
                mock.patch.object(log.celery_logger, "warning") as warning:
            assert self.write(md, [1], [10], [11], TASK["a"]) is None
        assert "bucket down" in warning.call_args[0][0]
        with mock.patch.object(log, "updates_frame", side_effect=ValueError("bad row")):
            assert self.write(md, [1], [10], [11], TASK["a"]) is None
        with mock.patch.object(log, "_filesystem", side_effect=OSError("bucket down")):
            log.start(md)
            log.finalize(md)


class TestManifest:
    def write(self, md, ids, task, side="pre_pt"):
        df, old = chunk(ids, [i * 10 for i in ids], [i * 10 + 1 for i in ids], side)
        log.record_chunk(md, df, old, f"{side}_root_id", f"{side}_supervoxel_id", task)

    def test_each_finished_run_is_listed_once_with_its_window(self, bucket):
        tmp_path, _ = bucket
        first = metadata(lookup_all_root_ids=False)
        self.write(first, [1, 2], TASK["a"])
        log.finalize(first)
        log.finalize(first)  # again: no second row
        second = metadata(materialization_time_stamp="2026-10-07 16:01:02.000000",
                          last_updated_time_stamp=RUN_TS)
        self.write(second, [3], TASK["b"], side="post_pt")
        log.finalize(second)
        rows = sorted(manifest_rows(tmp_path), key=lambda r: r["run_id"])
        assert [r["run_id"] for r in rows] == [RUN_ID, "20261007T160102.000000Z"]
        assert [r["path"] for r in rows] == [r["run_id"] for r in rows]
        assert rows[0]["rows"] == 2 and rows[0]["files"] == 1 and rows[0]["root_columns"] == ["pre_pt"]
        assert rows[0]["start_time_stamp"].isoformat().startswith("2026-10-07T14:01:00")
        assert rows[0]["end_time_stamp"].isoformat().startswith("2026-10-07T15:01:02.123456")
        # windows chain: the second run starts where the first ended
        assert rows[1]["start_time_stamp"] == rows[0]["end_time_stamp"]
        assert rows[1]["root_columns"] == ["post_pt"] and rows[1]["duplicate_rows_removed"] is None

    def test_a_run_without_updates_is_not_listed(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        log.start(md)
        log.finalize(md)
        assert not (table_dir(tmp_path) / "_manifest").exists()


class TestMigrateOldLayout:
    def old_run(self, tmp_path, run_id, table, rows, last_updated):
        """A run as the old code wrote it: {datastack}/{run_id}/{table}/ Delta table + _run.json."""
        from deltalake import write_deltalake

        path = tmp_path / "root_id_updates" / "minnie65_phase3_v1" / run_id / table
        write_deltalake(str(path), pa.Table.from_pylist(rows, schema=log.SCHEMA))
        info = {"state": "done", "rows": len(rows), "files": 1, "root_columns": ["pre_pt"],
                "materialization_time_stamp": f"{run_id[:4]}-{run_id[4:6]}-{run_id[6:8]} {run_id[9:11]}:{run_id[11:13]}:{run_id[13:15]}.{run_id[16:22]}",
                "last_updated_time_stamp": last_updated, "lookup_all_root_ids": False}
        (path / "_run.json").write_text(json.dumps(info))
        return path

    def test_copies_deduplicated_runs_by_table_and_lists_them(self, tmp_path):
        row = lambda i: {"id": i, "root_column": "pre_pt", "supervoxel_id": i, "old_root_id": 10, "new_root_id": 11}
        old = self.old_run(tmp_path, RUN_ID, "synapses", [row(1), row(2), row(2), row(3), row(3)],
                           "2026-10-07 14:01:00.000000")
        self.old_run(tmp_path, "20261007T160102.000000Z", "synapses", [row(4)], "2026-10-07 15:01:02.123456")
        empty = tmp_path / "root_id_updates" / "minnie65_phase3_v1" / RUN_ID / "cells"
        empty.mkdir(parents=True)
        (empty / "_run.json").write_text(json.dumps({"state": "done", "rows": 0}))
        uri = str(tmp_path / "root_id_updates" / "minnie65_phase3_v1")

        plan = log.migrate_old_layout(uri)  # dry run: writes nothing
        assert {(p["table"], p["result"]) for p in plan} == {("synapses", "would copy"), ("cells", "no updates (rows=0), skipped")}
        assert not table_dir(tmp_path).exists()

        report = log.migrate_old_layout(uri, dry_run=False)
        first = next(p for p in report if p["run_id"] == RUN_ID and p["table"] == "synapses")
        assert first["result"] == "copied" and first["rows"] == 5 and first["duplicate_rows_removed"] == 2
        assert DeltaTable(str(run_dir(tmp_path))).to_pyarrow_table().num_rows == 3
        info = json.loads((run_dir(tmp_path) / "_run.json").read_text())
        assert info["rows"] == 3 and info["duplicate_rows_removed"] == 2 and info["migrated_from"] == str(old)
        rows = sorted(manifest_rows(tmp_path), key=lambda r: r["run_id"])
        assert [(r["run_id"], r["rows"], r["duplicate_rows_removed"]) for r in rows] == [
            (RUN_ID, 3, 2), ("20261007T160102.000000Z", 1, 0)]
        assert not (table_dir(tmp_path, "cells")).exists()
        # the old layout is untouched
        assert DeltaTable(str(old)).to_pyarrow_table().num_rows == 5

        again = log.migrate_old_layout(uri, dry_run=False)
        assert {p["result"] for p in again if p["table"] == "synapses"} == {"already copied"}
        assert len(manifest_rows(tmp_path)) == 2
