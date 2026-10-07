"""The record of root ID updates written for each update_root_ids run (Parquet + Delta Lake)."""

import json
from unittest import mock

import pandas as pd
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable

from materializationengine.workflows import root_id_update_log as log

RUN_TS = "2026-10-07 15:01:02.123456"


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


def run_dir(tmp_path, table="synapses"):
    return tmp_path / "root_id_updates" / "minnie65_phase3_v1" / "20261007T150102.123456Z" / table


class TestPaths:
    def test_folder_is_deterministic_per_run_and_shared_by_its_tables(self, bucket):
        tmp_path, _ = bucket
        assert log.run_uri(metadata()) == str(run_dir(tmp_path))
        assert log.run_uri(metadata()) == log.run_uri(metadata())
        # the ISO form of the same timestamp names the same run
        assert log.run_uri(metadata(materialization_time_stamp="2026-10-07T15:01:02.123456")) == str(run_dir(tmp_path))
        assert log.run_uri(metadata("cells")).rsplit("/", 1)[0] == log.run_uri(metadata()).rsplit("/", 1)[0]

    def test_no_bucket_no_folder(self, bucket):
        _, config = bucket
        del config["MATERIALIZATION_DUMP_BUCKET"]
        assert log.run_uri(metadata()) is None

    @pytest.mark.parametrize("value, expected", [(True, True), (False, False), ("True", True), ("false", False), ("1", True), (None, False)])
    def test_enabled_reads_bools_and_env_strings(self, bucket, value, expected):
        _, config = bucket
        config["ROOT_ID_UPDATE_LOG"] = value
        assert log.enabled() is expected


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
        self.write(md, [1, 2], [10, 20], [11, 21], "task-a")
        self.write(md, [1, 2], [10, 20], [12, 22], "task-b", side="post_pt")
        self.write(md, [3], [30], [31], "task-c")
        log.finalize(md)

        rows = DeltaTable(str(run_dir(tmp_path))).to_pyarrow_table()
        assert rows.num_rows == 5 and rows.schema == log.SCHEMA
        manifest = json.loads((run_dir(tmp_path) / "_run.json").read_text())
        assert manifest["state"] == "done" and manifest["rows"] == 5 and manifest["files"] == 3
        assert manifest["root_columns"] == ["post_pt", "pre_pt"]
        assert manifest["annotation_table"] == "synapses" and manifest["segmentation_table"] == "synapses__minnie3_v1"
        assert manifest["materialization_time_stamp"] == RUN_TS and manifest["lookup_all_root_ids"] is False

    def test_a_retried_task_overwrites_its_own_file(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        self.write(md, [1, 2], [10, 20], [11, 21], "task-a")
        self.write(md, [1, 2], [10, 20], [11, 21], "task-a")
        assert len(list(run_dir(tmp_path).glob("*.parquet"))) == 1
        assert pq.read_table(next(run_dir(tmp_path).glob("*.parquet"))).num_rows == 2

    def test_nothing_updated_writes_nothing(self, bucket):
        tmp_path, _ = bucket
        log.finalize(metadata())
        assert not (tmp_path / "root_id_updates").exists()

    def test_finalize_twice_keeps_one_table(self, bucket):
        tmp_path, _ = bucket
        md = metadata()
        self.write(md, [1], [10], [11], "task-a")
        log.finalize(md)
        log.finalize(md)
        assert DeltaTable(str(run_dir(tmp_path))).to_pyarrow_table().num_rows == 1

    def test_failures_are_logged_not_raised(self, bucket):
        md = metadata()
        with mock.patch.object(log.pq, "write_table", side_effect=OSError("bucket down")), \
                mock.patch.object(log.celery_logger, "warning") as warning:
            assert self.write(md, [1], [10], [11], "task-a") is None
        assert "bucket down" in warning.call_args[0][0]
        with mock.patch.object(log, "updates_frame", side_effect=ValueError("bad row")):
            assert self.write(md, [1], [10], [11], "task-a") is None
        with mock.patch.object(log, "_filesystem", side_effect=OSError("bucket down")):
            log.start(md)
            log.finalize(md)
