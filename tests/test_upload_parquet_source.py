"""Uploads read from a parquet file, a (hive-partitioned) folder of them, or a Delta table."""

import io
from datetime import datetime, timezone
from unittest import mock

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest
from deltalake import write_deltalake
from flask import Flask

from materializationengine.blueprints.upload import tasks
from materializationengine.blueprints.upload.parquet_source import (
    hive_partition_values,
    is_parquet_source,
    iter_parquet_part,
    list_parquet_files,
    plan_parquet_parts,
    preview_parquet,
)
from materializationengine.blueprints.upload.schemas import UploadRequestSchema
from tests.test_upload_csv_parts import (
    BUCKET,
    JOB,
    MAPPING_GENERATED_IDS,
    MAPPING_WITH_IDS,
    FakeBucket,
    FakeRedis,
    make_processor,
    run_parts_inline,
)


def synapses(n, seed=0):
    rng = np.random.default_rng(seed)
    df = pd.DataFrame({"id": np.arange(1000, 1000 + n, dtype=np.int64)})
    for point in ["pre", "post", "ctr"]:
        for axis in "xyz":
            df[f"{point}_{axis}"] = rng.integers(0, 300000, n).astype(np.int32)
    df["size"] = rng.integers(1, 10000, n).astype(float)
    df["bucket"] = df["id"] % 7  # a hive partition column
    return df


def write_hive_folder(path, df, row_group_size=40):
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        table,
        str(path),
        format="parquet",
        partitioning=ds.partitioning(pa.schema([("bucket", pa.int64())]), flavor="hive"),
        file_options=ds.ParquetFileFormat().make_write_options(),
        max_rows_per_group=row_group_size,
        min_rows_per_group=row_group_size,
        basename_template="part-{i}.parquet",
    )
    (path / "_SUCCESS").write_text("")
    (path / "bucket=0" / ".part-0.parquet.crc").write_text("x")
    return path


@pytest.fixture
def redis_clients():
    with mock.patch.object(tasks, "REDIS_CLIENT", FakeRedis()) as fake:
        yield fake


class TestListing:
    def test_is_parquet_source(self):
        assert is_parquet_source("gs://b/dir/") and is_parquet_source("s3://b/x.parquet")
        assert is_parquet_source("gs://b/delta_table")
        assert not is_parquet_source("gs://b/x.csv") and not is_parquet_source("gs://b/X.CSV")
        assert is_parquet_source("gs://b/table.v2/")  # a trailing slash means a folder
        with pytest.raises(ValueError, match="not a .csv or .parquet"):
            is_parquet_source("gs://b/notes.txt")

    def test_hive_folder_skips_hidden_entries(self, tmp_path):
        write_hive_folder(tmp_path / "t", synapses(300))
        listing = list_parquet_files(f"file://{tmp_path}/t")
        assert listing["kind"] == "folder"
        assert len(listing["files"]) == 7
        assert all(f.endswith(".parquet") and "/." not in f for f in listing["files"])

    def test_hive_partition_values(self):
        assert hive_partition_values("/r/a=1/b=x%2Fy/c=2.5/f.parquet", "/r") == {"a": 1, "b": "x/y", "c": 2.5}
        assert hive_partition_values("/r/plain/f.parquet", "/r") == {}
        assert hive_partition_values("/r/a=__HIVE_DEFAULT_PARTITION__/f.parquet", "/r") == {"a": None}

    def test_single_file(self, tmp_path):
        pq.write_table(pa.Table.from_pandas(synapses(50), preserve_index=False), tmp_path / "one.parquet")
        listing = list_parquet_files(f"file://{tmp_path}/one.parquet")
        assert listing["kind"] == "file" and len(listing["files"]) == 1

    def test_delta_table_reads_only_current_files(self, tmp_path):
        uri = f"file://{tmp_path}/delta"
        write_deltalake(str(tmp_path / "delta"), synapses(100, seed=1), partition_by=["bucket"])
        newer = synapses(60, seed=2)
        write_deltalake(str(tmp_path / "delta"), newer, partition_by=["bucket"], mode="overwrite")
        on_disk = list((tmp_path / "delta").rglob("*.parquet"))
        listing = list_parquet_files(uri)
        assert listing["kind"] == "delta" and listing["partition_columns"] == ["bucket"]
        assert len(on_disk) > len(listing["files"])  # the overwritten files are still there
        plan = plan_parquet_parts(uri)
        assert plan["total_rows"] == 60
        rows = pd.concat(iter_parquet_part(uri, plan["root"], plan["parts"][0]["pieces"], 1000))
        assert sorted(rows["id"]) == sorted(newer["id"])

    def test_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            list_parquet_files(f"file://{tmp_path}/nope")
        (tmp_path / "empty").mkdir()
        with pytest.raises(FileNotFoundError):
            list_parquet_files(f"file://{tmp_path}/empty")


class TestPlanAndRead:
    @pytest.mark.parametrize("target_rows", [1, 40, 100, 10**9])
    def test_parts_cover_every_row_group_once(self, tmp_path, target_rows):
        df = synapses(900)
        write_hive_folder(tmp_path / "t", df)
        uri = f"file://{tmp_path}/t"
        plan = plan_parquet_parts(uri, target_rows)
        assert plan["total_rows"] == 900 == sum(p["rows"] for p in plan["parts"])
        # a piece is [path, groups] or a slice [path, [group], first row, rows]
        seen = [
            (piece[0], g, piece[2] if len(piece) == 4 else None)
            for p in plan["parts"] for piece in p["pieces"] for g in piece[1]
        ]
        assert len(seen) == len(set(seen))
        assert all(p["rows"] <= target_rows for p in plan["parts"])
        rows = pd.concat(
            [d for p in plan["parts"] for d in iter_parquet_part(uri, plan["root"], p["pieces"], 25)]
        )
        assert len(rows) == 900
        merged = rows.sort_values("id").reset_index(drop=True)
        expected = df.sort_values("id").reset_index(drop=True)
        assert (merged["bucket"].to_numpy() == expected["bucket"].to_numpy()).all()  # from the paths
        assert (merged["pre_x"].to_numpy() == expected["pre_x"].to_numpy()).all()

    @pytest.mark.parametrize("target_rows", [1, 7, 300, 999, 1000])
    def test_a_big_row_group_is_split_into_slices(self, tmp_path, target_rows):
        df = synapses(1000)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), tmp_path / "big.parquet",
                       row_group_size=10**6)
        uri = f"file://{tmp_path}/big.parquet"
        plan = plan_parquet_parts(uri, target_rows)
        assert len(plan["parts"]) == -(-1000 // target_rows)
        assert all(p["rows"] <= target_rows for p in plan["parts"])
        rows = pd.concat(
            [d for p in plan["parts"] for d in iter_parquet_part(uri, plan["root"], p["pieces"], 64)]
        )
        assert rows["id"].tolist() == df["id"].tolist()  # every row once, in order

    def test_preview(self, tmp_path):
        write_hive_folder(tmp_path / "t", synapses(100))
        info = preview_parquet(f"file://{tmp_path}/t")
        assert info["kind"] == "folder" and info["files"] == 7 and info["total_rows"] == 100
        header, first = info["preview_rows"]
        assert header[-1] == "bucket" and "pre_x" in header
        assert len(first) == len(header) and first[header.index("bucket")] == "0"


class TestProcessCsvFromParquet:
    """process_csv on a parquet source matches the same rows uploaded as a CSV."""

    def run(self, uri, data_csv, mapping, redis_clients, part_rows):
        bucket = FakeBucket({"in.csv": data_csv})
        app = Flask(__name__)
        app.config.update(
            MATERIALIZATION_UPLOAD_BUCKET_PATH=BUCKET,
            UPLOAD_PARQUET_PART_ROWS=part_rows,
            UPLOAD_CSV_PART_SIZE_MB=1000,
        )
        outputs = {}
        with app.app_context(), \
                mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(bucket, 50)), \
                mock.patch.object(tasks, "_run_csv_part_tasks", run_parts_inline), \
                mock.patch.object(tasks, "datetime") as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 10, 25, tzinfo=timezone.utc)
            fake_datetime.fromisoformat = datetime.fromisoformat
            for name, path in [("parquet", uri), ("csv", f"gs://{BUCKET}/in.csv")]:
                result = tasks.process_csv.run(
                    file_path=path, schema_type="synapse", column_mapping=mapping,
                    job_id_for_status=JOB,
                )
                outputs[name] = (bucket.objects.pop(result["output_path"].split("/", 1)[1]), result)
        assert not any(k.startswith("processed_parts/") for k in bucket.objects)
        return outputs

    @pytest.mark.parametrize("mapping", [MAPPING_WITH_IDS, MAPPING_GENERATED_IDS], ids=["ids", "generated"])
    def test_matches_the_csv_path(self, tmp_path, redis_clients, mapping):
        df = synapses(600)
        df.loc[[5, 77], "pre_x"] = np.nan  # dropped: missing coordinates
        # sort the CSV the way the parquet parts are read: by partition folder, then file order
        write_hive_folder(tmp_path / "t", df)
        uri = f"file://{tmp_path}/t"
        plan = plan_parquet_parts(uri, 10**9)
        ordered = pd.concat(iter_parquet_part(uri, plan["root"], plan["parts"][0]["pieces"], 10**6))
        ordered = ordered[["id"] + [c for c in df.columns if c not in ("id",)]]
        if "id" not in mapping:
            ordered = ordered.drop(columns=["id"])
        csv = io.StringIO()
        ordered.to_csv(csv, index=False)
        outputs = self.run(uri, csv.getvalue().encode(), mapping, redis_clients, part_rows=120)
        parquet_out, parquet_result = outputs["parquet"]
        csv_out, csv_result = outputs["csv"]
        assert parquet_out == csv_out
        assert parquet_result["dropped_rows"] == csv_result["dropped_rows"] == 2
        assert parquet_result["last_assigned_id"] == csv_result["last_assigned_id"]

    def test_empty_source_fails(self, tmp_path, redis_clients):
        pq.write_table(pa.Table.from_pandas(synapses(0), preserve_index=False), tmp_path / "e.parquet")
        app = Flask(__name__)
        app.config.update(MATERIALIZATION_UPLOAD_BUCKET_PATH=BUCKET)
        with app.app_context(), \
                mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(FakeBucket(), 50)):
            with pytest.raises(ValueError, match="no rows"):
                tasks.process_csv.run(
                    file_path=f"file://{tmp_path}/e.parquet", schema_type="synapse",
                    column_mapping=MAPPING_WITH_IDS, job_id_for_status=JOB,
                )

    def test_part_with_changed_row_count_fails(self, tmp_path, redis_clients):
        write_hive_folder(tmp_path / "t", synapses(100))
        uri = f"file://{tmp_path}/t"
        plan = plan_parquet_parts(uri, 10**9)
        bucket = FakeBucket()
        with mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(bucket, 50)):
            with pytest.raises(ValueError, match="expected"):
                tasks.process_parquet_part.run(
                    bucket_name=BUCKET, source_uri=uri, root=plan["root"],
                    pieces=plan["parts"][0]["pieces"], expected_rows=101,
                    destination_blob_name="p.csv", schema_type="synapse",
                    column_mapping=MAPPING_WITH_IDS, timestamp="2026-10-25T00:00:00",
                )


class TestRequestSchema:
    BASE = {
        "columnMapping": {},
        "metadata": {
            "schema_type": "synapse", "datastack_name": "d", "table_name": "t",
            "description": "x", "voxel_resolution_nm_x": 4, "voxel_resolution_nm_y": 4,
            "voxel_resolution_nm_z": 40, "read_permission": "PUBLIC", "write_permission": "PRIVATE",
        },
    }

    def test_a_parquet_folder_needs_no_csv_name(self):
        loaded = UploadRequestSchema().load({**self.BASE, "filename": "id", "sourceUri": "gs://b/t/id/"})
        assert loaded["source_uri"] == "gs://b/t/id/"

    def test_a_browser_upload_must_be_a_csv(self):
        with pytest.raises(Exception, match="CSV"):
            UploadRequestSchema().load({**self.BASE, "filename": "data.parquet"})
