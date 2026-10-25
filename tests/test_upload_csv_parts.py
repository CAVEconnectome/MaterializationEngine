"""Processing an uploaded CSV in byte-range parts gives the same output as one stream."""

import io
from datetime import datetime, timezone
from unittest import mock

import numpy as np
import pandas as pd
import pytest
from flask import Flask

from materializationengine.blueprints.upload import gcs_processor as gcs_module
from materializationengine.blueprints.upload import tasks
from materializationengine.blueprints.upload.gcs_processor import GCSCsvProcessor
from materializationengine.blueprints.upload.processor import SchemaProcessor



class FakeRedis:
    """The subset of redis the job status and cancel key use."""

    def __init__(self):
        self.data = {}

    def set(self, key, value, ex=None):
        self.data[key] = value if isinstance(value, bytes) else str(value).encode()

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return int(key in self.data)

BUCKET = "uploads"
JOB = "minnie65_phase3_v1_synapses_parts_test_20261024_120000"
MAPPING_WITH_IDS = {
    f"{point}_position_{axis}": f"{point.split('_')[0]}_{axis}"
    for point in ["pre_pt", "post_pt", "ctr_pt"]
    for axis in "xyz"
}
MAPPING_WITH_IDS.update({"id": "id", "size": "size"})
MAPPING_GENERATED_IDS = {k: v for k, v in MAPPING_WITH_IDS.items() if k != "id"}


class FakeBlob:
    def __init__(self, bucket, name):
        self.bucket = bucket
        self.name = name
        self.content_type = None

    @property
    def size(self):
        return len(self.bucket.objects[self.name])

    def exists(self):
        return self.name in self.bucket.objects

    def download_as_bytes(self, start=0, end=None):
        data = self.bucket.objects[self.name]
        # like GCS, end is inclusive
        return data[start : None if end is None else end + 1]

    def open(self, mode="rb", **kwargs):
        if mode == "rb":
            return io.BytesIO(self.bucket.objects[self.name])
        if mode == "rt":
            return io.StringIO(self.bucket.objects[self.name].decode())
        blob = self

        class Writer(io.BytesIO):
            def close(self):
                blob.bucket.objects[blob.name] = self.getvalue()
                super().close()

        return Writer()

    def compose(self, sources):
        assert len(sources) <= gcs_module.MAX_COMPOSE_SOURCES
        self.bucket.compose_calls += 1
        self.bucket.objects[self.name] = b"".join(
            self.bucket.objects[s.name] for s in sources
        )

    def delete(self):
        if self.name not in self.bucket.objects:
            raise gcs_module.NotFound("missing")
        del self.bucket.objects[self.name]


class FakeBucket:
    def __init__(self, objects=None):
        self.name = BUCKET
        self.objects = dict(objects or {})
        self.compose_calls = 0

    def blob(self, name):
        return FakeBlob(self, name)

    def get_blob(self, name):
        return FakeBlob(self, name) if name in self.objects else None


def make_processor(bucket, chunk_size=10000):
    processor = object.__new__(GCSCsvProcessor)
    processor.client = None
    processor.bucket = bucket
    processor.pandas_chunk_size = chunk_size
    return processor


def synapse_csv(n_rows, seed=0, trailing_newline=True, with_ids=True):
    rng = np.random.default_rng(seed)
    columns = (["id"] if with_ids else []) + [
        f"{p}_{a}" for p in ["pre", "post", "ctr"] for a in "xyz"
    ] + ["size"]
    lines = [",".join(columns)]
    for i in range(n_rows):
        values = ([str(1000 + i)] if with_ids else []) + [
            str(v) for v in rng.integers(0, 300000, 9)
        ] + [str(rng.integers(1, 10000))]
        if i % 97 == 5:
            values[1 if with_ids else 0] = ""  # missing pre_x: the row is dropped
        lines.append(",".join(values))
    text = "\n".join(lines) + ("\n" if trailing_newline else "")
    return text.encode()


@pytest.fixture
def redis_clients():
    status_redis = FakeRedis()
    with mock.patch.object(tasks, "REDIS_CLIENT", status_redis):
        yield status_redis


class TestPlanCsvParts:
    """plan_csv_parts splits after the header into whole lines, covering every byte once."""

    @pytest.mark.parametrize("part_size", [1, 7, 100, 1000, 10**7])
    @pytest.mark.parametrize("trailing_newline", [True, False])
    def test_ranges_cover_the_body_on_line_starts(self, part_size, trailing_newline):
        data = synapse_csv(60, trailing_newline=trailing_newline)
        bucket = FakeBucket({"in.csv": data})
        header, ranges = make_processor(bucket).plan_csv_parts("in.csv", part_size)

        assert header == data.split(b"\n", 1)[0].decode() + "\n"
        assert ranges[0][0] == len(header)
        assert ranges[-1][1] == len(data)
        for (_, end), (next_start, _) in zip(ranges, ranges[1:]):
            assert end == next_start
            assert data[end - 1 : end] == b"\n"
        assert all(start < end for start, end in ranges)
        assert sum(data[s:e].count(b"\n") for s, e in ranges) == data.count(b"\n") - 1

    def test_large_part_size_gives_one_part(self):
        bucket = FakeBucket({"in.csv": synapse_csv(10)})
        _, ranges = make_processor(bucket).plan_csv_parts("in.csv", 10**9)
        assert len(ranges) == 1

    def test_header_only(self):
        bucket = FakeBucket({"in.csv": b"id,pre_x\n"})
        header, ranges = make_processor(bucket).plan_csv_parts("in.csv", 10)
        assert header == "id,pre_x\n" and ranges == []

    def test_long_lines_need_several_search_windows(self, monkeypatch):
        monkeypatch.setattr(gcs_module, "LINE_SEARCH_BYTES", 3)
        data = b"a,b\n" + b"".join(b"%d,%s\n" % (i, b"x" * 40) for i in range(20))
        bucket = FakeBucket({"in.csv": data})
        _, ranges = make_processor(bucket).plan_csv_parts("in.csv", 50)
        assert len(ranges) > 1
        assert all(data[e - 1 : e] == b"\n" for _, e in ranges)

    def test_bom_is_not_part_of_the_first_column(self):
        bucket = FakeBucket({"in.csv": "﻿id,x\n1,2\n".encode()})
        header, _ = make_processor(bucket).plan_csv_parts("in.csv", 10)
        assert header == "id,x\n"

    def test_missing_source(self):
        with pytest.raises(FileNotFoundError):
            make_processor(FakeBucket()).plan_csv_parts("nope.csv", 10)


class TestCompose:
    @pytest.mark.parametrize("n_parts", [1, 2, 32, 33, 100, 1025])
    def test_compose_keeps_order_and_removes_parts(self, n_parts):
        names = [f"p/part-{i:05d}.csv" for i in range(n_parts)]
        bucket = FakeBucket({name: b"%d\n" % i for i, name in enumerate(names)})
        make_processor(bucket).compose(names, "out.csv")
        assert bucket.objects["out.csv"] == b"".join(b"%d\n" % i for i in range(n_parts))
        assert set(bucket.objects) == {"out.csv"}


def run_parts_inline(signatures, job_id, phase):
    return [signature.apply().get() for signature in signatures]


def sequential_output(data, mapping, timestamp, chunk_size):
    bucket = FakeBucket({"in.csv": data})
    schema_processor = SchemaProcessor("synapse", column_mapping=mapping)
    make_processor(bucket, chunk_size).process_csv_in_chunks(
        "in.csv", "out.csv", lambda c: schema_processor.process_chunk(c, timestamp)
    )
    return bucket.objects["out.csv"], schema_processor


class TestProcessInParts:
    """_process_csv_in_parts against the one-stream path, with the part tasks run inline."""

    def run_parts(self, data, mapping, part_size, chunk_size=50, id_counter_start=0):
        bucket = FakeBucket({"in.csv": data})
        processor = make_processor(bucket, chunk_size)
        timestamp = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)
        header, ranges = processor.plan_csv_parts("in.csv", part_size)
        with mock.patch.object(tasks, "_run_csv_part_tasks", run_parts_inline), \
                mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(bucket, chunk_size)):
            result = tasks._process_csv_in_parts(
                processor,
                bucket_name=BUCKET,
                source_blob_name="in.csv",
                header=header,
                ranges=ranges,
                destination_blob_name="out.csv",
                job_id=JOB,
                part_kwargs={
                    "schema_type": "synapse",
                    "column_mapping": mapping,
                    "chunk_size": chunk_size,
                    "timestamp": timestamp.isoformat(),
                },
                generate_ids="id" not in mapping,
                id_counter_start=id_counter_start,
            )
        return bucket, ranges, result, timestamp

    @pytest.mark.parametrize("trailing_newline", [True, False])
    def test_matches_one_stream_with_id_column(self, redis_clients, trailing_newline):
        data = synapse_csv(1000, trailing_newline=trailing_newline)
        bucket, ranges, result, timestamp = self.run_parts(data, MAPPING_WITH_IDS, 4000)
        assert len(ranges) > 5
        expected, schema_processor = sequential_output(data, MAPPING_WITH_IDS, timestamp, 50)
        assert bucket.objects["out.csv"] == expected
        assert result["dropped_rows"] == schema_processor.dropped_rows > 0
        assert result["rows_read"] == 1000
        assert not any(name.startswith("processed_parts/") for name in bucket.objects)

    def test_generated_ids_continue_across_parts(self, redis_clients):
        data = synapse_csv(1000, with_ids=False)
        bucket, ranges, result, timestamp = self.run_parts(
            data, MAPPING_GENERATED_IDS, 4000, id_counter_start=500
        )
        assert len(ranges) > 5
        schema_processor = SchemaProcessor(
            "synapse", column_mapping=MAPPING_GENERATED_IDS, id_counter_start=500
        )
        bucket_one = FakeBucket({"in.csv": data})
        make_processor(bucket_one, 50).process_csv_in_chunks(
            "in.csv", "out.csv", lambda c: schema_processor.process_chunk(c, timestamp)
        )
        assert bucket.objects["out.csv"] == bucket_one.objects["out.csv"]
        assert result["last_assigned_id"] == schema_processor._id_counter == 1500

    def test_quoted_newlines_fall_back_to_one_stream(self, redis_clients):
        rows = "".join(f'{i},"a note\nover two lines",{i}\n' for i in range(200))
        data = ("id,note,size\n" + rows).encode()
        bucket = FakeBucket({"in.csv": data})
        processor = make_processor(bucket, 20)
        header, ranges = processor.plan_csv_parts("in.csv", 500)

        def run_parts(signatures, job_id, phase):
            # what process_csv_part reports for each range, with an identity transform
            results = []
            for i, (start, end) in enumerate(ranges):
                try:
                    r = processor.process_csv_range(
                        "in.csv", start, end, header,
                        f"processed_parts/{JOB}/part-{i:05d}.csv", lambda chunk: chunk,
                    )
                except pd.errors.ParserError as e:
                    r = {"split_error": str(e)}
                if r.get("rows_read") != r.get("lines"):
                    r["split_error"] = "rows and lines differ"
                results.append(r)
            return results

        with mock.patch.object(tasks, "_run_csv_part_tasks", run_parts):
            result = tasks._process_csv_in_parts(
                processor, BUCKET, "in.csv", header, ranges, "out.csv", JOB,
                {"schema_type": "synapse", "column_mapping": {}, "timestamp": "2026-10-24T00:00:00"},
                generate_ids=False, id_counter_start=0,
            )
        assert result is None
        assert "out.csv" not in bucket.objects
        assert set(bucket.objects) == {"in.csv"}

    def test_range_reader_counts_rows_against_lines(self):
        rows = "".join(f'{i},"two\nlines",{i}\n' for i in range(50))
        data = ("id,note,size\n" + rows).encode()
        processor = make_processor(FakeBucket({"in.csv": data}), 10)
        header, _ = processor.plan_csv_parts("in.csv", 10**6)
        result = processor.process_csv_range(
            "in.csv", len(header), len(data), header, "out.csv", lambda c: c
        )
        assert result["rows_read"] == 50
        assert result["lines"] == 100  # mismatch: the caller treats it as a split error


class TestProcessCsvTask:
    """process_csv uses parts for a file larger than one part, and one stream otherwise."""

    def run_task(self, data, part_size_mb, redis_clients):
        bucket = FakeBucket({"in.csv": data})
        app = Flask(__name__)
        app.config.update(
            MATERIALIZATION_UPLOAD_BUCKET_PATH=BUCKET, UPLOAD_CSV_PART_SIZE_MB=part_size_mb
        )
        with app.app_context(), \
                mock.patch.object(tasks, "GCSCsvProcessor", lambda *a, **k: make_processor(bucket, 50)), \
                mock.patch.object(tasks, "_run_csv_part_tasks", run_parts_inline):
            result = tasks.process_csv.run(
                file_path=f"gs://{BUCKET}/in.csv",
                schema_type="synapse",
                column_mapping=MAPPING_WITH_IDS,
                job_id_for_status=JOB,
            )
        return bucket, result

    def test_parts_and_one_stream_agree(self, redis_clients):
        data = synapse_csv(3000)
        # 0.01 MB parts split this ~150 KB file; 100 MB keeps it in one stream
        parts_bucket, parts_result = self.run_task(data, 0.01, redis_clients)
        one_bucket, one_result = self.run_task(data, 100, redis_clients)
        out = parts_result["output_path"].split("/", 1)[1]
        assert parts_bucket.compose_calls >= 1 and one_bucket.compose_calls == 0
        # outputs differ only in the "created" timestamp of each run
        def strip(output):
            # drop the second column, "created"
            return [
                (lambda f: f[0] + "," + f[2])(line.split(",", 2))
                for line in output.decode().splitlines()
            ]

        assert strip(parts_bucket.objects[out]) == strip(one_bucket.objects[one_result["output_path"].split("/", 1)[1]])
        assert parts_result["dropped_rows"] == one_result["dropped_rows"] > 0


class FakeAsyncResult:
    def __init__(self, states, result=None):
        self.id = f"task-{id(self)}"
        self._states = list(states)
        self.result = result
        self.revoked = False

    @property
    def state(self):
        return self._states[0] if len(self._states) == 1 else self._states.pop(0)

    def revoke(self):
        self.revoked = True


class TestRunCsvPartTasks:
    """_run_csv_part_tasks waits for every part, and stops on a cancel or a failure."""

    def run(self, async_results, before_poll=None):
        signatures = [mock.Mock(apply_async=mock.Mock(return_value=r)) for r in async_results]
        app = Flask(__name__)
        app.config["UPLOAD_CSV_PART_POLL_SECONDS"] = 0

        def sleep(_):
            if before_poll:
                before_poll()

        with app.app_context(), mock.patch.object(tasks.time, "sleep", sleep):
            return tasks._run_csv_part_tasks(signatures, JOB, "Processing CSV Data")

    def test_returns_results_in_order_and_reports_progress(self, redis_clients):
        results = [
            FakeAsyncResult(["PENDING", "SUCCESS"], {"rows_read": 10}),
            FakeAsyncResult(["STARTED", "STARTED", "SUCCESS"], {"rows_read": 20}),
        ]
        assert self.run(results) == [{"rows_read": 10}, {"rows_read": 20}]
        status = tasks.get_job_status(JOB)
        assert status["progress"] == 100.0 and status["processed_rows"] == 30
        assert status["total_chunks"] == 2

    def test_cancel_revokes_parts_still_waiting(self, redis_clients):
        done = FakeAsyncResult(["SUCCESS"], {"rows_read": 1})
        waiting = FakeAsyncResult(["PENDING"])
        with pytest.raises(tasks.UploadCancelled):
            self.run([done, waiting], before_poll=lambda: tasks.request_upload_cancel(JOB))
        assert waiting.revoked and not done.revoked

    def test_failed_part_fails_the_upload(self, redis_clients):
        failed = FakeAsyncResult(["FAILURE"], ValueError("bad"))
        waiting = FakeAsyncResult(["PENDING"])
        with pytest.raises(RuntimeError, match="failed"):
            self.run([waiting, failed])
        assert waiting.revoked
