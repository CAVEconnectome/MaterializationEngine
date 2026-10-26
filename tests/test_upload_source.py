"""Reading an upload's CSV in place from a gs:// or public s3:// location."""

import io
from datetime import datetime
from unittest import mock

import pytest
from botocore.exceptions import ClientError
from flask import Flask
from google.api_core import exceptions as google_exceptions

from materializationengine.blueprints.upload import api, source as source_module
from materializationengine.blueprints.upload.schemas import UploadRequestSchema
from materializationengine.blueprints.upload.source import (
    GCSSource,
    S3Source,
    SourceAccessError,
    parse_source_uri,
)
from materializationengine.blueprints.upload.processor import SchemaProcessor
from tests.test_upload_csv_parts import (
    MAPPING_WITH_IDS,
    FakeBucket,
    make_processor,
    synapse_csv,
)


class FakeS3:
    """The subset of a boto3 S3 client that S3Source uses."""

    def __init__(self, objects, forbidden=()):
        self.objects = objects
        self.forbidden = set(forbidden)
        self.ranges = []

    def _check(self, bucket, key):
        if (bucket, key) in self.forbidden:
            raise ClientError({"Error": {"Code": "403"}}, "HeadObject")
        if (bucket, key) not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def head_object(self, Bucket, Key):
        self._check(Bucket, Key)
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def get_object(self, Bucket, Key, Range):
        self._check(Bucket, Key)
        self.ranges.append(Range)
        start, end = (int(x) for x in Range.removeprefix("bytes=").split("-"))
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)][start : end + 1])}


class FakeClient:
    def __init__(self, buckets):
        self.buckets = buckets

    def bucket(self, name):
        return self.buckets[name]


class TestParseSourceUri:
    @pytest.mark.parametrize(
        "uri, expected",
        [
            ("gs://b/k.csv", ("gs", "b", "k.csv")),
            ("s3://b/dir/k.csv", ("s3", "b", "dir/k.csv")),
            ("  S3://b/k.csv ", ("s3", "b", "k.csv")),
        ],
    )
    def test_valid(self, uri, expected):
        assert parse_source_uri(uri) == expected

    @pytest.mark.parametrize(
        "uri", ["", "k.csv", "https://b/k.csv", "gs://b", "gs://b/", "s3:///k.csv", "gs://b/dir/"]
    )
    def test_invalid(self, uri):
        with pytest.raises(ValueError):
            parse_source_uri(uri)


class TestS3Source:
    def test_size_and_ranged_reads(self):
        client = FakeS3({("b", "k.csv"): b"0123456789"})
        src = S3Source("b", "k.csv", "s3://b/k.csv", client=client)
        assert src.size == 10
        assert src.read(2, 5) == b"234"
        assert src.read(5, 5) == b""
        with src.open_range(7, 10) as stream:
            assert stream.read() == b"789"
        assert client.ranges == ["bytes=2-4", "bytes=7-9"]

    def test_missing_and_private(self):
        client = FakeS3({("b", "private.csv"): b"x"}, forbidden=[("b", "private.csv")])
        with pytest.raises(FileNotFoundError):
            S3Source("b", "nope.csv", "s3://b/nope.csv", client=client)
        with pytest.raises(SourceAccessError, match="publicly readable"):
            S3Source("b", "private.csv", "s3://b/private.csv", client=client)


class TestProcessorSources:
    """GCSCsvProcessor.source picks where to read from, and reads the same bytes."""

    def test_routing(self):
        own = FakeBucket({"in.csv": b"a\n1\n"})
        other = FakeBucket({"dir/in.csv": b"a\n2\n"})
        other.name = "public-bucket"
        processor = make_processor(own)
        processor.client = FakeClient({"public-bucket": other})
        assert processor.source("in.csv").read(0, 4) == b"a\n1\n"
        assert processor.source("gs://uploads/in.csv").read(0, 4) == b"a\n1\n"
        assert processor.source("gs://public-bucket/dir/in.csv").read(0, 4) == b"a\n2\n"
        with mock.patch.object(source_module, "anonymous_s3_client",
                               return_value=FakeS3({("pub", "k.csv"): b"a\n3\n"})):
            assert processor.source("s3://pub/k.csv").read(0, 4) == b"a\n3\n"
        with pytest.raises(FileNotFoundError):
            processor.source("gs://public-bucket/missing.csv")

    def test_gcs_forbidden_is_an_access_error(self):
        bucket = mock.Mock(get_blob=mock.Mock(side_effect=google_exceptions.Forbidden("no")))
        with pytest.raises(SourceAccessError):
            GCSSource(bucket, "k.csv", "gs://private/k.csv")

    def test_s3_parts_match_gcs_parts(self):
        data = synapse_csv(800)
        gcs = make_processor(FakeBucket({"in.csv": data}), 50)
        s3_processor = make_processor(FakeBucket(), 50)
        fake_s3 = FakeS3({("pub", "in.csv"): data})
        schema_processor = SchemaProcessor("synapse", column_mapping=MAPPING_WITH_IDS)
        created = datetime(2026, 10, 24)

        def transform(chunk):
            return schema_processor.process_chunk(chunk, created)

        with mock.patch.object(source_module, "anonymous_s3_client", return_value=fake_s3):
            header, ranges = s3_processor.plan_csv_parts("s3://pub/in.csv", 5000)
            assert (header, ranges) == gcs.plan_csv_parts("in.csv", 5000)
            for i, (start, end) in enumerate(ranges):
                result = s3_processor.process_csv_range(
                    "s3://pub/in.csv", start, end, header, f"part-{i}.csv", transform
                )
                assert result["rows_read"] == result["lines"]
            s3_processor.process_csv_in_chunks("s3://pub/in.csv", "one.csv", transform)
        gcs.process_csv_in_chunks("in.csv", "one.csv", transform)
        joined = b"".join(s3_processor.bucket.objects[f"part-{i}.csv"] for i in range(len(ranges)))
        assert joined == s3_processor.bucket.objects["one.csv"] == gcs.bucket.objects["one.csv"]


class TestInspectEndpoint:
    def call(self, uri, processor):
        app = Flask(__name__)
        app.config["MATERIALIZATION_UPLOAD_BUCKET_PATH"] = "uploads"
        app.register_blueprint(api.upload_bp)
        with mock.patch.object(api, "GCSCsvProcessor", return_value=processor):
            response = app.test_client().post(
                "/materialize/upload/api/source/inspect/minnie65_phase3_v1", json={"uri": uri}
            )
        return response.status_code, response.json

    def test_preview(self):
        own = FakeBucket()
        public = FakeBucket({"syn.csv": b'id,pre_x,note\n1,2,"a, b"\n3,4,c\n'})
        processor = make_processor(own)
        processor.client = FakeClient({"pub": public})
        status, body = self.call("gs://pub/syn.csv", processor)
        assert status == 200
        assert body["filename"] == "syn.csv" and body["size"] == len(public.objects["syn.csv"])
        assert body["previewRows"] == [["id", "pre_x", "note"], ["1", "2", "a, b"]]

    def test_preview_of_a_large_file_drops_the_cut_off_line(self, monkeypatch):
        monkeypatch.setattr(api, "SOURCE_PREVIEW_BYTES", 20)
        processor = make_processor(FakeBucket({"big.csv": b"id,x\n1,22222\n3,44444444444\n"}))
        status, body = self.call("gs://uploads/big.csv", processor)
        assert status == 200 and body["previewRows"] == [["id", "x"], ["1", "22222"]]

    @pytest.mark.parametrize(
        "uri, code",
        [("in.csv", 400), ("ftp://b/in.csv", 400), ("gs://uploads/in.txt", 404),
         ("gs://uploads/missing.csv", 404)],
    )
    def test_errors(self, uri, code):
        processor = make_processor(FakeBucket({"in.csv": b"a\n1\n"}))
        status, body = self.call(uri, processor)
        assert status == code and body["status"] == "error"

    def test_not_a_csv(self):
        processor = make_processor(FakeBucket({"in.txt": b"a\n1\n"}))
        status, body = self.call("gs://uploads/in.txt", processor)
        assert status == 400 and "CSV" in body["message"]

    def test_private_gcs_names_the_service_account(self):
        processor = make_processor(FakeBucket())
        private = mock.Mock(get_blob=mock.Mock(side_effect=google_exceptions.Forbidden("no")))
        processor.client = mock.Mock(
            bucket=mock.Mock(return_value=private),
            _credentials=mock.Mock(service_account_email="mat@proj.iam.gserviceaccount.com"),
        )
        status, body = self.call("gs://private/in.csv", processor)
        assert status == 403
        assert "mat@proj.iam.gserviceaccount.com" in body["message"]


class TestUploadRequestSchema:
    BASE = {
        "filename": "syn.csv",
        "columnMapping": {},
        "metadata": {
            "schema_type": "synapse",
            "datastack_name": "minnie65_phase3_v1",
            "table_name": "t",
            "description": "d",
            "voxel_resolution_nm_x": 4,
            "voxel_resolution_nm_y": 4,
            "voxel_resolution_nm_z": 40,
            "read_permission": "PUBLIC",
            "write_permission": "PRIVATE",
        },
    }

    def test_source_uri_is_optional_and_validated(self):
        assert UploadRequestSchema().load(self.BASE).get("source_uri") is None
        loaded = UploadRequestSchema().load({**self.BASE, "sourceUri": "s3://pub/syn.csv"})
        assert loaded["source_uri"] == "s3://pub/syn.csv"
        with pytest.raises(Exception):
            UploadRequestSchema().load({**self.BASE, "sourceUri": "https://x/syn.csv"})
