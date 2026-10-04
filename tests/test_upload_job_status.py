"""The upload job list shows a job as soon as /api/process/start is called, before
the orchestration worker that runs process_and_upload has been scheduled."""

import json
from datetime import datetime
from unittest import mock

from flask import Flask, g

from materializationengine.blueprints.upload import api, tasks


class FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value, ex=None):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return int(key in self.data)


def _run_process_and_upload(**kwargs):
    """Run process_and_upload with the chain mocked out; return the job id it used."""
    file_metadata = {
        "metadata": {"table_name": "synapse_1M_test", "schema_type": "synapse"},
        "column_mapping": {},
    }
    workflow = mock.MagicMock()
    workflow.apply_async.return_value.id = "chain-id"
    app = Flask(__name__)
    with app.app_context(), mock.patch.object(
        tasks, "chain", return_value=workflow
    ), mock.patch.object(tasks, "update_job_status") as mock_update, mock.patch.object(
        tasks, "is_upload_cancelled", return_value=False
    ):
        tasks.process_and_upload.run(
            "gs://bucket/file.csv",
            file_metadata,
            {"datastack": "minnie65_phase3_v1"},
            user_id="121",
            **kwargs,
        )
    return mock_update.call_args.args[0]


class TestUploadJobStatus:
    """Job records are written as soon as processing is started."""

    def test_make_upload_job_id(self):
        job_id = tasks.make_upload_job_id(
            "minnie65_phase3_v1", "synapse_1M_test", datetime(2026, 10, 2, 15, 45, 35)
        )
        assert job_id == "minnie65_phase3_v1_synapse_1M_test_20261002_154535"

    def test_update_job_status_keeps_status_when_update_omits_it(self):
        fake = FakeRedis()
        with mock.patch.object(tasks, "REDIS_CLIENT", fake):
            tasks.update_job_status(
                "job1", {"status": "pending", "user_id": "121", "datastack_name": "ds"}
            )
            # process_and_upload's "chain initialized" update has no status of its own
            tasks.update_job_status("job1", {"phase": "Workflow Chain Initialized"})
            record = tasks.get_job_status("job1")

        assert record["status"] == "pending"
        assert record["phase"] == "Workflow Chain Initialized"
        assert record["user_id"] == "121"
        assert record["datastack_name"] == "ds"

    def test_update_job_status_overwrites_status_when_given(self):
        fake = FakeRedis()
        with mock.patch.object(tasks, "REDIS_CLIENT", fake):
            tasks.update_job_status("job1", {"status": "pending"})
            tasks.update_job_status("job1", {"status": "processing"})
            assert tasks.get_job_status("job1")["status"] == "processing"

    def test_process_and_upload_uses_job_id_from_api(self):
        job_id = _run_process_and_upload(
            job_id="minnie65_phase3_v1_synapse_1M_test_20261002_154535"
        )
        assert job_id == "minnie65_phase3_v1_synapse_1M_test_20261002_154535"

    def test_process_and_upload_generates_job_id_without_one(self):
        job_id = _run_process_and_upload()
        assert job_id.startswith("minnie65_phase3_v1_synapse_1M_test_")

    def test_start_writes_pending_record_before_enqueueing(self):
        app = Flask(__name__)
        app.config.update(
            MATERIALIZATION_UPLOAD_BUCKET_PATH="bucket",
            SQLALCHEMY_DATABASE_URI="postgresql://x/y",
            STAGING_DATABASE_NAME="staging",
        )
        file_metadata = {
            "filename": "file.csv",
            "metadata": {"datastack_name": "minnie65_phase3_v1", "table_name": "synapse_1M_test"},
        }
        calls = mock.MagicMock()
        calls.process_and_upload.s.return_value.apply_async.return_value.id = "task-id"

        with app.test_request_context(json=file_metadata), mock.patch.object(
            api, "UploadRequestSchema"
        ) as mock_schema, mock.patch.object(
            api, "get_datastack_info", return_value={"datastack": "minnie65_phase3_v1"}
        ), mock.patch.object(
            api, "update_job_status", calls.update_job_status
        ), mock.patch.object(
            api, "process_and_upload", calls.process_and_upload
        ):
            mock_schema.return_value.load.return_value = file_metadata
            g.auth_user = {"id": 121}
            response = api.start_csv_processing.__wrapped__()

        body = json.loads(response.get_data())
        job_id = body["job_id"]
        assert job_id.startswith("minnie65_phase3_v1_synapse_1M_test_")
        assert body["task_id"] == "task-id"

        names = [name for name, *_ in calls.mock_calls]
        assert names.index("update_job_status") < names.index("process_and_upload.s().apply_async")

        record_job_id, record = calls.update_job_status.call_args.args
        assert record_job_id == job_id
        assert record["status"] == "pending"
        assert record["user_id"] == "121"
        assert record["datastack_name"] == "minnie65_phase3_v1"

        assert calls.process_and_upload.s.call_args.kwargs["job_id"] == job_id
