"""The upload API with Ray-run uploads: start, status, cancel and the job lists read the
upload state database, and give the pages the same JSON as for a celery upload.

Uses the test PostGIS server (as the store tests) and, for the comparison with redis
checkpoints, redis on localhost:6390 (skipped without it).
"""

import datetime
import inspect
import json
import os
from unittest import mock

import pytest
import redis
from flask import Flask, g

from materializationengine.blueprints.upload import api, checkpoint_manager, ray_status
from materializationengine.rayjobs import submitter
from materializationengine.upload_state import store
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))
DATASTACK = "minnie65_phase3_v1"
FILE_METADATA = {
    "filename": "file.csv",
    "column_mapping": {"pre_pt_position": "pre"},
    "metadata": {"datastack_name": DATASTACK, "table_name": "synapses_test", "schema_type": "synapse"},
}


@pytest.fixture
def ray_on(engine, monkeypatch):
    monkeypatch.setenv("RAY_ENABLED", "true")
    monkeypatch.setenv("RAY_UPLOAD", "true")
    with mock.patch.object(ray_status.db, "get_engine", return_value=engine):
        yield engine


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(
        MATERIALIZATION_UPLOAD_BUCKET_PATH="bucket",
        SQLALCHEMY_DATABASE_URI="postgresql://x/y",
        STAGING_DATABASE_NAME="staging",
        SQL_INSTANCE_NAME="project:region:instance",
    )
    return app


def call(app, view, *args, json_body=None, user=1174):
    """Run an endpoint function without its auth decorators."""
    with app.test_request_context(json=json_body):
        g.auth_user = {"id": user}
        response = inspect.unwrap(view)(*args)
        if isinstance(response, tuple):
            response, code = response
        else:
            code = response.status_code
        return json.loads(response.get_data()), code


DATASTACK_INFO = {
    "datastack": DATASTACK,
    "aligned_volume": {"name": "minnie65_phase3"},
    "segmentation_source": "graphene://https://example/segmentation/table/minnie3_v1",
}


def start(app, taken=(), **submit):
    with mock.patch.object(api, "UploadRequestSchema") as schema, \
            mock.patch.object(api, "get_datastack_info", return_value=DATASTACK_INFO), \
            mock.patch.object(submitter, "submit_rayjob", return_value="upload-synapses-test-x1", **submit) as submit_mock, \
            mock.patch("materializationengine.blueprints.upload.production_build.names_in_use", return_value=list(taken)), \
            mock.patch("materializationengine.database.db_manager.get_engine"), \
            mock.patch.object(api, "process_and_upload") as celery:
        schema.return_value.load.return_value = json.loads(json.dumps(FILE_METADATA))
        body, code = call(app, api.start_csv_processing, json_body=FILE_METADATA)
    celery.s.assert_not_called()
    return body, code, submit_mock


class TestStart:
    def test_creates_the_job_and_submits_its_rayjob(self, ray_on, app):
        body, code, submit_mock = start(app)
        assert code == 200 and body["task_id"] == "upload-synapses-test-x1"
        job = store.get_job(ray_on, body["job_id"])
        assert (job["status"], job["rayjob_name"], job["user_id"]) == ("pending", "upload-synapses-test-x1", "1174")
        assert job["record"]["phase"] == ray_status.QUEUED_PHASE
        params = job["params"]
        assert params["file_path"] == "gs://bucket/file.csv"
        assert params["sql_instance_name"] == "project:region:instance"
        assert params["file_metadata"]["metadata"]["user_id"] == "1174"
        datetime.datetime.fromisoformat(params["materialization_time_stamp"])
        entrypoint, prefix = submit_mock.call_args.args
        assert entrypoint.endswith(f"--job-id {body['job_id']}")
        assert prefix == "upload-synapses_test"
        assert submit_mock.call_args.kwargs["metadata"] == {
            "materialize-workload": "bulk-upload", "upload-job": body["job_id"],
        }

    def test_the_tables_are_built_in_production_under_temporary_names(self, ray_on, app):
        body, code, _ = start(app)
        job = store.get_job(ray_on, body["job_id"])
        names = job["params"]["build_tables"]
        assert job["params"]["build_in_production"] is True
        assert names["final_annotation"] == "synapses_test"
        assert names["annotation"].startswith("upl") and names["annotation"].endswith("_synapses_test")
        assert names["segmentation"] == names["annotation"] + "__minnie3_v1"
        assert job["staging_database"] == "minnie65_phase3"
        assert job["params"]["table_meta"]["schema_type"] == FILE_METADATA["metadata"]["schema_type"]

    def test_a_table_name_taken_in_production_is_refused(self, ray_on, app):
        body, code, submit_mock = start(app, taken=["synapses_test"])
        assert code == 409 and "synapses_test" in body["message"]
        submit_mock.assert_not_called()

    def test_a_second_upload_into_the_same_table_is_refused(self, ray_on, app):
        start(app)
        with mock.patch.object(api, "make_upload_job_id", return_value=JOB + "_2"):
            body, code, submit_mock = start(app)
        assert code == 409 and "already running" in body["message"]
        submit_mock.assert_not_called()

    def test_a_failed_submission_fails_the_job(self, ray_on, app):
        body, code, _ = start(app, side_effect=submitter.RayJobSubmissionError("forbidden"))
        assert code == 500
        (job,) = store.list_jobs(ray_on)
        assert job["status"] == "error" and "forbidden" in job["error"]

    def test_celery_when_ray_uploads_are_off(self, engine, app, monkeypatch):
        monkeypatch.setenv("RAY_ENABLED", "true")
        monkeypatch.delenv("RAY_UPLOAD", raising=False)
        with mock.patch.object(api, "UploadRequestSchema") as schema, \
                mock.patch.object(api, "get_datastack_info", return_value={"datastack": DATASTACK}), \
                mock.patch.object(api, "update_job_status"), \
                mock.patch.object(api, "process_and_upload") as celery, \
                mock.patch.object(submitter, "submit_rayjob") as submit_mock:
            schema.return_value.load.return_value = json.loads(json.dumps(FILE_METADATA))
            celery.s.return_value.apply_async.return_value.id = "task-id"
            body, code = call(app, api.start_csv_processing, json_body=FILE_METADATA)
        assert body["task_id"] == "task-id"
        submit_mock.assert_not_called()


def rayjob(**status):
    return {"finished": False, "retrying": False, "failed_attempts": None, **status}


class TestStatus:
    def job(self, engine, **fields):
        new_job(engine, status="processing", phase="Uploading to Database")
        store.set_job_fields(engine, JOB, rayjob_name="upload-x", **fields)

    def status(self, app, rayjob_status=None, error=None):
        side_effect = error or (lambda name: rayjob_status)
        with mock.patch.object(submitter, "get_rayjob_status", side_effect=side_effect):
            return call(app, api.check_processing_status, JOB)

    def test_the_stored_record(self, ray_on, app):
        self.job(ray_on, driver_heartbeat_at=datetime.datetime.now(datetime.timezone.utc))
        body, code = self.status(app, rayjob(jobStatus="RUNNING"))
        assert code == 200
        assert (body["status"], body["phase"], body["item_type"]) == ("processing", "Uploading to Database", "rows")

    def test_a_rayjob_that_ended_without_the_driver_finishing_is_an_error(self, ray_on, app):
        self.job(ray_on)
        body, _ = self.status(app, rayjob(finished=True, jobDeploymentStatus="Failed", message="BackoffLimitExceeded"))
        assert body["status"] == "error" and "BackoffLimitExceeded" in body["error"]
        assert store.get_job(ray_on, JOB)["status"] == "error"

    def test_a_retrying_rayjob_says_so(self, ray_on, app):
        self.job(ray_on)
        body, _ = self.status(app, rayjob(retrying=True, jobDeploymentStatus="Retrying"))
        assert body["status"] == "processing"
        assert body["phase"].endswith("(retrying after a failed attempt)")

    def test_a_missing_rayjob_gets_a_grace_period(self, ray_on, app):
        self.job(ray_on)
        gone = submitter.RayJobSubmissionError("not found")
        api_error = Exception("Not Found")
        api_error.status = 404
        gone.__cause__ = api_error
        body, _ = self.status(app, error=gone)
        assert body["status"] == "processing"
        store.set_job_fields(ray_on, JOB, rayjob_checked_at=None)
        with mock.patch.object(ray_status, "RAYJOB_MISSING_GRACE_SECONDS", -1):
            body, _ = self.status(app, error=gone)
        assert body["status"] == "error" and "no longer exists" in body["error"]

    def test_kubernetes_is_asked_at_most_every_few_seconds(self, ray_on, app):
        self.job(ray_on, driver_heartbeat_at=datetime.datetime.now(datetime.timezone.utc))
        with mock.patch.object(submitter, "get_rayjob_status", return_value=rayjob()) as get:
            call(app, api.check_processing_status, JOB)
            call(app, api.check_processing_status, JOB)
        assert get.call_count == 1

    def test_other_users_jobs_need_permission(self, ray_on, app):
        self.job(ray_on, driver_heartbeat_at=datetime.datetime.now(datetime.timezone.utc))
        with mock.patch.object(submitter, "get_rayjob_status", return_value=rayjob()), \
                mock.patch.object(api, "is_auth_disabled", return_value=False), \
                mock.patch.object(api, "_has_datastack_permission", return_value=False):
            _, code = call(app, api.check_processing_status, JOB, user=99)
        assert code == 403


class TestCancel:
    def test_a_cancel_is_a_request_to_the_driver(self, ray_on, app):
        new_job(ray_on, status="processing")
        body, code = call(app, api.cancel_job, JOB)
        assert code == 200 and body["details"]["phase"] == "Cancelling"
        job = store.get_job(ray_on, JOB)
        assert job["cancel_requested_by"] == "1174"
        assert job["status"] == "processing"  # the driver sets cancelled once staging is gone

    def test_refused_once_the_transfer_started(self, ray_on, app):
        new_job(ray_on, status="processing")
        store.set_job_fields(ray_on, JOB, current_stage="transfer")
        body, code = call(app, api.cancel_job, JOB)
        assert code == 409 and "transfer" in body["message"]
        assert store.get_job(ray_on, JOB)["cancel_requested_at"] is None

    def test_redis_jobs_are_cancelled_as_before(self, ray_on, app):
        with mock.patch.object(api, "get_job_status", return_value={"user_id": "1174"}), \
                mock.patch.object(api, "request_upload_cancel", return_value={"status": "cancelled"}) as cancel:
            body, code = call(app, api.cancel_job, "celery-job")
        cancel.assert_called_once_with("celery-job")
        assert body["status"] == "success"


class TestJobLists:
    def test_user_jobs_include_ray_uploads(self, ray_on, app):
        new_job(ray_on, status="processing")
        redis_job = {"user_id": "1174", "datastack_name": DATASTACK, "last_updated": "2000-01-01T00:00:00"}
        fake_redis = mock.MagicMock()
        fake_redis.scan_iter.return_value = [b"csv_processing:celery-job"]
        fake_redis.get.return_value = json.dumps(redis_job).encode()
        with mock.patch.object(api, "REDIS_CLIENT", fake_redis):
            body, _ = call(app, api.get_user_jobs)
        assert [j["job_id"] for j in body["jobs"]] == [JOB, "celery-job"]
        assert body["jobs"][0]["phase"] == "Queued"


def fill_redis_checkpoint(manager, name):
    manager.initialize_workflow(name, JOB, DATASTACK)
    manager.update_workflow(
        table_name=name, status="processing_chunks", total_chunks=4,
        chunking_strategy="grid", used_chunk_size=2048, total_point_estimate=8,
    )
    manager.set_chunk_status(name, 0, "COMPLETED", {"rows_processed": 2})
    manager.set_chunk_status(name, 2, "FAILED_RETRYABLE", {"error_message": "blip", "attempt_count": 1})


def fill_store(engine, name):
    new_job(engine, status="processing")
    store.init_spatial(
        engine, JOB, database_name="staging", workflow_name=name, datastack_name=DATASTACK,
        status="processing_chunks", total_chunks=4, chunking_strategy="grid", used_chunk_size=2048,
        total_point_estimate=8, mat_metadata={},
    )
    store.record_chunk_results(engine, JOB, [
        {"chunk_index": 0, "status": "COMPLETED", "attempt_count": 1, "points": 2},
        {"chunk_index": 2, "status": "FAILED_RETRYABLE", "attempt_count": 1,
         "error_message": "blip", "error_type": "ConnectionError"},
    ])
    store.update_spatial(engine, JOB, completed_chunks=1, rows_processed=2)


@pytest.fixture
def real_redis():
    client = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=12)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    client.flushdb()
    with mock.patch.object(checkpoint_manager, "REDIS_CLIENT", client):
        yield client
    client.flushdb()


class TestSameAsRedis:
    """A workflow in the upload state database reads like the same one in redis."""

    VOLATILE = {"updated_at", "created_at", "start_time", "task_id", "rate_samples",
                "processing_rate", "estimated_completion", "last_error", "last_failure_time"}

    def views(self, engine):
        fill_redis_checkpoint(checkpoint_manager.RedisCheckpointManager("staging"), "wf_redis")
        fill_store(engine, "wf_store")
        return (
            checkpoint_manager.RedisCheckpointManager("staging"),
            ray_status.StoreCheckpointView(engine, "staging"),
        )

    def test_workflow_data(self, ray_on, real_redis):
        from dataclasses import asdict

        redis_view, store_view = self.views(ray_on)
        redis_wf = asdict(redis_view.get_workflow_data("wf_redis"))
        store_wf = asdict(store_view.get_workflow_data("wf_store"))
        for wf in (redis_wf, store_wf):
            wf.pop("table_name")
            for key in self.VOLATILE:
                wf.pop(key, None)
        assert store_wf == redis_wf

    def test_chunk_statuses_and_failures(self, ray_on, real_redis):
        redis_view, store_view = self.views(ray_on)
        assert store_view.get_all_chunk_statuses("wf_store") == redis_view.get_all_chunk_statuses("wf_redis")
        redis_failed = redis_view.get_all_failed_chunk_details("wf_redis")
        store_failed = store_view.get_all_failed_chunk_details("wf_store")
        assert set(store_failed) == set(redis_failed)
        assert set(redis_failed["2"]) <= set(store_failed["2"])
        for key in ("error_message", "attempt_count", "status"):
            assert store_failed["2"][key] == redis_failed["2"][key]

    def test_active_workflows_include_both(self, ray_on, real_redis):
        self.views(ray_on)
        workflows = ray_status.active_workflows("staging")
        assert {wf["table_name"] for wf in workflows} == {"wf_redis", "wf_store"}
        redis_keys = set(next(wf for wf in workflows if wf["table_name"] == "wf_redis"))
        store_keys = set(next(wf for wf in workflows if wf["table_name"] == "wf_store"))
        assert store_keys == redis_keys

    def test_the_workflow_details_endpoint(self, ray_on, real_redis, app):
        self.views(ray_on)
        app.config["DATASTACKS"] = [DATASTACK]
        view = inspect.unwrap(api.SpatialLookupWorkflowDetails.get)
        bodies = {}
        for name in ("wf_redis", "wf_store"):
            with app.test_request_context("/?database_name=staging"):
                bodies[name] = view(api.SpatialLookupWorkflowDetails(), DATASTACK, name)
        redis_body, store_body = bodies["wf_redis"], bodies["wf_store"]
        assert store_body["status"] == "success"
        assert store_body["status_counts"] == redis_body["status_counts"]
        assert [c["status"] for c in store_body["chunks"]] == [c["status"] for c in redis_body["chunks"]]
        assert store_body["chunks"][2]["error_type"] == "ConnectionError"
        assert redis_body["chunks"][2]["error_type"] == "N/A"
        assert set(store_body["workflow_summary"]) == set(redis_body["workflow_summary"])

    def test_the_newer_run_of_a_workflow_wins(self, ray_on, real_redis):
        fill_store(ray_on, "wf")
        assert isinstance(ray_status.checkpoint_reader("staging", "wf"), ray_status.StoreCheckpointView)
        fill_redis_checkpoint(checkpoint_manager.RedisCheckpointManager("staging"), "wf")  # newer
        assert isinstance(ray_status.checkpoint_reader("staging", "wf"), checkpoint_manager.RedisCheckpointManager)
