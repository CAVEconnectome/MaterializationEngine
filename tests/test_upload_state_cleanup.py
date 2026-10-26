"""The upload state database's daily cleanup: dead drivers, leftover staging, old RayJobs."""

from unittest import mock

import pytest
from flask import Flask
from sqlalchemy import text

from materializationengine.blueprints.upload import tasks
from materializationengine.rayjobs import submitter
from materializationengine.upload_state import cleanup, store
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(STAGING_DATABASE_NAME="staging")
    with app.app_context():
        yield app


def age(engine, job_id, minutes=0, hours=0):
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE upload_jobs SET driver_heartbeat_at = now() - make_interval(mins => :m), "
                "finished_at = now() - make_interval(hours => :h), "
                "created_at = now() - make_interval(hours => :h) WHERE job_id = :j"
            ),
            {"m": minutes, "h": hours, "j": job_id},
        )


class TestReconcile:
    def jobs(self, engine):
        for suffix, table in (("_a", "t_a"), ("_b", "t_b"), ("_c", "t_c"), ("_d", "t_d")):
            new_job(engine, JOB + suffix, table=table, status="processing")
            store.set_job_fields(engine, JOB + suffix, rayjob_name=f"upload{suffix}")
            age(engine, JOB + suffix, minutes=30)
        store.set_job_fields(engine, JOB + "_d", rayjob_name=None)

    def test_ends_only_jobs_whose_rayjob_is_over(self, engine):
        self.jobs(engine)
        statuses = {
            "upload_a": {"finished": True, "jobDeploymentStatus": "Failed", "jobStatus": "FAILED"},
            "upload_c": {"finished": False, "jobDeploymentStatus": "Retrying", "jobStatus": "FAILED"},
        }
        with mock.patch.object(submitter, "rayjob_exists", side_effect=lambda n: n != "upload_b"), \
                mock.patch.object(submitter, "get_rayjob_status", side_effect=statuses.__getitem__):
            ended = cleanup.reconcile(engine, stale_minutes=10, dry_run=False)
        assert sorted(ended) == [JOB + "_a", JOB + "_b", JOB + "_d"]
        a = store.get_job(engine, JOB + "_a")
        assert (a["status"], a["retryable"]) == ("error", False)
        assert "ended (Failed)" in a["record"]["error"]
        assert store.get_job(engine, JOB + "_c")["status"] == "processing"  # being retried

    def test_a_live_driver_is_left_alone(self, engine):
        new_job(engine, status="processing")
        store.acquire_lease(engine, JOB, "head")
        with mock.patch.object(submitter, "rayjob_exists") as exists:
            assert cleanup.reconcile(engine, 10, False) == []
        exists.assert_not_called()


class TestStagingCleanup:
    def test_drops_leftover_staging_after_the_grace_period(self, engine, app):
        new_job(engine, JOB + "_old", table="t_old", status="processing")
        new_job(engine, JOB + "_new", table="t_new", status="processing")
        for job_id in (JOB + "_old", JOB + "_new"):
            store.mark_terminal_if_active(engine, job_id, "error", "driver died")
        age(engine, JOB + "_old", hours=72)
        with mock.patch.object(tasks, "drop_upload_tables", return_value=["t_old"]) as drop, \
                mock.patch.object(tasks.db_manager, "get_engine"):
            assert cleanup.drop_failed_staging(engine, 48, False) == [JOB + "_old"]
            # recorded on the job, so the next run does not drop them again
            assert cleanup.drop_failed_staging(engine, 48, False) == []
        assert drop.call_args.args[0] == "t_old"
        assert store.get_job_record(engine, JOB + "_old")["staging_cleanup"] == "done"


class TestOldRayJobs:
    def test_deletes_finished_upload_rayjobs_past_retention(self):
        listing = [
            {"name": "old", "finished": True, "endTime": "2026-10-01T00:00:00Z"},
            {"name": "recent", "finished": True, "endTime": "2099-01-01T00:00:00Z"},
            {"name": "running", "finished": False, "endTime": None},
        ]
        with mock.patch.object(submitter, "list_rayjobs", return_value=listing) as lister, \
                mock.patch.object(submitter, "delete_rayjob") as delete:
            assert cleanup.delete_old_rayjobs(7, False) == ["old"]
        assert lister.call_args.args[0] == "materialize-workload=bulk-upload"
        delete.assert_called_once_with("old")
