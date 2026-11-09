"""pg_repack table maintenance: ordering report, preconditions, and the repack task.

pg_repack itself is mocked (its extension is not in the test image); the generated
command was checked end to end against Postgres 18 with pg_repack 1.5.3. Needs redis on
localhost:6390 (`docker run -d --rm -p 6390:6379 redis:7`), or those tests are skipped.
"""

import json
import os
import subprocess
import uuid
from unittest import mock

import pytest
import redis
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from materializationengine.workflows import table_maintenance as tm

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))


@pytest.fixture
def scratch_db(mat_metadata):
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    name = f"repack_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    with engine.begin() as conn:
        conn.execute("CREATE TABLE annotation_table_metadata (table_name varchar PRIMARY KEY)")
        conn.execute("CREATE TABLE segmentation_table_metadata (table_name varchar PRIMARY KEY, annotation_table varchar)")
        conn.execute("CREATE TABLE synapses (id bigint PRIMARY KEY, v text)")
        conn.execute("INSERT INTO synapses SELECT x, md5(x::text) FROM (SELECT x FROM generate_series(1, 20000) x ORDER BY random()) s")
        conn.execute("CREATE TABLE synapses__v1 (id bigint PRIMARY KEY, root bigint)")
        conn.execute("INSERT INTO synapses__v1 SELECT x, x FROM generate_series(1, 20000) x")
        conn.execute("CREATE TABLE no_pkey (id bigint)")
        conn.execute("INSERT INTO annotation_table_metadata VALUES ('synapses'), ('no_pkey')")
        conn.execute("INSERT INTO segmentation_table_metadata VALUES ('synapses__v1', 'synapses')")
        conn.execute("ANALYZE")
    with mock.patch.object(tm.db_manager, "get_engine", return_value=engine):
        yield name, engine
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


@pytest.fixture
def real_redis():
    client = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=13)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    client.flushdb()
    with mock.patch.object(tm, "REDIS_CLIENT", client):
        yield client
    client.flushdb()


class TestOrderReport:
    def test_reports_ordering_of_annotation_and_segmentation_tables(self, scratch_db):
        name, _ = scratch_db
        report = {t["table_name"]: t for t in tm.table_order_report(name, min_rows=1000)}
        assert set(report) == {"synapses", "synapses__v1"}
        assert report["synapses"]["kind"] == "annotation" and abs(report["synapses"]["id_correlation"]) < 0.2
        assert report["synapses__v1"]["kind"] == "segmentation" and report["synapses__v1"]["id_correlation"] == 1.0

    def test_max_correlation_lists_only_poorly_ordered_tables(self, scratch_db):
        name, _ = scratch_db
        assert [t["table_name"] for t in tm.table_order_report(name, min_rows=1000, max_correlation=0.5)] == ["synapses"]

    def test_frozen_database_lists_tables_from_materializedmetadata(self, scratch_db):
        # A frozen copy leaves the live metadata tables empty and lists its tables here.
        name, engine = scratch_db
        with engine.begin() as conn:
            conn.execute("DELETE FROM annotation_table_metadata")
            conn.execute("DELETE FROM segmentation_table_metadata")
            conn.execute("CREATE TABLE materializedmetadata (id serial PRIMARY KEY, table_name varchar, row_count bigint)")
            conn.execute("INSERT INTO materializedmetadata (table_name, row_count) VALUES ('synapses', 20000)")
        report = {t["table_name"]: t["kind"] for t in tm.table_order_report(name, min_rows=1000)}
        assert report == {"synapses": "annotation", "synapses__v1": "segmentation"}

    @pytest.mark.parametrize("database", ["postgres", "cloudsqladmin", "bad;name", ""])
    def test_refuses_system_and_unsafe_database_names(self, database):
        with pytest.raises(tm.RepackRefused):
            tm.check_database(database)


class TestRepackCommand:
    def test_flags_cloud_sql_needs(self):
        url = make_url("postgresql://u:secret@127.0.0.1:3306/minnie65_phase3")
        cmd = tm.build_repack_command(url, "synapses_pni_2", "id", 4, dry_run=True, wait_timeout=30)
        assert "--no-superuser-check" in cmd and "--no-kill-backend" in cmd and "--wait-timeout=30" in cmd
        assert '--table=public."synapses_pni_2"' in cmd and "--order-by=id" in cmd
        assert "--jobs=4" in cmd and "--dry-run" in cmd
        assert "--port=3306" in cmd and "--dbname=minnie65_phase3" in cmd
        assert not any("secret" in part for part in cmd)  # password goes in PGPASSWORD


def _run(name, table="synapses", **kwargs):
    return tm.repack_table.run(job_id="job1", database=name, table_name=table, **kwargs)


@pytest.fixture
def repack_tools():
    """Pretend the client is 1.5.3 and the extension is installed, and capture the pg_repack call."""
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return subprocess.CompletedProcess(cmd, 0, stdout="INFO: repacking table", stderr="")

    with mock.patch.object(tm, "_client_version", return_value="1.5.3"), \
            mock.patch.object(tm, "_ensure_extension", return_value="1.5.3"), \
            mock.patch.object(tm.subprocess, "run", side_effect=fake_run):
        yield calls


class TestRepackTask:
    def test_repack_runs_pg_repack_then_flags_and_analyzes(self, scratch_db, real_redis, repack_tools):
        name, engine = scratch_db
        status = _run(name, dry_run=False, fillfactor=90)
        assert status["state"] == "done", status
        cmd, kwargs = repack_tools[0]
        assert "--dry-run" not in cmd and kwargs["env"]["PGPASSWORD"] == engine.url.password
        with engine.connect() as conn:
            assert conn.execute("SELECT indisclustered FROM pg_index WHERE indrelid='synapses'::regclass AND indisprimary").scalar()
            assert conn.execute("SELECT reloptions FROM pg_class WHERE relname='synapses'").scalar() == ["fillfactor=90"]
        assert status["before"]["primary_key"] == "synapses_pkey" and "after" in status
        assert tm.get_repack_status("job1")["state"] == "done"
        assert not real_redis.exists(f"{tm.LOCK_KEY_PREFIX}{name}")  # lock released

    def test_dry_run_changes_nothing(self, scratch_db, real_redis, repack_tools):
        name, engine = scratch_db
        status = _run(name, dry_run=True, fillfactor=90)
        assert status["state"] == "dry run" and "--dry-run" in repack_tools[0][0]
        with engine.connect() as conn:
            assert not conn.execute("SELECT indisclustered FROM pg_index WHERE indrelid='synapses'::regclass AND indisprimary").scalar()
            assert conn.execute("SELECT reloptions FROM pg_class WHERE relname='synapses'").scalar() is None

    def test_dry_run_without_the_extension_does_not_install_it(self, scratch_db, real_redis):
        name, _ = scratch_db
        with mock.patch.object(tm, "_client_version", return_value="1.5.3"), \
                mock.patch.object(tm.subprocess, "run") as run:
            status = _run(name, dry_run=True)
        assert status["state"] == "dry run" and status["would_install_extension"]
        run.assert_not_called()

    @pytest.mark.parametrize(
        "table, kwargs, reason",
        [
            ("no_pkey", {}, "no primary key"),
            ("missing", {}, "not found"),
            ("synapses", {"max_table_gb": 0}, "force=true"),
        ],
    )
    def test_refusals_change_nothing(self, scratch_db, real_redis, repack_tools, table, kwargs, reason):
        name, _ = scratch_db
        status = _run(name, table=table, dry_run=False, **kwargs)
        assert status["state"] == "refused" and reason in status["reason"]
        assert repack_tools == []

    def test_large_table_with_force_runs(self, scratch_db, real_redis, repack_tools):
        name, _ = scratch_db
        assert _run(name, dry_run=False, max_table_gb=0, force=True)["state"] == "done"

    def test_version_mismatch_refuses(self, scratch_db, real_redis):
        name, _ = scratch_db
        with mock.patch.object(tm, "_client_version", return_value="1.5.3"), \
                mock.patch.object(tm, "_ensure_extension", return_value="1.5.2"), \
                mock.patch.object(tm.subprocess, "run") as run:
            status = _run(name, dry_run=False)
        assert status["state"] == "refused" and "does not match" in status["reason"]
        run.assert_not_called()

    def test_one_repack_per_database(self, scratch_db, real_redis, repack_tools):
        name, _ = scratch_db
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}{name}", "other-job")
        status = _run(name, dry_run=False)
        assert status["state"] == "refused" and "another repack" in status["reason"]
        assert real_redis.get(f"{tm.LOCK_KEY_PREFIX}{name}") == b"other-job"  # not released by us

    def test_pg_repack_failure_is_reported(self, scratch_db, real_redis):
        name, _ = scratch_db
        failed = subprocess.CompletedProcess([], 1, stdout="", stderr="ERROR: could not get lock")
        with mock.patch.object(tm, "_client_version", return_value="1.5.3"), \
                mock.patch.object(tm, "_ensure_extension", return_value="1.5.3"), \
                mock.patch.object(tm.subprocess, "run", return_value=failed):
            status = _run(name, dry_run=False)
        assert status["state"] == "failed" and "could not get lock" in status["output"]
        assert not real_redis.exists(f"{tm.LOCK_KEY_PREFIX}{name}")


class TestKubernetesJobs:
    """start_repack submits a Job where the deployment runs them; a dead Job is a failure."""

    @pytest.fixture
    def jobs_on(self, monkeypatch):
        from materializationengine import k8s_jobs

        monkeypatch.setenv("MAINTENANCE_JOBS_ENABLED", "true")
        with mock.patch.object(k8s_jobs, "submit_job", return_value="repack-synapses-abcde") as submit, \
                mock.patch.object(k8s_jobs, "get_job_status") as status, \
                mock.patch.object(tm.repack_table, "apply_async") as celery_task:
            yield submit, status, celery_task

    def test_submits_a_job_instead_of_a_celery_task(self, scratch_db, real_redis, jobs_on):
        submit, status, celery_task = jobs_on
        name, _ = scratch_db
        status.return_value = {"finished": False, "active": False, "reason": None}
        recorded = []
        submit.side_effect = lambda module, kwargs, name, **k: recorded.append(
            json.loads(real_redis.get(f"{tm.STATUS_KEY_PREFIX}{kwargs['job_id']}")).get("k8s_job")) or name
        out = tm.start_repack(name, "synapses", dry_run=False, jobs=2)
        celery_task.assert_not_called()
        module, kwargs = submit.call_args.args[:2]
        assert module == "materializationengine.jobs.repack"
        assert kwargs["database"] == name and kwargs["table_name"] == "synapses" and kwargs["order_by"] == "id"
        assert kwargs["dry_run"] is False and kwargs["job_id"] == out["job_id"]
        assert submit.call_args.kwargs["labels"]["materialize-workload"] == "repack"
        assert out["state"] == "queued" and out["backend"] == "kubernetes job"
        assert out["k8s_job"].startswith("repack-synapses-") and out["k8s_job"] == submit.call_args.kwargs["name"]
        assert recorded == [out["k8s_job"]]  # in redis before the Job was created
        assert out["k8s_job_status"] == "pending"

    def test_celery_without_jobs(self, scratch_db, real_redis, monkeypatch):
        monkeypatch.delenv("MAINTENANCE_JOBS_ENABLED", raising=False)
        name, _ = scratch_db
        with mock.patch.object(tm.repack_table, "apply_async") as celery_task:
            out = tm.start_repack(name, "synapses")
        assert celery_task.call_args.kwargs["kwargs"]["job_id"] == out["job_id"] and out["backend"] == "celery"

    def test_refused_up_front_while_another_repack_holds_the_database(self, scratch_db, real_redis, jobs_on):
        submit, _, _ = jobs_on
        name, _ = scratch_db
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}{name}", "other-job")
        with pytest.raises(tm.RepackRefused, match="other-job"):
            tm.start_repack(name, "synapses")
        submit.assert_not_called()

    def test_submission_failure_is_recorded(self, scratch_db, real_redis, jobs_on):
        from materializationengine import k8s_jobs

        submit, status, _ = jobs_on
        submit.side_effect = k8s_jobs.JobSubmissionError("forbidden")
        status.return_value = {"exists": False, "finished": True, "failed": True}
        out = tm.start_repack(scratch_db[0], "synapses")
        assert out["state"] == "failed" and "forbidden" in out["error"]

    def test_a_job_created_despite_an_error_is_kept(self, scratch_db, real_redis, jobs_on):
        from materializationengine import k8s_jobs

        submit, status, _ = jobs_on
        submit.side_effect = k8s_jobs.JobSubmissionError("read timed out")
        status.return_value = {"exists": True, "finished": False, "active": True, "reason": None}
        out = tm.start_repack(scratch_db[0], "synapses")
        assert out["state"] == "queued" and out["k8s_job"] and "the Job exists" in out["submit_warning"]

    def test_a_killed_job_mid_repack_is_failed_with_cleanup_and_frees_the_database(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        name, _ = scratch_db
        tm._set_status("j1", state="running", database=name, table_name="synapses", k8s_job="repack-synapses-abcde")
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}{name}", "j1")
        status.return_value = {"finished": True, "succeeded": False, "failed": True,
                               "reason": "the Job no longer exists"}
        out = tm.get_repack_status("j1")
        assert out["state"] == "failed" and "no longer exists" in out["error"]
        assert "DROP EXTENSION pg_repack CASCADE" in out["cleanup"]
        assert not real_redis.exists(f"{tm.LOCK_KEY_PREFIX}{name}")
        assert tm.list_repack_jobs()[0]["state"] == "failed"  # persisted

    def test_a_job_that_died_before_pg_repack_needs_no_cleanup(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        tm._set_status("j2", state="queued", database=scratch_db[0], k8s_job="repack-x")
        status.return_value = {"finished": True, "succeeded": False, "failed": True,
                               "reason": "Failed: BackoffLimitExceeded; container job Error (exit 1)"}
        out = tm.get_repack_status("j2")
        assert out["state"] == "failed" and "exit 1" in out["error"] and "cleanup" not in out

    def test_a_running_job_is_left_alone(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        tm._set_status("j3", state="running", database=scratch_db[0], k8s_job="repack-x")
        status.return_value = {"finished": False, "active": True, "reason": None}
        out = tm.get_repack_status("j3")
        assert out["state"] == "running" and out["k8s_job_status"] == "running"

    def test_a_result_recorded_as_the_job_finished_wins(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        tm._set_status("j4", state="running", database=scratch_db[0], k8s_job="repack-x")
        status.side_effect = lambda name: (tm._set_status("j4", state="done"),
                                           {"finished": True, "succeeded": True, "failed": False, "reason": None})[1]
        assert tm.get_repack_status("j4")["state"] == "done"

    def test_options_cannot_replace_validated_fields(self, scratch_db, real_redis, jobs_on):
        submit, status, _ = jobs_on
        status.return_value = {"finished": False, "active": False, "reason": None}
        name, _ = scratch_db
        with pytest.raises(tm.RepackRefused, match="unknown repack options"):
            tm.start_repack(name, "synapses", job_id="other-job")  # database/table_name are positional
        submit.assert_not_called()
        out = tm.start_repack(name, "synapses", order_by="id", dry_run=True)
        kwargs = submit.call_args.args[1]
        assert kwargs["database"] == name and kwargs["table_name"] == "synapses" and kwargs["job_id"] == out["job_id"]

    def test_a_record_without_a_database_leaves_locks_alone(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}None", "j6")
        tm._set_status("j6", state="running", k8s_job="repack-x")
        status.return_value = {"finished": True, "succeeded": False, "failed": True, "reason": "gone"}
        assert tm.get_repack_status("j6")["state"] == "failed"
        assert real_redis.get(f"{tm.LOCK_KEY_PREFIX}None") == b"j6"

    def test_reconcile_does_not_overwrite_a_result_written_meanwhile(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        tm._set_status("j7", state="running", database=scratch_db[0], k8s_job="repack-x")
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}{scratch_db[0]}", "someone-else")
        real_get = real_redis.get
        calls = {"n": 0}

        def get(key):  # the runner records "done" right after reconcile's re-read
            calls["n"] += 1
            raw = real_get(key)
            if calls["n"] == 2 and key.endswith("j7"):
                real_redis.set(key, raw.replace(b'"running"', b'"done"'))
            return raw

        status.return_value = {"finished": True, "succeeded": True, "failed": False, "reason": None}
        with mock.patch.object(real_redis, "get", side_effect=get):
            tm.get_repack_status("j7")
        assert tm.get_repack_status("j7")["state"] == "done"
        assert real_redis.get(f"{tm.LOCK_KEY_PREFIX}{scratch_db[0]}") == b"someone-else"

    def test_finished_records_are_not_looked_up(self, scratch_db, real_redis, jobs_on):
        _, status, _ = jobs_on
        tm._set_status("j5", state="done", k8s_job="repack-x")
        assert tm.get_repack_status("j5")["state"] == "done"
        status.assert_not_called()


class TestStatusRecord:
    def test_a_concurrent_write_is_merged_not_lost(self, real_redis):
        """The runner's progress write must not drop the k8s_job the api added meanwhile."""
        tm._set_status("j1", state="queued")
        key = f"{tm.STATUS_KEY_PREFIX}j1"
        other = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=13)
        real_pipeline = real_redis.pipeline
        injected = {"done": False}

        def pipeline(*a, **k):
            pipe = real_pipeline(*a, **k)
            real_get = pipe.get

            def get(name):
                raw = real_get(name)
                if not injected["done"]:  # another writer lands between our read and write
                    injected["done"] = True
                    other.set(key, raw.replace(b'"queued"', b'"queued", "k8s_job": "repack-x"'))
                return raw
            pipe.get = get
            return pipe

        with mock.patch.object(real_redis, "pipeline", side_effect=pipeline):
            tm._set_status("j1", state="checking")
        record = json.loads(real_redis.get(key))  # raw: get_repack_status would reconcile repack-x
        assert record["state"] == "checking" and record["k8s_job"] == "repack-x"

    def test_only_if_state_in(self, real_redis):
        tm._set_status("j2", state="done")
        assert tm._set_status("j2", only_if_state_in=("running",), state="failed")["state"] == "done"
        assert tm.get_repack_status("j2")["state"] == "done"

    def test_releasing_a_lock_spares_a_newer_owner(self, real_redis):
        real_redis.set(f"{tm.LOCK_KEY_PREFIX}db", "new-job")
        tm._release_lock("db", "old-job")
        assert real_redis.get(f"{tm.LOCK_KEY_PREFIX}db") == b"new-job"
        tm._release_lock("db", "new-job")
        assert not real_redis.exists(f"{tm.LOCK_KEY_PREFIX}db")


class TestRepackJobEntrypoint:
    @pytest.mark.parametrize("state,code", [("done", 0), ("dry run", 0), ("refused", 0), ("failed", 1)])
    def test_exit_status(self, monkeypatch, state, code):
        from materializationengine.jobs import repack

        monkeypatch.setenv("MAINTENANCE_JOB_ARGS", '{"job_id": "db.t.1", "database": "db", "table_name": "t"}')
        app = mock.MagicMock()
        with mock.patch("materializationengine.app.create_app", return_value=app), \
                mock.patch.object(tm, "run_repack", return_value={"state": state}) as run:
            assert repack.main() == code
        run.assert_called_once_with(job_id="db.t.1", database="db", table_name="t")
        app.app_context.assert_called_once()

    def test_no_job_id(self, monkeypatch):
        from materializationengine.jobs import repack

        monkeypatch.setenv("MAINTENANCE_JOB_ARGS", "{}")
        assert repack.main() == 2


class TestEndpoints:
    @pytest.fixture
    def client(self):
        from flask import Flask
        from flask_restx import Api

        from materializationengine.blueprints.materialize import api

        app = Flask(__name__)
        Api(app).add_namespace(api.mat_bp, path="/api/v2")
        with app.test_client() as client:
            yield client

    def test_repack_is_a_dry_run_unless_told_otherwise(self, client):
        with mock.patch.object(tm, "start_repack", return_value={"state": "queued"}) as start:
            client.post("/api/v2/maintenance/repack/minnie65_phase3/synapses_pni_2")
            client.post("/api/v2/maintenance/repack/minnie65_phase3/synapses_pni_2?dry_run=false&force=true&fillfactor=90")
        first, second = (c.kwargs for c in start.call_args_list)
        assert first["dry_run"] is True and first["force"] is False and first["order_by"] == "id"
        assert second["dry_run"] is False and second["force"] is True and second["fillfactor"] == 90

    def test_refusal_is_a_bad_request(self, client):
        with mock.patch.object(tm, "start_repack", side_effect=tm.RepackRefused("database 'postgres' is not an annotation database")):
            r = client.post("/api/v2/maintenance/repack/postgres/x")
        assert r.status_code == 400

    def test_status_404_for_unknown_job(self, client):
        with mock.patch.object(tm, "get_repack_status", return_value=None):
            assert client.get("/api/v2/maintenance/repack/status/nope").status_code == 404
