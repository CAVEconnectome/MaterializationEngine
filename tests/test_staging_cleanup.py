"""Uploads remove their staging tables, and admins can purge what an upload left behind.

The purge tests need a real redis on localhost:6390 (`docker run -d --rm -p 6390:6379
redis:7`) and are skipped without one.
"""

import base64
import json
import os
import uuid
from unittest import mock

import pytest
import redis
import sqlalchemy as sa
from flask import Flask
from sqlalchemy.engine.url import make_url

from materializationengine.blueprints.upload import checkpoint_manager, tasks

ANNO = "synapses_cleanup_test"
SEG = f"{ANNO}__minnie3_v1"
JOB = f"minnie65_phase3_v1_{ANNO}_20261006_190000"


def _make_db(base, name):
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    with engine.begin() as conn:
        conn.execute(
            "CREATE TABLE annotation_table_metadata (table_name varchar PRIMARY KEY,"
            " created timestamp DEFAULT now())"
        )
        conn.execute(
            "CREATE TABLE segmentation_table_metadata (table_name varchar PRIMARY KEY,"
            " annotation_table varchar REFERENCES annotation_table_metadata(table_name))"
        )
        conn.execute(
            "CREATE TABLE combined_table_metadata (table_name varchar PRIMARY KEY,"
            " annotation_table varchar REFERENCES annotation_table_metadata(table_name),"
            " reference_table varchar REFERENCES annotation_table_metadata(table_name))"
        )
    return admin, engine


def add_upload_tables(engine, table, age_hours=0):
    with engine.begin() as conn:
        conn.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY)')
        conn.execute(f'CREATE TABLE "{table}__minnie3_v1" (id bigint)')
        conn.execute(
            sa.text("INSERT INTO annotation_table_metadata VALUES (:t, now() - make_interval(secs => :age))"),
            {"t": table, "age": age_hours * 3600},
        )
        conn.execute(
            sa.text("INSERT INTO segmentation_table_metadata VALUES (:s, :t)"),
            {"s": f"{table}__minnie3_v1", "t": table},
        )


@pytest.fixture
def databases(mat_metadata):
    """Scratch staging and production databases with the metadata tables (and FKs)."""
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    suffix = uuid.uuid4().hex[:8]
    created = [_make_db(base, f"cleanup_{kind}_{suffix}") for kind in ("staging", "production")]
    staging, production = created[0][1], created[1][1]
    for engine in (staging, production):
        add_upload_tables(engine, ANNO)
        add_upload_tables(engine, "other_upload")
    engines = {"staging": staging, "minnie65_phase3": production}
    with mock.patch.object(tasks, "get_config_param", return_value="staging"), \
            mock.patch.object(tasks.db_manager, "get_engine", side_effect=engines.__getitem__):
        yield staging, production
    for (admin, engine), kind in zip(created, ("staging", "production")):
        engine.dispose()
        with admin.connect() as conn:
            conn.execute(f"DROP DATABASE IF EXISTS cleanup_{kind}_{suffix}")
        admin.dispose()


def tables_in(engine):
    with engine.connect() as conn:
        tables = {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")}
        anno = {r[0] for r in conn.execute("SELECT table_name FROM annotation_table_metadata")}
        seg = {r[0] for r in conn.execute("SELECT table_name FROM segmentation_table_metadata")}
    return tables - {"annotation_table_metadata", "segmentation_table_metadata", "combined_table_metadata"}, anno, seg


def has_upload(engine, table=ANNO):
    tables, anno, seg = tables_in(engine)
    present = [table in tables, f"{table}__minnie3_v1" in tables, table in anno, f"{table}__minnie3_v1" in seg]
    assert all(present) or not any(present), present
    return all(present)


def _transfer_result(status="success"):
    return {
        "status": status,
        "job_id_for_status": JOB,
        "tables_transferred": {
            "annotation_table": {"name": ANNO, "success": True},
            "segmentation_table": {"name": SEG, "success": True},
        },
    }


class TestAutomaticCleanup:
    def test_finished_upload_drops_its_staging_tables_only(self, databases):
        staging, production = databases
        with mock.patch.object(tasks, "update_job_status") as status:
            result = tasks.cleanup_staging_tables.run(_transfer_result())
        assert result == {"status": "success", "dropped": sorted([ANNO, SEG])}
        assert not has_upload(staging)
        assert has_upload(staging, "other_upload")
        assert has_upload(production)  # production is never touched here
        status.assert_called_once_with(JOB, {"staging_cleanup": "done"})

    def test_rerun_is_harmless(self, databases):
        with mock.patch.object(tasks, "update_job_status"):
            tasks.cleanup_staging_tables.run(_transfer_result())
            assert tasks.cleanup_staging_tables.run(_transfer_result()) == {"status": "success", "dropped": []}

    def test_unsuccessful_transfer_leaves_staging(self, databases):
        staging, _ = databases
        with mock.patch.object(tasks, "update_job_status"):
            assert tasks.cleanup_staging_tables.run(_transfer_result("error"))["status"] == "skipped"
        assert has_upload(staging)

    def test_failed_or_cancelled_upload_drops_its_staging_tables(self, databases):
        staging, production = databases
        with mock.patch.object(tasks, "update_job_status"):
            result = tasks.discard_upload_staging.run(table_name=ANNO, job_id=JOB, reason="failed")
        assert result["status"] == "success"
        assert not has_upload(staging) and has_upload(staging, "other_upload") and has_upload(production)


class TestCleanupIsWiredIn:
    def test_chain_ends_with_cleanup_and_discards_on_failure(self):
        app = Flask(__name__)
        with app.app_context(), mock.patch.object(tasks, "chain") as chain, \
                mock.patch.object(tasks, "is_upload_cancelled", return_value=False), \
                mock.patch.object(tasks, "update_job_status") as status:
            tasks.process_and_upload.run(
                "gs://bucket/file.csv",
                {"metadata": {"table_name": ANNO, "schema_type": "synapse"}, "column_mapping": {}},
                {"datastack": "minnie65_phase3_v1"},
                job_id=JOB,
            )
        steps = [sig.task for sig in chain.call_args.args]
        assert steps[-2:] == ["workflow:transfer_to_production", "workflow:cleanup_staging_tables"]
        errback = chain.return_value.apply_async.call_args.kwargs["link_error"]
        assert errback.task == "workflow:discard_upload_staging"
        assert errback.kwargs == {"table_name": ANNO, "job_id": JOB, "reason": "failed"}
        assert status.call_args.args[1]["staging_table_name"] == ANNO

    def test_cancel_schedules_the_discard(self):
        with mock.patch.object(tasks, "get_job_status", return_value={"status": "processing", "staging_table_name": ANNO}), \
                mock.patch.object(tasks, "REDIS_CLIENT"), mock.patch.object(tasks, "update_job_status"), \
                mock.patch.object(tasks, "_stop_spatial_workflow"), \
                mock.patch.object(tasks.discard_upload_staging, "si") as si:
            tasks.request_upload_cancel(JOB)
        si.assert_called_once_with(table_name=ANNO, job_id=JOB, reason="cancelled")
        si.return_value.apply_async.assert_called_once_with(countdown=tasks.STAGING_DISCARD_DELAY_SECONDS)


# --- admin purge --------------------------------------------------------------

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))


@pytest.fixture
def redis_clients():
    status_redis = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=14)
    checkpoint_redis = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=15)
    try:
        status_redis.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    for client in (status_redis, checkpoint_redis):
        client.flushdb()
    with mock.patch.object(tasks, "REDIS_CLIENT", status_redis), \
            mock.patch.object(checkpoint_manager, "REDIS_CLIENT", checkpoint_redis):
        yield status_redis, checkpoint_redis
    for client in (status_redis, checkpoint_redis):
        client.flushdb()


def celery_message(task, **kwargs):
    body = base64.b64encode(json.dumps([[], kwargs, {}]).encode()).decode()
    return {"body": body, "headers": {"task": task}, "content-type": "application/json"}


def plant_upload_state(status_redis, checkpoint_redis, job_status="error", table=ANNO, job=JOB):
    status_redis.set(f"csv_processing:{job}", json.dumps(
        {"status": job_status, "datastack_name": "minnie65_phase3_v1", "staging_table_name": table}))
    status_redis.set(f"{tasks.CANCEL_KEY_PREFIX}{job}", "1")
    for key in (f"workflow:staging:{table}", f"workflow:staging:{table}:chunk_statuses"):
        checkpoint_redis.set(key, "x")
    message = celery_message("workflow:transfer_to_production", monitor_result={"table_name": table})
    status_redis.hset("unacked", "tag1", json.dumps([message, "", "workflow"]))
    status_redis.zadd("unacked_index", {"tag1": 1})
    status_redis.rpush("spatial", json.dumps(celery_message("spatial:process_chunk", mat_metadata={"annotation_table_name": table})))


@pytest.fixture
def datastack_info():
    with mock.patch("materializationengine.info_client.get_datastack_info",
                    return_value={"aligned_volume": {"name": "minnie65_phase3"}}):
        yield


class TestPurgeUpload:
    def test_removes_everything_the_upload_left(self, databases, redis_clients):
        staging, production = databases
        status_redis, checkpoint_redis = redis_clients
        plant_upload_state(status_redis, checkpoint_redis)
        # another upload's state is left alone
        checkpoint_redis.set("workflow:staging:other_upload", "x")
        status_redis.hset("unacked", "tag2", json.dumps([celery_message("workflow:x", t="other_upload"), "", "workflow"]))

        report = tasks.purge_upload(JOB)

        assert report["result"] == "removed"
        assert sorted(report["celery_messages_removed"]) == ["spatial:process_chunk", "workflow:transfer_to_production"]
        assert report["staging_tables_dropped"] == sorted([ANNO, SEG])
        assert not has_upload(staging) and has_upload(production)
        assert not status_redis.exists(f"csv_processing:{JOB}", f"{tasks.CANCEL_KEY_PREFIX}{JOB}")
        assert checkpoint_redis.keys("*") == [b"workflow:staging:other_upload"]
        assert status_redis.hkeys("unacked") == [b"tag2"] and status_redis.zcard("unacked_index") == 0
        assert status_redis.llen("spatial") == 0
        assert has_upload(staging, "other_upload")

    def test_dry_run_changes_nothing(self, databases, redis_clients):
        staging, _ = databases
        status_redis, checkpoint_redis = redis_clients
        plant_upload_state(status_redis, checkpoint_redis)
        report = tasks.purge_upload(JOB, dry_run=True)
        assert report["result"] == "would remove" and report["staging_tables_dropped"] == sorted([ANNO, SEG])
        assert has_upload(staging) and status_redis.hlen("unacked") == 1 and status_redis.llen("spatial") == 1
        assert status_redis.exists(f"csv_processing:{JOB}") and checkpoint_redis.dbsize() == 2

    def test_active_upload_needs_force(self, databases, redis_clients):
        staging, _ = databases
        plant_upload_state(*redis_clients, job_status="processing")
        assert tasks.purge_upload(JOB)["result"] == "refused"
        assert has_upload(staging)
        assert tasks.purge_upload(JOB, force=True)["result"] == "removed"
        assert not has_upload(staging)

    def test_production_only_when_asked(self, databases, redis_clients, datastack_info):
        staging, production = databases
        plant_upload_state(*redis_clients)
        report = tasks.purge_upload(JOB, include_production=True)
        assert report["production_tables_dropped"] == sorted([ANNO, SEG])
        assert not has_upload(staging) and not has_upload(production)
        assert has_upload(production, "other_upload")

    def test_finished_uploads_production_tables_need_force(self, databases, redis_clients, datastack_info):
        _, production = databases
        plant_upload_state(*redis_clients, job_status="done")
        assert tasks.purge_upload(JOB, include_production=True)["result"] == "refused"
        assert has_upload(production)

    def test_expired_job_record_needs_the_table_name(self, databases, redis_clients):
        staging, _ = databases
        assert tasks.purge_upload(JOB)["result"] == "refused"
        assert tasks.purge_upload(JOB, table_name=ANNO)["staging_tables_dropped"] == sorted([ANNO, SEG])
        assert not has_upload(staging)


class TestPurgeFailedUploads:
    def test_failed_jobs_and_old_orphans_go_active_and_recent_stay(self, databases, redis_clients):
        staging, production = databases
        status_redis, checkpoint_redis = redis_clients
        plant_upload_state(status_redis, checkpoint_redis, job_status="error")  # ANNO: failed job
        add_upload_tables(staging, "old_orphan", age_hours=48)
        add_upload_tables(staging, "recent_orphan", age_hours=1)
        add_upload_tables(staging, "old_but_running", age_hours=48)
        status_redis.set("csv_processing:j_active", json.dumps(
            {"status": "processing", "staging_table_name": "old_but_running"}))
        add_upload_tables(staging, "old_with_task_in_flight", age_hours=48)
        status_redis.rpush("workflow", json.dumps(celery_message("workflow:upload_to_db", t="old_with_task_in_flight")))
        with staging.begin() as conn:  # other_upload: make it an old orphan too
            conn.execute("UPDATE annotation_table_metadata SET created = now() - interval '48 hours' WHERE table_name = 'other_upload'")

        report = tasks.purge_failed_uploads(dry_run=False)

        assert [j["job_id"] for j in report["purged_jobs"]] == [JOB]
        assert sorted(o["table_name"] for o in report["orphaned_staging_tables"]) == ["old_orphan", "other_upload"]
        assert not any(has_upload(staging, t) for t in (ANNO, "old_orphan", "other_upload"))
        assert all(has_upload(staging, t) for t in ("recent_orphan", "old_but_running", "old_with_task_in_flight"))
        assert has_upload(production) and has_upload(production, "other_upload")

    def test_dry_run_changes_nothing(self, databases, redis_clients):
        staging, _ = databases
        plant_upload_state(*redis_clients)
        add_upload_tables(staging, "old_orphan", age_hours=48)
        report = tasks.purge_failed_uploads(dry_run=True)
        assert report["purged_jobs"][0]["result"] == "would remove"
        assert [o["table_name"] for o in report["orphaned_staging_tables"]] == ["old_orphan"]
        assert has_upload(staging) and has_upload(staging, "old_orphan")


class TestAdminEndpoints:
    @pytest.fixture
    def client(self):
        from materializationengine.blueprints.upload import api

        app = Flask(__name__)
        app.register_blueprint(api.upload_bp)
        prefix = api.upload_bp.url_prefix or ""
        with app.test_client() as client:
            yield client, prefix, api

    def test_single_job_cleanup_passes_flags(self, client):
        client, prefix, api = client
        with mock.patch.object(api, "purge_upload", return_value={"result": "removed"}) as purge:
            r = client.post(f"{prefix}/api/admin/jobs/{JOB}/cleanup?dry_run=true&include_production=1",
                            json={"table_name": ANNO})
        assert r.status_code == 200 and r.json["report"] == {"result": "removed"}
        purge.assert_called_once_with(JOB, include_production=True, force=False, dry_run=True, table_name=ANNO)

    def test_refusal_is_a_conflict(self, client):
        client, prefix, api = client
        with mock.patch.object(api, "purge_upload", return_value={"result": "refused", "reason": "active"}):
            r = client.post(f"{prefix}/api/admin/jobs/{JOB}/cleanup")
        assert r.status_code == 409 and r.json["status"] == "refused"

    def test_bulk_cleanup_is_a_dry_run_unless_told_otherwise(self, client):
        client, prefix, api = client
        with mock.patch.object(api, "purge_failed_uploads", return_value={}) as purge:
            client.post(f"{prefix}/api/admin/jobs/cleanup")
            client.post(f"{prefix}/api/admin/jobs/cleanup", json={"dry_run": False, "statuses": "error",
                                                                     "orphan_min_age_hours": 6})
        assert purge.call_args_list[0].kwargs == {"statuses": ["error", "failed", "cancelled"], "include_orphans": True,
                                                  "orphan_min_age_hours": 24.0, "dry_run": True}
        assert purge.call_args_list[1].kwargs == {"statuses": ["error"], "include_orphans": True,
                                                  "orphan_min_age_hours": 6.0, "dry_run": False}
