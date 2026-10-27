"""The upload state database: created when missing, and the store's semantics.

Runs against the test PostGIS server (tests/test_data/mat_metadata.json sql_uri), in a
scratch database of its own that is dropped afterwards.
"""

import threading
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from materializationengine.upload_state import db, models, store

JOB = "minnie65_phase3_v1_synapses_test_20261026_080000"


@pytest.fixture
def admin(mat_metadata):
    url = mat_metadata["sql_uri"]
    engine = create_engine(url, poolclass=NullPool)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("no test postgres")
    yield url


@pytest.fixture
def engine(admin):
    name = f"upload_state_test_{uuid.uuid4().hex[:8]}"
    engine = db.get_engine(name, admin=admin)
    yield engine
    db.dispose_engines()
    drop = create_engine(admin, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    with drop.connect() as conn:
        conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
    drop.dispose()


def new_job(engine, job_id=JOB, table="synapses_test", **record):
    store.create_job(
        engine, job_id, user_id="1174", datastack_name="minnie65_phase3_v1",
        table_name=table, staging_database="staging",
        record={"phase": "Queued", "progress": 0, "user_id": "1174",
                "datastack_name": "minnie65_phase3_v1", **record},
        params={"file_path": "gs://b/f.csv"},
    )


class TestDatabaseCreation:
    def test_created_when_missing_and_reused(self, admin):
        name = f"upload_state_test_{uuid.uuid4().hex[:8]}"
        try:
            assert db.ensure_database(admin, name) is True
            assert db.ensure_database(admin, name) is False
            engine = create_engine(db.database_url(admin, name), poolclass=NullPool)
            assert db.ensure_schema(engine) == models.SCHEMA_VERSION
            assert db.ensure_schema(engine) == models.SCHEMA_VERSION  # idempotent
            with engine.connect() as conn:
                tables = {r[0] for r in conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname='public'"))}
                assert {"upload_jobs", "upload_stages", "upload_parts", "upload_cloudsql_ops",
                        "upload_spatial", "upload_spatial_chunks", "schema_version"} <= tables
                assert conn.execute(text("SELECT max(version) FROM schema_version")).scalar() == 1
            engine.dispose()
        finally:
            drop = create_engine(admin, poolclass=NullPool, isolation_level="AUTOCOMMIT")
            with drop.connect() as conn:
                conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
            drop.dispose()

    def test_concurrent_creators_do_not_fail(self, admin):
        name = f"upload_state_test_{uuid.uuid4().hex[:8]}"
        errors = []

        def create():
            try:
                db.ensure_database(admin, name)
                engine = create_engine(db.database_url(admin, name), poolclass=NullPool)
                db.ensure_schema(engine)
                engine.dispose()
            except Exception as e:  # pragma: no cover - the assertion reports it
                errors.append(e)

        threads = [threading.Thread(target=create) for _ in range(4)]
        try:
            [t.start() for t in threads]
            [t.join() for t in threads]
            assert errors == []
        finally:
            drop = create_engine(admin, poolclass=NullPool, isolation_level="AUTOCOMMIT")
            with drop.connect() as conn:
                conn.execute(text(f'DROP DATABASE IF EXISTS "{name}"'))
            drop.dispose()

    def test_newer_schema_is_refused(self, engine):
        with engine.begin() as conn:
            conn.execute(text("INSERT INTO schema_version (version) VALUES (99)"))
        with pytest.raises(db.UploadStateSchemaError):
            db.ensure_schema(engine)


class TestJobs:
    def test_record_merge_and_list(self, engine):
        new_job(engine)
        assert store.update_job(engine, JOB, {"status": "processing", "phase": "Processing CSV Data", "progress": 12})
        record = store.get_job_record(engine, JOB)
        assert record["status"] == "processing" and record["phase"] == "Processing CSV Data"
        assert record["user_id"] == "1174" and "last_updated" in record  # kept, and stamped
        assert store.get_job(engine, JOB)["status"] == "processing"
        assert [j["job_id"] for j in store.list_jobs(engine)] == [JOB]

    def test_one_running_upload_per_staging_table(self, engine):
        new_job(engine)
        with pytest.raises(store.ActiveUploadExists):
            new_job(engine, job_id=JOB + "_b")
        store.update_job(engine, JOB, {"status": "done"}, finished=True)
        new_job(engine, job_id=JOB + "_b")  # fine once the first has finished

    def test_cancel_blocks_later_updates(self, engine):
        new_job(engine)
        store.request_cancel(engine, JOB, by="1174")
        assert store.is_cancel_requested(engine, JOB)
        assert not store.update_job(engine, JOB, {"status": "processing", "phase": "late"})
        assert store.update_job(engine, JOB, {"status": "cancelled", "phase": "Cancelled by user"})
        assert store.get_job_record(engine, JOB)["status"] == "cancelled"

    def test_mark_terminal_only_when_active(self, engine):
        new_job(engine)
        assert store.mark_terminal_if_active(engine, JOB, "error", "Ray job gone")
        assert store.get_job_record(engine, JOB)["error"] == "Ray job gone"
        assert not store.mark_terminal_if_active(engine, JOB, "error", "again")


class TestLease:
    def test_epochs_fence_writes(self, engine):
        new_job(engine)
        first = store.acquire_lease(engine, JOB, "head-a")
        assert first == 1
        assert store.acquire_lease(engine, JOB, "head-b") is None  # first is alive
        assert store.heartbeat(engine, JOB, first) == (True, False)
        # the first driver dies: its heartbeat goes stale
        with engine.begin() as conn:
            conn.execute(text("UPDATE upload_jobs SET driver_heartbeat_at = now() - interval '5 minutes'"))
        second = store.acquire_lease(engine, JOB, "head-b")
        assert second == 2
        assert store.heartbeat(engine, JOB, first) == (False, False)
        assert not store.update_job(engine, JOB, {"phase": "stale"}, epoch=first)
        assert store.update_job(engine, JOB, {"phase": "current"}, epoch=second)
        store.start_stage(engine, JOB, "import", second)
        assert not store.finish_stage(engine, JOB, "import", first)
        assert store.finish_stage(engine, JOB, "import", second, {"rows": 3})
        stage = store.get_stages(engine, JOB)["import"]
        assert stage["status"] == "done" and stage["result"] == {"rows": 3}

    def test_heartbeat_reports_cancel(self, engine):
        new_job(engine)
        epoch = store.acquire_lease(engine, JOB, "head")
        store.request_cancel(engine, JOB)
        assert store.heartbeat(engine, JOB, epoch) == (True, True)


class TestPartsAndImport:
    def test_part_plan_is_stored_once(self, engine):
        new_job(engine)
        plan = [{"part_index": i, "kind": "csv", "spec": {"start": i, "end": i + 1}, "output_blob": f"p/{i}"} for i in range(3)]
        store.plan_parts(engine, JOB, plan)
        store.plan_parts(engine, JOB, [{**plan[0], "spec": {"start": 99}}])  # ignored
        store.update_parts(engine, JOB, [{"part_index": 1, "status": "done", "rows_read": 10}])
        parts = store.get_parts(engine, JOB)
        assert [p["spec"]["start"] for p in parts] == [0, 1, 2]
        assert [p["status"] for p in parts] == ["pending", "done", "pending"]

    def test_import_intent_then_operation(self, engine):
        new_job(engine)
        row = store.record_import_intent(engine, JOB, "gs://b/p.csv", "staging", "synapses_test")
        assert row["status"] == "requested" and row["operation_name"] is None
        store.record_import_intent(engine, JOB, "gs://other", "x", "y")  # first intent kept
        store.set_import_operation(engine, JOB, "op-1")
        store.finish_import(engine, JOB, "done")
        row = store.get_import(engine, JOB)
        assert (row["uri"], row["operation_name"], row["status"]) == ("gs://b/p.csv", "op-1", "done")
        store.reset_import(engine, JOB)
        assert store.get_import(engine, JOB) is None


class TestSpatial:
    def init(self, engine):
        new_job(engine)
        return store.init_spatial(
            engine, JOB, database_name="staging", workflow_name="synapses_test",
            datastack_name="minnie65_phase3_v1", status="processing_chunks", total_chunks=5,
            total_point_estimate=100, mat_metadata={"materialization_time_stamp": "2026-10-26 08:00:00.000001"},
        )

    def test_init_once_and_lookup_by_workflow(self, engine):
        row = self.init(engine)
        again = store.init_spatial(engine, JOB, database_name="staging", workflow_name="synapses_test",
                                   status="initializing", mat_metadata={"materialization_time_stamp": "later"})
        assert again["status"] == "processing_chunks"
        assert again["mat_metadata"] == row["mat_metadata"]  # frozen
        assert store.get_spatial_by_workflow(engine, "staging", "synapses_test")["job_id"] == JOB
        assert [s["job_id"] for s in store.list_active_spatial(engine, "staging")] == [JOB]

    def test_chunk_results_and_totals(self, engine):
        self.init(engine)
        store.record_chunk_results(engine, JOB, [
            {"chunk_index": 0, "status": "COMPLETED", "points": 40, "rows_processed": 20},
            {"chunk_index": 1, "status": "FAILED_RETRYABLE", "attempt_count": 1, "error_message": "boom"},
            {"chunk_index": 2, "status": "FAILED_PERMANENT", "attempt_count": 10},
        ])
        store.record_chunk_results(engine, JOB, [
            {"chunk_index": 1, "status": "COMPLETED", "attempt_count": 2, "points": 30},
            {"chunk_index": 0, "status": "PROCESSING"},  # a completed chunk stays completed
        ])
        completed, permanent, attempts = store.chunk_state(engine, JOB)
        assert completed == {0, 1} and permanent == {2} and attempts[1] == 2
        assert store.chunk_totals(engine, JOB) == {"completed_chunks": 2, "failed_chunks": 1, "rows_processed": 70}

    def test_as_workflow_data(self, engine):
        self.init(engine)
        store.update_spatial(engine, JOB, completed_chunks=2, rows_processed=40,
                             processing_rate="100.00 rows/minute")
        data = store.as_workflow_data(store.get_spatial(engine, JOB))
        assert (data.table_name, data.total_chunks, data.completed_chunks) == ("synapses_test", 5, 2)
        assert data.progress == 40.0 and data.processing_rate == "100.00 rows/minute"
        assert isinstance(data.start_time, str)


class TestRetention:
    def test_purge_cascades_and_spares_recent_jobs(self, engine):
        new_job(engine, table="old")
        new_job(engine, job_id=JOB + "_new", table="new")
        store.init_spatial(engine, JOB, database_name="staging", workflow_name="old",
                           status="completed", mat_metadata={})
        store.record_chunk_results(engine, JOB, [{"chunk_index": 0, "status": "COMPLETED"}])
        store.plan_parts(engine, JOB, [{"part_index": 0, "kind": "csv", "spec": {}, "output_blob": "p"}])
        with engine.begin() as conn:
            conn.execute(text("UPDATE upload_jobs SET updated_at = now() - interval '31 days' WHERE job_id = :j"), {"j": JOB})
        assert store.purge_older_than(engine, 30, dry_run=True) == [JOB]
        assert store.get_job(engine, JOB) is not None
        assert store.purge_older_than(engine, 30) == [JOB]
        assert store.get_job(engine, JOB) is None and store.get_job(engine, JOB + "_new") is not None
        with engine.connect() as conn:
            for table in ("upload_spatial", "upload_spatial_chunks", "upload_parts"):
                assert conn.execute(text(f"SELECT count(*) FROM {table}")).scalar() == 0

    def test_dead_driver_detection(self, engine):
        new_job(engine)
        assert store.jobs_with_dead_driver(engine, stale_minutes=10) == []
        with engine.begin() as conn:
            conn.execute(text("UPDATE upload_jobs SET created_at = now() - interval '1 hour'"))
        assert [j["job_id"] for j in store.jobs_with_dead_driver(engine, stale_minutes=10)] == [JOB]


class TestStatusSink:
    """tasks.py's status helpers report to the store inside a StoreSink, redis outside."""

    @pytest.fixture
    def tasks(self):
        from unittest import mock

        from materializationengine.blueprints.upload import tasks

        redis = mock.MagicMock()
        redis.get.return_value = None
        redis.exists.return_value = 0
        with mock.patch.object(tasks, "REDIS_CLIENT", redis):
            yield tasks, redis

    def test_job_status_goes_to_the_store(self, engine, tasks):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        tasks, redis = tasks
        new_job(engine)
        with use_sink(StoreSink(engine, JOB, stage="import")):
            tasks.update_job_status(JOB, {"status": "processing", "phase": "Importing", "progress": 30})
            assert tasks.get_job_status(JOB)["phase"] == "Importing"
            assert tasks.get_job_status(JOB)["user_id"] == "1174"  # merged, not replaced
            assert not tasks.is_upload_cancelled(JOB)
            # another job id in the same context still uses redis
            tasks.update_job_status("other-job", {"phase": "x"})
        redis.set.assert_called_once()
        assert redis.set.call_args.args[0] == "csv_processing:other-job"
        job = store.get_job(engine, JOB)
        assert (job["status"], job["current_stage"]) == ("processing", "import")
        # outside the sink: redis
        assert tasks.get_job_status(JOB) is None

    def test_a_cancel_stops_later_updates(self, engine, tasks):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        tasks, _ = tasks
        new_job(engine)
        store.request_cancel(engine, JOB, by="1174")
        with use_sink(StoreSink(engine, JOB)):
            assert tasks.is_upload_cancelled(JOB)
            tasks.update_job_status(JOB, {"status": "processing", "phase": "still going"})
            assert tasks.get_job_status(JOB)["phase"] != "still going"

    def test_a_stale_driver_cannot_write(self, engine, tasks):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        tasks, _ = tasks
        new_job(engine)
        old = store.acquire_lease(engine, JOB, "head-1")
        store.set_job_fields(engine, JOB, driver_heartbeat_at=None)  # head-1 died
        assert store.acquire_lease(engine, JOB, "head-2") == old + 1
        with use_sink(StoreSink(engine, JOB, epoch=old)):
            tasks.update_job_status(JOB, {"phase": "from the dead driver"})
            assert tasks.get_job_status(JOB)["phase"] == "Queued"

    def test_import_state_round_trip(self, engine, tasks):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        tasks, _ = tasks
        new_job(engine)
        with use_sink(StoreSink(engine, JOB)):
            assert tasks.get_import_state(JOB) == {}
            intent = dict(status="requested", uri="gs://b/p.csv", database="staging", table="t")
            tasks.set_import_state(JOB, **intent)
            assert tasks.get_import_state(JOB) == {"status": "requested", "uri": "gs://b/p.csv"}
            tasks.set_import_state(JOB, operation="op-1", status="running")
            tasks.set_import_state(JOB, **intent)  # a second request keeps the operation
            assert tasks.get_import_state(JOB) == {
                "status": "running", "uri": "gs://b/p.csv", "operation": "op-1",
            }
            tasks.set_import_state(JOB, status="done")
            assert tasks.get_import_state(JOB)["status"] == "done"
