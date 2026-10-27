"""The long upload steps survive a redelivery, and job pods take one task each.

upload_to_db is acked late in an orchestration job pod. If the pod is lost, the task
is redelivered; the Cloud SQL import it started may still be running (it carries on
without its client) or be done, and so may a CREATE INDEX. The redelivery resumes from
them instead of creating, truncating and importing the table again.
"""

import json
from unittest import mock

import pytest
from celery.signals import task_postrun, task_prerun, worker_ready
from flask import Flask

from materializationengine import celery_worker
from materializationengine.blueprints.upload import tasks

JOB = "minnie65_phase3_v1_synapses_300m_test3_20261025_130540"
TABLE = "synapses_300m_test3"


class FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value, ex=None):
        self.data[key] = value if isinstance(value, bytes) else str(value).encode()

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return int(key in self.data)


FILE_METADATA = {
    "metadata": {
        "table_name": TABLE,
        "schema_type": "synapse",
        "description": "test",
        "user_id": "1",
        "voxel_resolution_nm_x": 4,
        "voxel_resolution_nm_y": 4,
        "voxel_resolution_nm_z": 40,
        "write_permission": "PRIVATE",
        "read_permission": "PUBLIC",
    }
}


@pytest.fixture
def env():
    redis = FakeRedis()
    db_client = mock.MagicMock()
    app = Flask(__name__)
    app.config.update(STAGING_DATABASE_NAME="staging")
    with app.app_context(), \
            mock.patch.object(tasks, "REDIS_CLIENT", redis), \
            mock.patch.object(tasks.dynamic_annotation_cache, "get_db", return_value=db_client), \
            mock.patch.object(tasks.index_cache, "drop_table_indices") as drop, \
            mock.patch.object(tasks.index_cache, "add_indices_sql_commands", return_value=["CREATE INDEX IF NOT EXISTS a", "CREATE INDEX IF NOT EXISTS b"]), \
            mock.patch.object(tasks, "add_index") as add_index, \
            mock.patch.object(tasks, "import_csv") as import_csv, \
            mock.patch.object(tasks, "wait_for_operation_name") as wait_for_operation, \
            mock.patch.object(tasks, "wait_for_index_builds") as wait_for_index_builds:
        yield {
            "redis": redis, "db_client": db_client, "drop": drop, "add_index": add_index,
            "import_csv": import_csv, "wait_for_operation": wait_for_operation,
            "wait_for_index_builds": wait_for_index_builds,
        }


def run_upload():
    return tasks.upload_to_database.run(
        {"job_id_for_status": JOB, "output_path": "bucket/processed.csv"},
        sql_instance_name="instance",
        file_metadata=FILE_METADATA,
        datastack_info={"datastack": "minnie65_phase3_v1"},
        job_id=JOB,
    )


class TestUploadToDatabaseResume:
    def test_a_fresh_run_records_its_import(self, env):
        def import_csv(*args, on_started=None, **kwargs):
            on_started("op-1")
            assert tasks.get_import_state(JOB) == {
                "operation": "op-1", "status": "running", "uri": "gs://bucket/processed.csv", "database": "staging", "table": TABLE,
            }

        env["import_csv"].side_effect = import_csv
        result = run_upload()
        assert result["table_name"] == TABLE
        env["db_client"].annotation.create_table.assert_called_once()
        env["drop"].assert_called_once()
        env["wait_for_operation"].assert_not_called()
        env["wait_for_index_builds"].assert_not_called()
        assert env["add_index"].call_count == 2
        assert tasks.get_import_state(JOB) == {"operation": "op-1", "status": "done", "uri": "gs://bucket/processed.csv", "database": "staging", "table": TABLE}

    def test_a_redelivery_waits_for_the_running_import(self, env):
        tasks.set_import_state(JOB, operation="op-1", status="running")
        run_upload()
        env["import_csv"].assert_not_called()  # not a second import
        env["db_client"].annotation.create_table.assert_not_called()
        env["drop"].assert_not_called()  # would drop indexes already built
        env["wait_for_operation"].assert_called_once()
        assert env["wait_for_operation"].call_args.args[:2] == ("instance", "op-1")
        env["wait_for_index_builds"].assert_called_once()  # a dead attempt's CREATE INDEX
        assert env["add_index"].call_count == 2  # IF NOT EXISTS: built ones are skipped
        assert tasks.get_import_state(JOB)["status"] == "done"

    def test_a_redelivery_after_the_import_finished_goes_to_the_indexes(self, env):
        tasks.set_import_state(JOB, operation="op-1", status="done")
        run_upload()
        env["import_csv"].assert_not_called()
        env["wait_for_operation"].assert_not_called()
        env["wait_for_index_builds"].assert_called_once()
        assert env["add_index"].call_count == 2

    def test_an_import_requested_but_not_recorded_is_adopted(self, env):
        # the attempt died between requesting the import and recording its operation
        tasks.set_import_state(JOB, status="requested", uri="gs://bucket/processed.csv")
        with mock.patch.object(tasks, "find_import_operation", return_value={"name": "op-7"}) as find:
            run_upload()
        assert find.call_args.args == ("instance", "gs://bucket/processed.csv")
        env["import_csv"].assert_not_called()
        env["db_client"].annotation.create_table.assert_not_called()
        assert env["wait_for_operation"].call_args.args[:2] == ("instance", "op-7")
        assert tasks.get_import_state(JOB)["status"] == "done"

    def test_an_import_requested_that_never_started_is_requested_again(self, env):
        tasks.set_import_state(JOB, status="requested", uri="gs://bucket/processed.csv")
        with mock.patch.object(tasks, "find_import_operation", return_value=None):
            run_upload()
        env["import_csv"].assert_called_once()
        env["wait_for_operation"].assert_not_called()

    def test_a_failed_import_fails_the_job(self, env):
        tasks.set_import_state(JOB, operation="op-1", status="running")
        env["wait_for_operation"].side_effect = tasks.CloudSQLAdminError("op-1 failed: disk full")
        with pytest.raises(RuntimeError, match="disk full"):
            run_upload()
        env["add_index"].assert_not_called()
        assert json.loads(env["redis"].get(f"csv_processing:{JOB}"))["status"] == "error"


class TestWaitForIndexBuilds:
    def test_waits_until_no_other_session_builds_an_index(self):
        counts = iter([2, 1, 0])
        conn = mock.MagicMock()
        conn.execute.return_value.scalar.side_effect = lambda: next(counts)
        engine = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        assert tasks.wait_for_index_builds(engine, TABLE, poll_seconds=0) == 2
        sql, params = conn.execute.call_args.args
        assert "query ILIKE :s0" in str(sql)
        assert params == {"table": f"%{TABLE}%", "s0": "%CREATE INDEX%"}

    def test_waits_for_other_statements_too(self):
        conn = mock.MagicMock()
        conn.execute.return_value.scalar.return_value = 0
        engine = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        assert tasks.wait_for_backend_work(engine, TABLE, ("CREATE INDEX", "CLUSTER"), poll_seconds=0) == 0
        sql, params = conn.execute.call_args.args
        assert "query ILIKE :s0 OR query ILIKE :s1" in str(sql)
        assert params["s1"] == "%CLUSTER%"


class TestAutoshutdownStopsConsuming:
    """A job pod that has started its last task takes no further messages."""

    @pytest.fixture
    def autoshutdown(self):
        app = Flask(__name__)
        app.config.update(
            CELERY_WORKER_AUTOSHUTDOWN_ENABLED=True,
            CELERY_WORKER_AUTOSHUTDOWN_MAX_TASKS=2,
            CELERY_WORKER_AUTOSHUTDOWN_DELAY_SECONDS=3600,
        )
        consumer = mock.MagicMock()
        consumer.task_consumer.queues = [mock.Mock(), mock.Mock()]
        consumer.task_consumer.queues[0].name = "orchestration"
        consumer.task_consumer.queues[1].name = "other"
        with mock.patch.object(celery_worker.os, "kill") as kill:
            celery_worker.configure_worker_autoshutdown(app)
            try:
                worker_ready.send(sender=consumer)
                yield consumer, kill
            finally:
                for signal, uid in zip(
                    (worker_ready, task_prerun, task_postrun),
                    celery_worker.AUTOSHUTDOWN_DISPATCH_UIDS,
                ):
                    signal.disconnect(dispatch_uid=uid)

    def test_stops_consuming_when_the_last_task_starts(self, autoshutdown):
        consumer, kill = autoshutdown
        task_prerun.send(sender=None)  # task 1 of 2: keep consuming
        consumer.cancel_task_queue.assert_not_called()
        task_prerun.send(sender=None)  # task 2 of 2: the last one
        assert [c.args[0] for c in consumer.cancel_task_queue.call_args_list] == ["orchestration", "other"]
        kill.assert_not_called()


class TestIndexMaintenanceWorkMem:
    """UPLOAD_INDEX_MAINTENANCE_WORK_MEM gives an upload's index builds their own memory."""

    def test_upload_builds_use_the_upload_setting(self, env):
        from flask import current_app

        current_app.config["UPLOAD_INDEX_MAINTENANCE_WORK_MEM"] = "6GB"
        tasks.set_import_state(JOB, operation="op-1", status="done")
        run_upload()
        assert [c.args[2] for c in env["add_index"].call_args_list] == ["6GB", "6GB"]

    def test_unset_keeps_add_indexs_default(self, env):
        tasks.set_import_state(JOB, operation="op-1", status="done")
        run_upload()
        assert [c.args[2] for c in env["add_index"].call_args_list] == [None, None]

    @pytest.mark.parametrize("value", ["6GB", "512MB", "65536kB"])
    def test_valid_values(self, value):
        from materializationengine.shared_tasks import maintenance_work_mem_setting

        assert maintenance_work_mem_setting(value) == value

    @pytest.mark.parametrize("value", ["6", "6 GiB", "6GB'; DROP TABLE x; --"])
    def test_values_are_checked_before_they_reach_sql(self, value):
        from materializationengine.shared_tasks import maintenance_work_mem_setting

        with pytest.raises(ValueError):
            maintenance_work_mem_setting(value)
