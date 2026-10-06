"""Cancelling a bulk upload stops every later step and a running spatial lookup."""

import json
from unittest import mock

import pytest
from flask import Flask, g

from materializationengine.blueprints.upload import api, checkpoint_manager, tasks
from materializationengine.blueprints.upload.checkpoint_manager import RedisCheckpointManager
from materializationengine.workflows import spatial_lookup

JOB = "minnie65_phase3_v1_synapse_1m_test_20261003_090000"
WORKFLOW = "synapse_1m_test"


class FakeRedis:
    """The subset of redis the job status, cancel key and checkpoint code use."""

    def __init__(self):
        self.data = {}

    def set(self, key, value, ex=None):
        self.data[key] = value if isinstance(value, bytes) else str(value).encode()

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return int(key in self.data)

    def delete(self, *keys):
        return sum(self.data.pop(k, None) is not None for k in keys)


@pytest.fixture
def redis_clients():
    status_redis, checkpoint_redis = FakeRedis(), FakeRedis()
    with mock.patch.object(tasks, "REDIS_CLIENT", status_redis), \
            mock.patch.object(checkpoint_manager, "REDIS_CLIENT", checkpoint_redis):
        yield status_redis, checkpoint_redis


def _start_spatial_workflow():
    manager = RedisCheckpointManager("staging")
    manager.initialize_workflow(WORKFLOW, "task-id", "minnie65_phase3_v1")
    manager.update_workflow(table_name=WORKFLOW, status="processing_chunks")
    return manager


class TestUploadCancel:
    """request_upload_cancel and the checks each upload step makes."""

    def test_cancel_marks_job_and_blocks_later_status_updates(self, redis_clients):
        tasks.update_job_status(JOB, {"status": "processing", "phase": "Processing CSV"})

        tasks.request_upload_cancel(JOB)
        # a step that was already running reports progress after the cancel
        tasks.update_job_status(JOB, {"status": "processing", "phase": "Rows 500/1000"})

        record = tasks.get_job_status(JOB)
        assert record["status"] == "cancelled"
        assert record["phase"] == "Cancelled by user"
        assert tasks.is_upload_cancelled(JOB)

    def test_cancel_stops_a_running_spatial_lookup(self, redis_clients):
        manager = _start_spatial_workflow()
        tasks.update_job_status(
            JOB,
            {
                "status": "processing",
                "spatial_lookup_config": {"table_name": WORKFLOW, "database_name": "staging"},
            },
        )

        tasks.request_upload_cancel(JOB)

        assert manager.get_workflow_data(WORKFLOW).status == "cancelled"
        assert manager.is_workflow_stopped(WORKFLOW)

    def test_cancelled_step_raises_ignore_not_failure(self, redis_clients):
        tasks.request_upload_cancel(JOB)
        with pytest.raises(tasks.UploadCancelled) as raised:
            tasks.raise_if_upload_cancelled(JOB, "test")
        # celery's Ignore stops the chain without recording a task failure
        from celery.exceptions import Ignore
        assert isinstance(raised.value, Ignore)
        tasks.raise_if_upload_cancelled("some_other_job", "test")  # not cancelled: no-op

    def test_process_and_upload_does_not_start_a_cancelled_job(self, redis_clients):
        tasks.request_upload_cancel(JOB)
        app = Flask(__name__)
        with app.app_context(), mock.patch.object(tasks, "chain") as chain:
            result = tasks.process_and_upload.run(
                "gs://bucket/file.csv",
                {"metadata": {"table_name": "t", "schema_type": "synapse"}, "column_mapping": {}},
                {"datastack": "minnie65_phase3_v1"},
                job_id=JOB,
            )
        assert result == {"status": "cancelled"}
        chain.assert_not_called()

    def test_process_csv_stops_mid_file(self, redis_clients):
        app = Flask(__name__)

        def progress(n):
            return {"progress": n * 10.0, "processed_rows": n * 100, "total_rows": 1000,
                    "current_chunk_num": n, "total_chunks": 10}

        def process_and_cancel(*args, chunk_upload_callback=None, **kwargs):
            chunk_upload_callback(progress(1))
            tasks.request_upload_cancel(JOB)  # user cancels while the file is processing
            chunk_upload_callback(progress(2))
            raise AssertionError("processing should have stopped at the cancel")

        processor = mock.MagicMock()
        processor.process_csv_in_chunks.side_effect = process_and_cancel
        with app.app_context(), mock.patch.object(tasks, "GCSCsvProcessor", return_value=processor), \
                mock.patch.object(tasks, "SchemaProcessor"):
            with pytest.raises(tasks.UploadCancelled):
                tasks.process_csv.run(
                    file_path="gs://bucket/file.csv",
                    schema_type="synapse",
                    column_mapping={},
                    job_id_for_status=JOB,
                )
        assert tasks.get_job_status(JOB)["status"] == "cancelled"

    @pytest.mark.parametrize(
        "run_step",
        [
            lambda: tasks.upload_to_database.run(
                {"job_id_for_status": JOB, "output_path": "b/f.csv"},
                sql_instance_name="i", file_metadata={"metadata": {}}, datastack_info={},
            ),
            lambda: tasks.cluster_staging_tables.run(
                {"datastack_info": {"segmentation_source": "graphene://x/table/pcg"},
                 "table_name": WORKFLOW, "job_id_for_status": JOB}
            ),
            lambda: tasks.transfer_to_production.run(
                {"datastack_info": {}, "table_name": WORKFLOW,
                 "materialization_time_stamp": "2026-10-03 09:00:00.000000",
                 "job_id_for_status": JOB}
            ),
        ],
        ids=["upload_to_db", "cluster_staging_tables", "transfer_to_production"],
    )
    def test_later_steps_stop_without_marking_an_error(self, redis_clients, run_step):
        tasks.request_upload_cancel(JOB)
        app = Flask(__name__)
        with app.app_context(), mock.patch.object(tasks, "cluster_table_by_id") as cluster, \
                mock.patch.object(tasks, "transfer_table_using_pg_dump") as transfer:
            with pytest.raises(tasks.UploadCancelled):
                run_step()
        cluster.assert_not_called()
        transfer.assert_not_called()
        assert tasks.get_job_status(JOB)["status"] == "cancelled"

    def test_monitor_stops_the_spatial_lookup_and_the_chain(self, redis_clients):
        manager = _start_spatial_workflow()
        tasks.request_upload_cancel(JOB)  # job record has no spatial config yet
        assert manager.get_workflow_data(WORKFLOW).status == "processing_chunks"

        with pytest.raises(tasks.UploadCancelled):
            tasks.monitor_spatial_workflow_completion.run(
                {"workflow_name": WORKFLOW, "database_name": "staging"},
                datastack_info={},
                table_name_for_transfer=WORKFLOW,
                materialization_time_stamp="2026-10-03 09:00:00",
                job_id_for_status=JOB,
            )
        assert manager.get_workflow_data(WORKFLOW).status == "cancelled"

    def test_dispatcher_and_chunk_tasks_stop_for_a_cancelled_workflow(self, redis_clients):
        manager = _start_spatial_workflow()
        manager.update_workflow(table_name=WORKFLOW, status="cancelled")

        with mock.patch.object(spatial_lookup.db_manager, "get_engine"), \
                mock.patch.object(spatial_lookup, "chord") as chord, \
                mock.patch.object(spatial_lookup, "ChunkingStrategy") as chunking:
            result = spatial_lookup.process_table_in_chunks.run(
                datastack_info={}, mat_metadata={"x": 1}, workflow_name=WORKFLOW,
                annotation_table_name=WORKFLOW, database_name="staging",
                chunk_scale_factor=1, supervoxel_batch_size=50,
            )
        assert result is None
        chord.assert_not_called()
        chunking.assert_not_called()
        # nothing reset the status back to processing_chunks
        assert manager.get_workflow_data(WORKFLOW).status == "cancelled"

        chunk = spatial_lookup.process_chunk.run(
            [0, 0, 0], [1, 1, 1], {}, {"chunk_idx": 7}, "staging", workflow_name=WORKFLOW
        )
        assert chunk == {"status": "skipped_workflow_stopped", "chunk_idx": 7}

        sub_batch = spatial_lookup.process_and_insert_sub_batch.run(
            [{"id": 1}], {}, "staging", 7, 0, WORKFLOW
        )
        assert sub_batch["status"] == "skipped_workflow_stopped"

    def test_cancelled_workflow_is_not_resumed(self, redis_clients):
        manager = _start_spatial_workflow()
        manager.update_workflow(table_name=WORKFLOW, status="cancelled")
        with mock.patch.object(spatial_lookup, "get_materialization_info", return_value=[{}]), \
                mock.patch.object(spatial_lookup.process_table_in_chunks, "si") as dispatch, \
                mock.patch.object(spatial_lookup, "chain"), \
                mock.patch.object(RedisCheckpointManager, "initialize_workflow",
                                  wraps=manager.initialize_workflow) as initialize:
            try:
                spatial_lookup.run_spatial_lookup_workflow.run(
                    {"datastack": "minnie65_phase3_v1", "aligned_volume": {"name": "av"}},
                    WORKFLOW,
                    use_staging_database=True,
                    resume_from_checkpoint=True,
                )
            except Exception:
                pass  # only the resume decision matters here
        initialize.assert_called_once()  # started fresh, not resumed

    def test_cancel_endpoint_cancels_without_a_worker(self, redis_clients):
        app = Flask(__name__)
        with app.test_request_context(method="POST"), \
                mock.patch.object(api, "is_auth_disabled", return_value=True):
            g.auth_user = None
            response = api.cancel_job.__wrapped__(JOB)

        body = json.loads(response.get_data())
        assert body["status"] == "success"
        assert body["details"]["status"] == "cancelled"
        assert tasks.is_upload_cancelled(JOB)
