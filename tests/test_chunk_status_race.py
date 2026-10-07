"""A chunk marked COMPLETED stays COMPLETED, whatever order status writes arrive in.

Runs against a real redis (WATCH/MULTI semantics matter here). Start one with
`docker run -d --rm -p 6390:6379 redis:7`, or the tests are skipped.
"""

import os
import threading
from unittest import mock

import pytest
import redis

from materializationengine.blueprints.upload import checkpoint_manager as cm

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))


@pytest.fixture
def real_redis():
    client = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=15)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    client.flushdb()
    with mock.patch.object(cm, "REDIS_CLIENT", client):
        yield client
    client.flushdb()


@pytest.fixture
def manager(real_redis):
    manager = cm.RedisCheckpointManager("staging")
    manager.initialize_workflow("synapses", task_id="t1")
    return manager


class TestCompletedIsFinal:
    def test_late_processing_subtasks_does_not_overwrite_completed(self, manager):
        # chunk 2541 of ltv7 final2: finalize marked it COMPLETED before process_chunk's
        # delayed PROCESSING_SUBTASKS write landed
        manager.set_chunk_status("synapses", 2541, cm.CHUNK_STATUS_PROCESSING)
        assert manager.set_chunk_status("synapses", 2541, cm.CHUNK_STATUS_COMPLETED) is not False
        assert manager.set_chunk_status(
            "synapses", 2541, cm.CHUNK_STATUS_PROCESSING_SUBTASKS, {"sub_chord_id": "x"}
        ) is False

        assert manager.get_chunk_status("synapses", 2541) == cm.CHUNK_STATUS_COMPLETED
        assert manager.get_workflow_data("synapses").completed_chunks == 1

    @pytest.mark.parametrize(
        "late_status",
        [cm.CHUNK_STATUS_PROCESSING, cm.CHUNK_STATUS_FAILED_RETRYABLE, cm.CHUNK_STATUS_PENDING],
    )
    def test_no_status_replaces_completed(self, manager, late_status):
        manager.set_chunk_status("synapses", 7, cm.CHUNK_STATUS_COMPLETED)
        manager.set_chunk_status("synapses", 7, late_status)
        assert manager.get_chunk_status("synapses", 7) == cm.CHUNK_STATUS_COMPLETED

    def test_completing_twice_counts_once(self, manager):
        manager.set_chunk_status("synapses", 3, cm.CHUNK_STATUS_COMPLETED, {"rows_processed": 10})
        manager.set_chunk_status("synapses", 3, cm.CHUNK_STATUS_PROCESSING_SUBTASKS)
        manager.set_chunk_status("synapses", 3, cm.CHUNK_STATUS_COMPLETED, {"rows_processed": 10})
        data = manager.get_workflow_data("synapses")
        assert (data.completed_chunks, data.rows_processed) == (1, 10)

    def test_new_run_can_reprocess_a_completed_chunk(self, manager):
        manager.set_chunk_status("synapses", 4, cm.CHUNK_STATUS_COMPLETED)
        manager.initialize_workflow("synapses", task_id="t2")
        manager.set_chunk_status("synapses", 4, cm.CHUNK_STATUS_PROCESSING)
        assert manager.get_chunk_status("synapses", 4) == cm.CHUNK_STATUS_PROCESSING

    def test_concurrent_writers_never_leave_a_completed_chunk_unfinished(self, manager):
        # Many writers on the one workflow key, as with 100 spatial workers: each chunk
        # gets its finalize and a racing late PROCESSING_SUBTASKS write
        chunks = range(40)
        barrier = threading.Barrier(2 * len(chunks))

        def write(chunk, status):
            barrier.wait()
            manager.set_chunk_status("synapses", chunk, status)

        threads = [
            threading.Thread(target=write, args=(c, s))
            for c in chunks
            for s in (cm.CHUNK_STATUS_COMPLETED, cm.CHUNK_STATUS_PROCESSING_SUBTASKS)
        ]
        with mock.patch.object(cm.time, "sleep"):
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        statuses = manager.get_all_chunk_statuses("synapses")
        completed = [c for c in chunks if statuses.get(str(c)) == cm.CHUNK_STATUS_COMPLETED]
        # A chunk whose COMPLETED write landed is COMPLETED; the counter matches
        assert manager.get_workflow_data("synapses").completed_chunks == len(completed)
        assert len(completed) == len(chunks)


class TestCountersSurviveConcurrentWorkflowUpdates:
    def test_completion_during_update_workflow_is_not_overwritten(self, manager):
        # Deterministic version of the final3 race: pause update_workflow between its
        # read and its write while another worker completes a chunk.
        real_get = manager.get_workflow_data
        paused = threading.Event()
        completer = threading.Thread(
            target=manager.set_chunk_status,
            args=("synapses", 9, cm.CHUNK_STATUS_COMPLETED, {"rows_processed": 7}),
        )

        def get_then_let_a_chunk_complete(table_name):
            data = real_get(table_name)
            if not paused.is_set():
                paused.set()
                completer.start()
                completer.join(timeout=1.0)  # without the lock it lands now
            return data

        with mock.patch.object(manager, "get_workflow_data", side_effect=get_then_let_a_chunk_complete):
            manager.update_workflow("synapses", current_pending_scan_cursor=3)
        completer.join()

        data = manager.get_workflow_data("synapses")
        assert manager.get_chunk_status("synapses", 9) == cm.CHUNK_STATUS_COMPLETED
        assert (data.completed_chunks, data.rows_processed) == (1, 7)
        assert data.current_pending_scan_cursor == 3
