"""Spatial lookup chunk tasks run on their own queue, served by their own worker pool.

They use about 0.9 core and up to ~1.5GB each, unlike the light process-queue tasks,
so they get a worker deployment sized for them instead of inflating every consumer.
"""

from types import SimpleNamespace
from unittest import mock

import pytest

from materializationengine.task_router import TaskRouter
from materializationengine.workflows import spatial_lookup


class TestSpatialQueueRouting:
    @pytest.mark.parametrize(
        "task",
        [spatial_lookup.process_chunk, spatial_lookup.process_and_insert_sub_batch],
        ids=["process_chunk", "process_and_insert_sub_batch"],
    )
    def test_chunk_tasks_route_to_spatial_queue(self, task):
        assert TaskRouter().route_for_task(task.name) == {"queue": "spatial"}
        assert task.acks_late


class TestWorkerIsolationQueue:
    """A worker whose database is unreachable stops consuming the queue the task came from."""

    @pytest.fixture(autouse=True)
    def reset_isolation_state(self):
        spatial_lookup._consecutive_infra_failures = 0
        spatial_lookup._worker_isolated = False
        yield
        spatial_lookup._consecutive_infra_failures = 0
        spatial_lookup._worker_isolated = False

    def _task(self, delivery_info):
        return SimpleNamespace(
            name="spatial:process_chunk",
            request=SimpleNamespace(hostname="worker.spatial@pod", delivery_info=delivery_info),
            app=mock.MagicMock(),
        )

    @pytest.mark.parametrize(
        "delivery_info, queue",
        [({"routing_key": "spatial"}, "spatial"), ({}, "spatial"), (None, "spatial")],
        ids=["delivered", "no-routing-key", "no-delivery-info"],
    )
    def test_task_queue(self, delivery_info, queue):
        assert spatial_lookup._task_queue(self._task(delivery_info)) == queue

    def test_isolation_cancels_the_tasks_queue(self):
        task = self._task({"routing_key": "spatial"})
        with mock.patch.object(spatial_lookup.threading, "Thread") as thread:
            for _ in range(spatial_lookup._INFRA_ISOLATION_THRESHOLD):
                spatial_lookup._on_connection_error("db", task)
        task.app.control.cancel_consumer.assert_called_once_with(
            "spatial", destination=["worker.spatial@pod"], reply=False
        )
        assert thread.call_args.kwargs["args"][-1] == "spatial"
