"""update_database_workflow holds one lock per datastack, however it was queued.

LockedTask hashed every argument: the hourly update (kwargs Datastack) and the admin
endpoint (datastack_info with lookup_all_root_ids) locked different keys, so both
could update one live database at once, and periodic materialization's check saw
only the hourly one.
"""

from unittest import mock

import pytest
from celery.app.task import Task

from materializationengine import task as task_module
from materializationengine.task import LockedTask, argument_signature, datastack_lock_id
from materializationengine.workflows import periodic_materialization
from materializationengine.workflows.update_database_workflow import (
    update_database_workflow,
)

INFO = {"datastack": "minnie65_phase3_v1", "aligned_volume": {"name": "minnie65_phase3"}}
ADMIN_INFO = {**INFO, "lookup_all_root_ids": False}


class FakeRedis:
    def __init__(self):
        self.data = {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return None
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        return int(self.data.pop(key, None) is not None)


@pytest.fixture
def redis():
    fake = FakeRedis()
    with mock.patch.object(task_module, "REDIS_CLIENT", fake), \
            mock.patch.object(periodic_materialization, "REDIS_CLIENT", fake), \
            mock.patch.object(Task, "apply_async", return_value="queued"):
        yield fake


def hourly(datastack="minnie65_phase3_v1", info=INFO):
    """run_periodic_database_update's enqueue."""
    return update_database_workflow.s({**info, "datastack": datastack}).apply_async(
        kwargs={"Datastack": datastack}
    )


def admin(info=ADMIN_INFO):
    """The admin endpoint's enqueue (/materialize/run/update_database)."""
    return update_database_workflow.s(dict(info)).apply_async()


class TestDatastackLock:
    def test_hourly_and_admin_enqueues_share_one_lock(self):
        assert update_database_workflow.lock_id(
            [INFO], {"Datastack": "minnie65_phase3_v1"}
        ) == update_database_workflow.lock_id([ADMIN_INFO], {})
        assert update_database_workflow.lock_id([ADMIN_INFO], {}) == datastack_lock_id(
            update_database_workflow.name, "minnie65_phase3_v1"
        )

    def test_an_admin_run_locks_out_the_hourly_one(self, redis):
        assert admin() == "queued"
        with pytest.raises(KeyError, match="duplicate"):
            hourly()

    def test_an_hourly_run_locks_out_an_admin_one(self, redis):
        assert hourly() == "queued"
        with pytest.raises(KeyError, match="duplicate"):
            admin()

    def test_other_datastacks_are_not_locked_out(self, redis):
        admin()
        assert hourly(datastack="zheng_ca3") == "queued"

    def test_finishing_releases_the_lock(self, redis):
        admin()
        # what the worker passes on_success: the args the task ran with
        update_database_workflow.on_success(True, "task-id", [ADMIN_INFO], {})
        assert redis.data == {}
        assert hourly() == "queued"

    def test_failing_releases_the_lock(self, redis):
        hourly()
        update_database_workflow.on_failure(
            RuntimeError("x"), "task-id", [INFO], {"Datastack": "minnie65_phase3_v1"}, None
        )
        assert redis.data == {}

    def test_materialization_sees_an_admin_queued_update(self, redis):
        assert not periodic_materialization.update_database_workflow_locked(
            "minnie65_phase3_v1", INFO
        )
        admin()
        assert periodic_materialization.update_database_workflow_locked(
            "minnie65_phase3_v1", INFO
        )

    def test_lock_keeps_the_prefix_the_lock_inspector_lists(self):
        assert datastack_lock_id("t", "ds").startswith("LOCKED_WORKFLOW_TASK")


class TestOtherLockedTasks:
    def test_plain_locked_tasks_still_lock_per_argument_set(self):
        task = mock.Mock(spec=LockedTask)
        task.name = "orchestration:something"
        assert LockedTask.lock_id(task, [1], {"a": 2}) == argument_signature(
            "orchestration:something", [1], {"a": 2}
        )
