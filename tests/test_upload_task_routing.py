"""Each upload step runs on the queue that fits how long it runs.

The long steps (hours on a 337M-row table) run on the orchestration queue: each in a
KEDA job pod of its own, on a non-preemptible node, that is never scaled down mid task.
They are acked late, so a pod lost anyway means a redelivery, which each step handles.
The process queue is for short tasks that are cheap to retry.
"""

import pytest

from materializationengine.blueprints.upload import tasks
from materializationengine.task_router import TaskRouter


class TestUploadTaskRouting:
    """Each upload step is routed to the queue that fits how long it runs."""

    @pytest.mark.parametrize(
        "task",
        [
            tasks.upload_to_database,  # a redelivery resumes from the recorded import
            tasks.cluster_staging_tables,
            tasks.transfer_to_production,  # rerun is safe: tables are truncated first
        ],
        ids=["upload_to_db", "cluster_staging_tables", "transfer_to_production"],
    )
    def test_long_running_upload_tasks_run_in_orchestration_jobs(self, task):
        assert TaskRouter().route_for_task(task.name) == {"queue": "orchestration"}
        assert task.acks_late is True

    @pytest.mark.parametrize(
        "alias, task",
        [
            ("workflow:upload_to_db", tasks.upload_to_database),
            ("workflow:cluster_staging_tables", tasks.cluster_staging_tables),
            ("workflow:transfer_to_production", tasks.transfer_to_production),
        ],
    )
    def test_old_workflow_names_still_run_the_task(self, alias, task):
        # messages queued under the old name before a deploy
        from unittest import mock

        from materializationengine.celery_init import celery

        alias_task = celery.tasks[alias]
        assert TaskRouter().route_for_task(alias) == {"queue": "workflow"}
        with mock.patch.object(task, "run", return_value="ran") as run:
            assert alias_task.run(1, key="v") == "ran"
        run.assert_called_once_with(1, key="v")

    @pytest.mark.parametrize(
        "task",
        [tasks.process_csv, tasks.process_and_upload, tasks.cancel_processing_job],
        ids=["process_csv", "process_and_upload", "cancel_processing"],
    )
    def test_other_upload_tasks_keep_their_queues(self, task):
        assert TaskRouter().route_for_task(task.name)["queue"] in {"orchestration", "process"}

    @pytest.mark.parametrize(
        "task",
        [tasks.process_csv_part, tasks.count_csv_part],
        ids=["process_csv_part", "count_csv_part"],
    )
    def test_csv_part_tasks_are_short_retryable_process_tasks(self, task):
        # each part runs for about a minute and overwrites its own output, so the
        # process queue's 300 s visibility timeout and a redelivery are both fine
        assert TaskRouter().route_for_task(task.name) == {"queue": "process"}
        assert task.acks_late
