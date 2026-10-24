"""Long-running upload steps run on the workflow queue, not the process queue.

The process queue's consumers are preemptible, scale down quickly and use a short
visibility timeout; it is meant for short tasks that are cheap to retry. The upload
steps below hold their worker for minutes to hours.
"""

import pytest

from materializationengine.blueprints.upload import tasks
from materializationengine.task_router import TaskRouter


class TestUploadTaskRouting:
    """Each upload step is routed to the queue that fits how long it runs."""

    @pytest.mark.parametrize(
        "task, acks_late",
        [
            (tasks.upload_to_database, False),  # rerun would import the rows twice
            (tasks.cluster_staging_tables, True),
            (tasks.transfer_to_production, True),  # rerun is safe: tables are truncated first
        ],
        ids=["upload_to_db", "cluster_staging_tables", "transfer_to_production"],
    )
    def test_long_running_upload_tasks_use_workflow_queue(self, task, acks_late):
        assert TaskRouter().route_for_task(task.name) == {"queue": "workflow"}
        assert bool(task.acks_late) is acks_late

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
