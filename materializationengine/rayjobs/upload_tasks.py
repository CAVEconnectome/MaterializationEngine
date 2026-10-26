"""The bulk upload's work that runs on Ray workers.

Each task is the body of the matching celery task, run in a Flask app that has only
the configuration (create_worker_app): no blueprints, and none of create_app's startup
work (CORS on the upload bucket, staging database setup, the admin views), which every
one of dozens of workers would otherwise repeat. Workers reach only the staging
database, through their own cloud-sql-proxy, and never touch redis: the driver owns
the job's state, so cancellation is checked there, not here.
"""

import logging
import pickle
import threading
from typing import Any, Dict

logger = logging.getLogger(__name__)

_app = None
_app_lock = threading.Lock()


def create_worker_app():
    """The app configuration, as create_app loads it, without its startup side effects."""
    from flask import Flask

    from materializationengine.config import configure_app

    app = Flask("materializationengine.ray_worker")
    # configure_app also pushes an app context; run_task pushes its own anyway
    app = configure_app(app)
    logging.basicConfig(level=app.config.get("LOGGING_LEVEL", logging.INFO))
    return app


def worker_app():
    global _app
    with _app_lock:
        if _app is None:
            _app = create_worker_app()
    return _app


def count_csv_part(bucket_name: str, source_blob_name: str, start: int, end: int) -> int:
    from materializationengine.blueprints.upload.tasks import count_csv_part as task

    return task.run(bucket_name, source_blob_name, start, end)


def csv_part(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    from materializationengine.blueprints.upload.tasks import process_csv_part

    return process_csv_part.run(**{**kwargs, "job_id_for_status": None})


def parquet_part(kwargs: Dict[str, Any]) -> Dict[str, Any]:
    from materializationengine.blueprints.upload.tasks import process_parquet_part

    return process_parquet_part.run(**{**kwargs, "job_id_for_status": None})


def spatial_chunk(database: str, mat_metadata: dict, min_corner, max_corner, sub_batch_size: int) -> Dict[str, int]:
    from materializationengine.workflows.spatial_lookup import process_chunk_points

    return process_chunk_points(database, mat_metadata, min_corner, max_corner, sub_batch_size)


TASKS = {
    "count_csv_part": count_csv_part,
    "csv_part": csv_part,
    "parquet_part": parquet_part,
    "spatial_chunk": spatial_chunk,
}

# The celery task each part signature names, and the task here that runs its body.
CELERY_PART_TASKS = {
    "process:count_csv_part": "count_csv_part",
    "process:process_csv_part": "csv_part",
    "process:process_parquet_part": "parquet_part",
}


class TaskFailed(Exception):
    """A task's exception that could not travel back to the driver as itself."""

    def __init__(self, error_type: str, message: str):
        super().__init__(error_type, message)
        self.error_type = error_type
        self.message = message

    def __str__(self):
        return f"{self.error_type}: {self.message}"


def run_task(name: str, *args):
    """Entry point of every Ray task: run TASKS[name] in the worker's app context.

    Ray sends a task's exception back pickled. Many client libraries' exceptions do
    not pickle (google.api_core's hold a lock), and the driver then saw only "isn't
    serializable"; those are re-raised as TaskFailed with their type and message.
    """
    with worker_app().app_context():
        try:
            return TASKS[name](*args)
        except Exception as e:
            try:
                pickle.dumps(e)
            except Exception:
                raise TaskFailed(type(e).__name__, str(e)) from None
            raise
