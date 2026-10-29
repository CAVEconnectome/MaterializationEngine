"""Where the upload steps in blueprints/upload/tasks.py report to, for one context.

The steps call update_job_status, get_job_status, is_upload_cancelled and the import
state helpers. With no sink set (celery) those use redis, as before. The Ray driver
sets a StoreSink around each step, so the same code reports to the upload state
database instead, fenced on the driver's lease epoch. A part runner can be set the
same way, to run the CSV/parquet part tasks somewhere other than celery.
"""

import contextlib
import contextvars
from typing import Any, Callable, Dict, List, Optional

from materializationengine.upload_state import store

_sink: contextvars.ContextVar = contextvars.ContextVar("upload_status_sink", default=None)
_part_runner: contextvars.ContextVar = contextvars.ContextVar("upload_part_runner", default=None)


class StoreSink:
    """Job status, cancellation and import state of one job, in the upload state database."""

    def __init__(self, engine, job_id: str, epoch: Optional[int] = None, stage: Optional[str] = None):
        self.engine = engine
        self.job_id = job_id
        self.epoch = epoch
        self.stage = stage

    def handles(self, job_id: Optional[str]) -> bool:
        return bool(job_id) and job_id == self.job_id

    def update_job(self, status: Dict[str, Any]) -> bool:
        return store.update_job(
            self.engine,
            self.job_id,
            status,
            epoch=self.epoch,
            stage=self.stage,
            finished=status.get("status") in ("done", "error", "cancelled"),
        )

    def get_job(self) -> Optional[dict]:
        return store.get_job_record(self.engine, self.job_id)

    def is_cancelled(self) -> bool:
        return store.is_cancel_requested(self.engine, self.job_id)

    def get_import_state(self, kind: str = "import") -> Dict[str, Any]:
        row = store.get_import(self.engine, self.job_id, kind)
        if not row:
            return {}
        state = {"status": row["status"], "uri": row["uri"]}
        if row.get("operation_name"):
            state["operation"] = row["operation_name"]
        return state

    def clear_import_state(self, kind: str = "import") -> None:
        store.reset_import(self.engine, self.job_id, kind)

    def set_import_state(self, kind: str = "import", **fields) -> None:
        """The import state fields tasks.py writes: uri+database+table (the intent),
        operation (started), status (running/done/failed)."""
        if fields.get("status") == "requested":
            store.record_import_intent(
                self.engine, self.job_id, fields["uri"], fields["database"], fields["table"], kind
            )
            return
        if fields.get("operation"):
            store.set_import_operation(self.engine, self.job_id, fields["operation"], kind)
        status = fields.get("status")
        if status in ("done", "failed"):
            store.finish_import(self.engine, self.job_id, status, fields.get("error"), kind)


def current(job_id: Optional[str]) -> Optional[StoreSink]:
    """The sink for job_id in this context, or None to use redis."""
    sink = _sink.get()
    return sink if sink is not None and sink.handles(job_id) else None


@contextlib.contextmanager
def use_sink(sink: StoreSink):
    token = _sink.set(sink)
    try:
        yield sink
    finally:
        _sink.reset(token)


PartRunner = Callable[[List[Any], str, str], List[Any]]


def part_runner() -> Optional[PartRunner]:
    """A replacement for _run_csv_part_tasks: (signatures, job_id, phase) -> results."""
    return _part_runner.get()


@contextlib.contextmanager
def use_part_runner(runner: PartRunner):
    token = _part_runner.set(runner)
    try:
        yield runner
    finally:
        _part_runner.reset(token)
