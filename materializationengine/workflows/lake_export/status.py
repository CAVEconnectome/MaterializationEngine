"""A table export's progress, where the API and the wizard already read it.

The same Redis record as before (deltalake_export:<ds>:v<v>:<table>:<job_id>, read by
get_deltalake_export_progress and the wizard's Running Exports page), with the same
fields, plus ranges_done / ranges_total and lakes_optimized / lakes_total. Writes are
best effort: losing Redis never fails an export.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Optional

from materializationengine.workflows import deltalake_export as dx

logger = logging.getLogger(__name__)

PHASES = ("planning", "streaming", "optimizing", "finalizing", "complete", "failed")


def _redis_writer(key: str, payload: dict) -> None:
    dx._get_redis_client().set(key, json.dumps(payload), ex=dx._DELTALAKE_PROGRESS_TTL)


def _redis_logger(datastack, version, table, job_id, message: str) -> None:
    dx.append_deltalake_log(datastack, version, table, message, job_id=job_id)


class TableStatus:
    def __init__(self, datastack: str, version: int, table: str, job_id: str,
                 write: Optional[Callable[[str, dict], None]] = None,
                 log: Optional[Callable[..., None]] = None, min_interval: float = 1.0):
        self.datastack, self.version, self.table, self.job_id = datastack, int(version), table, job_id
        self.key = dx.deltalake_export_redis_key(datastack, version, table, job_id=job_id)
        self._write = write or _redis_writer
        self._log = log or _redis_logger
        self.min_interval = min_interval
        self.started = time.monotonic()
        self._last = 0.0
        self.fields = {"status": "exporting", "phase": "planning", "rows_processed": 0, "total_rows": None,
                       "ranges_done": 0, "ranges_total": None, "lakes_optimized": 0, "lakes_total": None,
                       "error": None}

    def log(self, message: str) -> None:
        logger.info("%s v%d %s: %s", self.datastack, self.version, self.table, message)
        try:
            self._log(self.datastack, self.version, self.table, self.job_id, message)
        except Exception:
            logger.warning("could not append to the export log of %s", self.key, exc_info=True)

    def update(self, force: bool = False, **fields) -> None:
        self.fields.update(fields)
        now = time.monotonic()
        if not force and now - self._last < self.min_interval:
            return
        self._last = now
        elapsed = now - self.started
        rows, total = self.fields["rows_processed"], self.fields["total_rows"]
        payload = {
            **self.fields,
            "percent_complete": round(rows / total * 100, 2) if rows is not None and total else None,
            "elapsed_seconds": round(elapsed, 1),
            "rows_per_second": round(rows / elapsed) if rows and elapsed > 0 else None,
            "last_updated": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._write(self.key, payload)
        except Exception:
            logger.warning("could not write export status %s", self.key, exc_info=True)

    def phase(self, phase: str, message: str, **fields) -> None:
        self.log(message)
        self.update(force=True, status="exporting", phase=phase, **fields)

    def complete(self) -> None:
        self.log("Export complete.")
        self.update(force=True, status="complete", phase="complete", rows_processed=self.fields["total_rows"])

    def failed(self, error: str) -> None:
        self.log(f"Export failed: {error}")
        self.update(force=True, status="failed", phase="failed", error=error)
