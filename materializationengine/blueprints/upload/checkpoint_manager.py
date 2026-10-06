import contextlib
import datetime
import hashlib
import json
import random
import time
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import redis

from materializationengine.redis_client import SharedRedis
from celery.utils.log import get_task_logger

from materializationengine.utils import get_config_param


celery_logger = get_task_logger(__name__)


REDIS_CLIENT = SharedRedis(db=1)

CHUNK_STATUS_PENDING = "PENDING"
CHUNK_STATUS_PROCESSING = "PROCESSING"
CHUNK_STATUS_PROCESSING_SUBTASKS = "PROCESSING_SUBTASKS"
CHUNK_STATUS_COMPLETED = "COMPLETED"
CHUNK_STATUS_FAILED_RETRYABLE = "FAILED_RETRYABLE"
CHUNK_STATUS_FAILED_PERMANENT = "FAILED_PERMANENT"
CHUNK_STATUS_ERROR = "ERROR"

# Workflow statuses that mean no more chunks should be processed or dispatched.
WORKFLOW_STATUS_CANCELLED = "cancelled"
WORKFLOW_STOPPED_STATUSES = {WORKFLOW_STATUS_CANCELLED, "failed"}


@dataclass
class ChunkInfo:
    """Information about a processing chunk."""

    min_corner: List[float]
    max_corner: List[float]
    index: int = 0
    updated_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )


@dataclass
class WorkflowData:
    """Data model for spatial lookup workflow state."""

    table_name: str
    task_id: str
    datastack_name: Optional[str] = None
    status: str = "initializing"

    total_chunks: int = 0
    completed_chunks: int = 0
    # submitted_chunks: int = 0 # Kept for now, may be removed if redundant with new status system
    rows_processed: int = 0

    start_time: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    updated_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    created_at: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat()
    )
    estimated_completion: Optional[str] = None

    processing_rate: Optional[str] = None
    total_row_estimate: Optional[int] = None
    # Points to look up: rows x point columns with a supervoxel column (2 for a synapse
    # table). rows_processed counts points, so this, not total_row_estimate, is what it
    # runs up to.
    total_point_estimate: Optional[int] = None
    # Recent (iso time, rows_processed) samples, for a rate that follows the current
    # throughput rather than the average since the start (which includes scale-up)
    rate_samples: Optional[List[List[Any]]] = None

    min_enclosing_bbox: Optional[List[List[float]]] = None
    bbox_hash: Optional[str] = None

    latest_completed_chunk_info: Optional[ChunkInfo] = None
    mat_info_idx: int = 0
    chunking_strategy: Optional[str] = "grid"
    used_chunk_size: Optional[int] = 1024

    last_error: Optional[str] = None
    last_failure_time: Optional[str] = None
    chunking_parameters: Optional[dict] = None
    current_pending_scan_cursor: Optional[int] = 0
    submitted_chunks: int = 0
    recovery_attempts: int = 0

    @property
    def progress(self) -> float:
        """Calculate progress percentage."""
        if self.total_chunks <= 0:
            return 0.0
        return (self.completed_chunks / self.total_chunks) * 100


RATE_WINDOW_SECONDS = 300
RATE_SAMPLE_SPACING_SECONDS = 15


def _progress_estimate(workflow_data: "WorkflowData", rows_processed: int, now_iso: str) -> dict:
    """processing_rate, estimated_completion and rate_samples after a chunk completes.

    rows_processed counts looked-up points, so the remaining work is measured against
    total_point_estimate. It used to be measured against total_row_estimate (rows,
    half the points of a synapse table), so the estimate reached zero halfway through
    and read "done now" from then on (ltv7, 2026-10-06).
    The rate is taken over the last RATE_WINDOW_SECONDS, falling back to the average
    since the start while the window is still filling.
    """
    now_dt = datetime.datetime.fromisoformat(now_iso)
    samples = [
        s for s in (workflow_data.rate_samples or [])
        if (now_dt - datetime.datetime.fromisoformat(s[0])).total_seconds() <= RATE_WINDOW_SECONDS
    ]
    if not samples or (now_dt - datetime.datetime.fromisoformat(samples[-1][0])).total_seconds() >= RATE_SAMPLE_SPACING_SECONDS:
        samples.append([now_iso, rows_processed])
    result: Dict[str, Any] = {"rate_samples": samples}

    oldest_dt = datetime.datetime.fromisoformat(samples[0][0])
    window_seconds = (now_dt - oldest_dt).total_seconds()
    if window_seconds >= 60:
        rate = (rows_processed - samples[0][1]) / window_seconds
    elif workflow_data.start_time:
        elapsed = (now_dt - datetime.datetime.fromisoformat(workflow_data.start_time)).total_seconds()
        rate = rows_processed / elapsed if elapsed > 0 else 0.0
    else:
        rate = 0.0
    if rate > 0:
        result["processing_rate"] = f"{rate * 60:.2f} rows/minute"

    total = workflow_data.total_point_estimate
    if total and total > 0:
        remaining = total - rows_processed
        if remaining <= 0:
            result["estimated_completion"] = now_iso
        elif rate > 0:
            result["estimated_completion"] = (
                now_dt + datetime.timedelta(seconds=remaining / rate)
            ).isoformat()
    return result


class RedisCheckpointManager:
    """Manages workflow checkpoints and status for spatial lookup operations using Redis."""

    def __init__(self, database: str):
        """Initialize with database name for namespacing keys."""
        self.database = database
        self.workflow_prefix = f"workflow:{self.database}:"
        self.expiry_time = 86400 * 7  # 7 days for all Redis keys

    def _get_workflow_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}"

    def _get_chunk_statuses_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:chunk_statuses"

    def _get_chunk_failed_details_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:chunk_failed_details"

    def _get_retryable_chunks_set_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:failed_retryable_chunks"

    def _get_processing_subtasks_timestamps_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:processing_subtasks_timestamps"

    def _get_processing_timestamps_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:processing_timestamps"

    def _get_dispatch_params_key(self, table_name: str) -> str:
        return f"{self.workflow_prefix}{table_name}:dispatch_params"

    def get_bbox_hash(self, bbox: Union[np.ndarray, List]) -> str:
        """Generate hash for bounding box."""
        bbox_list = bbox.tolist() if isinstance(bbox, np.ndarray) else bbox
        return hashlib.md5(json.dumps(bbox_list).encode()).hexdigest()

    def is_workflow_stopped(self, table_name: str) -> bool:
        """True if the workflow was cancelled or failed, so chunk work should stop."""
        workflow_data = self.get_workflow_data(table_name)
        return bool(workflow_data and workflow_data.status in WORKFLOW_STOPPED_STATUSES)

    def get_workflow_data(self, table_name: str) -> Optional[WorkflowData]:
        """Get the complete workflow data for a table."""
        key = self._get_workflow_key(table_name)
        workflow_json = REDIS_CLIENT.get(key)

        if workflow_json:
            try:
                data_dict = json.loads(workflow_json)

                for f_name in ["chunking_parameters", "current_pending_scan_cursor"]:
                    if f_name not in data_dict:
                        data_dict[f_name] = None

                if (
                    "last_processed_chunk" in data_dict
                    and "latest_completed_chunk_info" not in data_dict
                ):
                    data_dict["latest_completed_chunk_info"] = data_dict.pop(
                        "last_processed_chunk"
                    )

                last_chunk = data_dict.get("latest_completed_chunk_info")
                if last_chunk and isinstance(last_chunk, dict):
                    data_dict["latest_completed_chunk_info"] = ChunkInfo(**last_chunk)
                elif last_chunk is None:
                    data_dict["latest_completed_chunk_info"] = None

                return WorkflowData(**data_dict)
            except (json.JSONDecodeError, TypeError) as e:
                celery_logger.error(f"Error parsing workflow data: {str(e)}")
                return None
        return None

    def get_progress(self, table_name: str) -> Dict[str, Any]:
        """Get workflow progress statistics."""
        workflow_data = self.get_workflow_data(table_name)
        if not workflow_data:
            return {
                "total_chunks": 0,
                "completed_chunks": 0,
                "percentage_complete": 0,
                "status": "not_started",
                "progress": 0,
            }

        data_dict = asdict(workflow_data)
        data_dict["percentage_complete"] = round(workflow_data.progress, 2)

        return data_dict

    def initialize_workflow(
        self, table_name: str, task_id: str, datastack_name: Optional[str] = None
    ) -> WorkflowData:
        """Initialize or reset workflow data."""
        key = self._get_workflow_key(table_name)

        workflow_data = WorkflowData(
            table_name=table_name, task_id=task_id, datastack_name=datastack_name
        )

        self.reset_chunk_statuses_and_details(table_name)

        workflow_data.current_pending_scan_cursor = 0

        REDIS_CLIENT.set(key, json.dumps(asdict(workflow_data)), ex=self.expiry_time)
        return workflow_data

    def update_workflow(
        self, table_name: str, min_enclosing_bbox: Optional[np.ndarray] = None, **kwargs
    ) -> bool:
        """Update workflow data, under the same lock as set_chunk_status."""
        try:
            with self._workflow_write_lock(table_name):
                return self._update_workflow_locked(table_name, min_enclosing_bbox, **kwargs)
        except TimeoutError as e:
            celery_logger.error(f"{e}; workflow fields {sorted(kwargs)} not updated.")
            return False

    def _update_workflow_locked(
        self, table_name: str, min_enclosing_bbox: Optional[np.ndarray] = None, **kwargs
    ) -> bool:
        key = self._get_workflow_key(table_name)

        workflow_data = self.get_workflow_data(table_name)
        if not workflow_data:
            celery_logger.warning(
                f"Workflow data for {table_name} not found during update. Creating a new default one."
            )
            workflow_data = WorkflowData(
                table_name=table_name,
                task_id=kwargs.get("task_id", "unknown_on_update"),
            )

        if (
            "latest_completed_chunk_info" in kwargs
            and kwargs["latest_completed_chunk_info"] is not None
        ):
            last_chunk = kwargs["latest_completed_chunk_info"]
            workflow_data.latest_completed_chunk_info = ChunkInfo(
                min_corner=(
                    last_chunk["min_corner"].tolist()
                    if isinstance(last_chunk["min_corner"], np.ndarray)
                    else last_chunk["min_corner"]
                ),
                max_corner=(
                    last_chunk["max_corner"].tolist()
                    if isinstance(last_chunk["max_corner"], np.ndarray)
                    else last_chunk["max_corner"]
                ),
                index=kwargs.get("last_chunk_index", last_chunk.get("index", 0)),
            )

        elif (
            "last_processed_chunk" in kwargs
            and kwargs["last_processed_chunk"] is not None
        ):
            celery_logger.warning(
                "Using deprecated 'last_processed_chunk' in update_workflow. Please use 'latest_completed_chunk_info'."
            )
            last_chunk = kwargs["last_processed_chunk"]
            workflow_data.latest_completed_chunk_info = ChunkInfo(
                min_corner=(
                    last_chunk["min_corner"].tolist()
                    if isinstance(last_chunk["min_corner"], np.ndarray)
                    else last_chunk["min_corner"]
                ),
                max_corner=(
                    last_chunk["max_corner"].tolist()
                    if isinstance(last_chunk["max_corner"], np.ndarray)
                    else last_chunk["max_corner"]
                ),
                index=kwargs.get("last_chunk_index", last_chunk.get("index", 0)),
            )

        if kwargs.get("increment_completed", 0) > 0:
            workflow_data.completed_chunks += kwargs["increment_completed"]

        if min_enclosing_bbox is not None and not workflow_data.min_enclosing_bbox:
            workflow_data.min_enclosing_bbox = (
                min_enclosing_bbox.tolist()
                if isinstance(min_enclosing_bbox, np.ndarray)
                else min_enclosing_bbox
            )
            workflow_data.bbox_hash = self.get_bbox_hash(min_enclosing_bbox)

        if "last_error" in kwargs and kwargs["last_error"]:
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            workflow_data.last_error = f"[{timestamp}] {kwargs['last_error']}"

        valid_fields = {f.name for f in fields(WorkflowData)}
        for field_name, value in kwargs.items():
            if (
                field_name in valid_fields
                and value is not None
                and field_name
                not in [
                    "last_processed_chunk",
                    "increment_completed",
                    "latest_completed_chunk_info",
                ]
            ):
                setattr(workflow_data, field_name, value)

        workflow_data.updated_at = datetime.datetime.now(
            datetime.timezone.utc
        ).isoformat()

        REDIS_CLIENT.set(key, json.dumps(asdict(workflow_data)), ex=self.expiry_time)
        return True

    def get_processing_rate(self, table_name: str) -> Optional[str]:
        """Get the processing rate for a workflow."""
        workflow_data = self.get_workflow_data(table_name)
        if workflow_data:
            return workflow_data.processing_rate
        return None

    def reset_chunk_statuses_and_details(self, table_name: str):
        """Deletes chunk_statuses, chunk_failed_details, failed_retryable_set, and stale-recovery timestamp keys for the table."""
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        processing_subtasks_ts_key = self._get_processing_subtasks_timestamps_key(table_name)
        processing_ts_key = self._get_processing_timestamps_key(table_name)
        try:
            REDIS_CLIENT.delete(
                chunk_statuses_key, chunk_failed_details_key, retryable_set_key,
                processing_subtasks_ts_key, processing_ts_key,
            )
            celery_logger.info(
                f"Reset chunk statuses, details, and retryable set for table: {table_name}"
            )
        except Exception as e:
            celery_logger.error(
                f"Error resetting chunk data for {table_name}: {str(e)}"
            )

    @contextlib.contextmanager
    def _workflow_write_lock(self, table_name: str):
        """Hold the per-workflow lock taken by every read-modify-write of the workflow key.

        set_chunk_status and update_workflow both read the workflow JSON, change it and
        write it back. Interleaved, the later write discards the earlier one: on ltv7
        final3 (2026-10-06) update_workflow's plain SET lost a chunk's completed_chunks
        increment, leaving 2551/2552 with every chunk COMPLETED. Raises TimeoutError if
        the lock is not acquired within 120s.
        """
        lock = REDIS_CLIENT.lock(
            f"{self._get_workflow_key(table_name)}:status_lock",
            timeout=30,
            blocking_timeout=120,
        )
        if not lock.acquire():
            raise TimeoutError(f"Timed out waiting for the workflow lock for {table_name}")
        try:
            yield
        finally:
            try:
                lock.release()
            except redis.exceptions.LockError:
                # Held past its 30s timeout; another writer may already have it
                celery_logger.warning(f"Workflow lock for {table_name} expired before release.")

    def set_chunk_status(
        self,
        table_name: str,
        chunk_index: int,
        status: str,
        status_payload: Optional[dict] = None,
    ):
        """
        Sets the status of a chunk and updates workflow aggregates.

        Writers take turns on the per-workflow lock (_workflow_write_lock). They all
        rewrite the one workflow key, and with only WATCH (optimistic locking) ~100
        spatial workers collided constantly: ~630 WatchError retries in a 25-minute
        ltv7 upload, and 3 writes dropped after their last attempt, which can leave a
        finished chunk looking unfinished. Under the lock a writer waits instead.
        """
        try:
            with self._workflow_write_lock(table_name):
                return self._set_chunk_status_watched(
                    table_name, chunk_index, status, status_payload
                )
        except TimeoutError as e:
            celery_logger.error(f"{e}; chunk {chunk_index} not set to {status}.")
            return False

    def _set_chunk_status_watched(
        self,
        table_name: str,
        chunk_index: int,
        status: str,
        status_payload: Optional[dict] = None,
    ):
        if status_payload is None:
            status_payload = {}

        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        workflow_key = self._get_workflow_key(table_name)

        # Writers of the workflow key hold _workflow_write_lock, so WATCH should not
        # fire; it stays as a guard against any writer that does not take the lock.
        max_retries = 10
        for attempt in range(max_retries):
            try:
                with REDIS_CLIENT.pipeline() as pipe:
                    pipe.watch(workflow_key)

                    workflow_data = self.get_workflow_data(table_name)
                    if not workflow_data:
                        celery_logger.error(
                            f"Workflow data not found for {table_name} when setting chunk status."
                        )
                        return False

                    old_status_bytes = REDIS_CLIENT.hget(
                        chunk_statuses_key, str(chunk_index)
                    )
                    old_status = (
                        old_status_bytes.decode("utf-8") if old_status_bytes else None
                    )

                    # COMPLETED is final within a run (initialize_workflow resets the
                    # statuses for a new one). process_chunk records PROCESSING_SUBTASKS
                    # after dispatching its sub-batches, so a small chunk's finalize can
                    # mark it COMPLETED first; overwriting that left chunk 2541 of ltv7
                    # final2 "in progress" until stale recovery re-ran it 10 min later.
                    if (
                        old_status == CHUNK_STATUS_COMPLETED
                        and status != CHUNK_STATUS_COMPLETED
                    ):
                        pipe.unwatch()
                        celery_logger.info(
                            f"Chunk {chunk_index} of {table_name} is already COMPLETED; "
                            f"not setting it to {status}."
                        )
                        return False

                    processing_subtasks_ts_key = self._get_processing_subtasks_timestamps_key(table_name)
                    processing_ts_key = self._get_processing_timestamps_key(table_name)
                    pipe.multi()

                    pipe.hset(chunk_statuses_key, str(chunk_index), status)
                    pipe.expire(chunk_statuses_key, self.expiry_time)

                    # Track when chunks enter PROCESSING for stale-chunk recovery
                    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
                    if status == CHUNK_STATUS_PROCESSING:
                        pipe.hset(processing_ts_key, str(chunk_index), now_iso)
                        pipe.expire(processing_ts_key, self.expiry_time)
                    else:
                        # Chunk left PROCESSING — remove its timestamp entry
                        pipe.hdel(processing_ts_key, str(chunk_index))

                    # Track when chunks enter PROCESSING_SUBTASKS for stale-chunk recovery
                    if status == CHUNK_STATUS_PROCESSING_SUBTASKS:
                        pipe.hset(
                            processing_subtasks_ts_key,
                            str(chunk_index),
                            now_iso,
                        )
                        pipe.expire(processing_subtasks_ts_key, self.expiry_time)
                    else:
                        # Chunk left PROCESSING_SUBTASKS — remove its timestamp entry
                        pipe.hdel(processing_subtasks_ts_key, str(chunk_index))

                    current_time_iso = datetime.datetime.now(
                        datetime.timezone.utc
                    ).isoformat()

                    updated_workflow_fields = {"updated_at": current_time_iso}

                    if status == CHUNK_STATUS_COMPLETED:
                        if old_status != CHUNK_STATUS_COMPLETED:
                            updated_workflow_fields["completed_chunks"] = (
                                workflow_data.completed_chunks + 1
                            )
                            if "rows_processed" in status_payload:
                                updated_workflow_fields["rows_processed"] = (
                                    workflow_data.rows_processed
                                    + status_payload["rows_processed"]
                                )

                        if (
                            "chunk_bounding_box" in status_payload
                            and status_payload["chunk_bounding_box"]
                        ):
                            updated_workflow_fields["latest_completed_chunk_info"] = (
                                asdict(
                                    ChunkInfo(
                                        min_corner=status_payload[
                                            "chunk_bounding_box"
                                        ].get("min_corner"),
                                        max_corner=status_payload[
                                            "chunk_bounding_box"
                                        ].get("max_corner"),
                                        index=chunk_index,
                                    )
                                )
                            )

                        current_rows_processed = updated_workflow_fields.get(
                            "rows_processed", workflow_data.rows_processed
                        )
                        updated_workflow_fields.update(
                            _progress_estimate(
                                workflow_data, current_rows_processed, current_time_iso
                            )
                        )

                    elif status in [
                        CHUNK_STATUS_FAILED_RETRYABLE,
                        CHUNK_STATUS_FAILED_PERMANENT,
                    ]:
                        failure_details = {
                            "error_message": status_payload.get(
                                "error_message", "Unknown error"
                            ),
                            "attempt_count": status_payload.get("attempt_count", 1),
                            "timestamp": current_time_iso,
                            "status": status,
                        }
                        pipe.hset(
                            chunk_failed_details_key,
                            str(chunk_index),
                            json.dumps(failure_details),
                        )
                        pipe.expire(chunk_failed_details_key, self.expiry_time)

                        updated_workflow_fields["last_error"] = (
                            f"Chunk {chunk_index}: {status_payload.get('error_message', 'Failed')}"
                        )
                        updated_workflow_fields["last_failure_time"] = current_time_iso

                        if status == CHUNK_STATUS_FAILED_RETRYABLE:
                            pipe.sadd(
                                self._get_retryable_chunks_set_key(table_name),
                                str(chunk_index),
                            )
                            pipe.expire(
                                self._get_retryable_chunks_set_key(table_name),
                                self.expiry_time,
                            )
                        elif (
                            old_status == CHUNK_STATUS_FAILED_RETRYABLE
                            and status != CHUNK_STATUS_FAILED_RETRYABLE
                        ):
                            pipe.srem(
                                self._get_retryable_chunks_set_key(table_name),
                                str(chunk_index),
                            )

                    if old_status == CHUNK_STATUS_FAILED_RETRYABLE and status in [
                        CHUNK_STATUS_COMPLETED,
                        CHUNK_STATUS_FAILED_PERMANENT,
                    ]:
                        pipe.srem(
                            self._get_retryable_chunks_set_key(table_name),
                            str(chunk_index),
                        )

                    temp_workflow_dict = asdict(workflow_data)
                    for key, value in updated_workflow_fields.items():
                        temp_workflow_dict[key] = value

                    if (
                        "latest_completed_chunk_info" in temp_workflow_dict
                        and isinstance(
                            temp_workflow_dict["latest_completed_chunk_info"], ChunkInfo
                        )
                    ):
                        temp_workflow_dict["latest_completed_chunk_info"] = asdict(
                            temp_workflow_dict["latest_completed_chunk_info"]
                        )
                    elif (
                        "latest_completed_chunk_info" in temp_workflow_dict
                        and not isinstance(
                            temp_workflow_dict["latest_completed_chunk_info"], dict
                        )
                        and temp_workflow_dict["latest_completed_chunk_info"]
                        is not None
                    ):
                        if (
                            status == CHUNK_STATUS_COMPLETED
                            and "chunk_bounding_box" in status_payload
                            and status_payload["chunk_bounding_box"]
                        ):
                            temp_workflow_dict["latest_completed_chunk_info"] = asdict(
                                ChunkInfo(
                                    min_corner=status_payload["chunk_bounding_box"].get(
                                        "min_corner"
                                    ),
                                    max_corner=status_payload["chunk_bounding_box"].get(
                                        "max_corner"
                                    ),
                                    index=chunk_index,
                                )
                            )
                        else:
                            temp_workflow_dict["latest_completed_chunk_info"] = None

                    pipe.set(
                        workflow_key,
                        json.dumps(temp_workflow_dict),
                        ex=self.expiry_time,
                    )
                    pipe.execute()
                    celery_logger.info(
                        f"Set status for chunk {chunk_index} of {table_name} to {status}. Updated workflow."
                    )
                    return True

            except redis.WatchError:
                celery_logger.warning(
                    f"WatchError for {workflow_key} setting chunk {chunk_index} status (attempt {attempt+1}/{max_retries}). Retrying..."
                )
                if attempt == max_retries - 1:
                    celery_logger.error(
                        f"Failed to set chunk status for {chunk_index} after {max_retries} retries due to WatchError."
                    )
                    return False
                time.sleep(random.uniform(0.1, 0.5) * min(attempt + 1, 4))
            except Exception as e:
                celery_logger.error(
                    f"Error setting chunk status for {table_name}, chunk {chunk_index}: {str(e)}"
                )
                return False
        return False

    def get_chunk_status(self, table_name: str, chunk_index: int) -> Optional[str]:
        """Retrieves status from chunk_statuses_key. Returns None or PENDING if not found."""
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        try:
            status = REDIS_CLIENT.hget(chunk_statuses_key, str(chunk_index))
            if status:
                return status.decode("utf-8")
            return CHUNK_STATUS_PENDING
        except Exception as e:
            celery_logger.error(
                f"Error getting chunk status for {table_name}, chunk {chunk_index}: {str(e)}"
            )
            return None

    def get_chunks_to_process(
        self,
        table_name: str,
        total_chunks: int,
        batch_size: int = 100,
        prioritize_failed_chunks: bool = True,
    ) -> Tuple[List[int], Optional[Any], Optional[int]]:
        """
        Gets a batch of chunk indices to process.
        Returns a tuple: (list_of_chunk_indices, None (failed_cursor deprecated), new_pending_scan_cursor).
        The cursors should be passed back in subsequent calls to continue scanning.
        """
        chunks_to_process = []

        workflow_data = self.get_workflow_data(table_name)
        if not workflow_data:
            celery_logger.error(
                f"Cannot get chunks to process, workflow_data not found for {table_name}"
            )
            return [], 0, 0

        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)

        new_pending_cursor = workflow_data.current_pending_scan_cursor

        if prioritize_failed_chunks:
            num_retryable_to_fetch = batch_size - len(chunks_to_process)
            if num_retryable_to_fetch > 0:
                retry_chunk_indices_bytes = REDIS_CLIENT.spop(
                    retryable_set_key, num_retryable_to_fetch
                )
                if retry_chunk_indices_bytes:
                    if not isinstance(retry_chunk_indices_bytes, list):
                        retry_chunk_indices_bytes = [retry_chunk_indices_bytes]

                    for chunk_idx_bytes in retry_chunk_indices_bytes:
                        if chunk_idx_bytes is None:
                            continue
                        chunk_idx = int(chunk_idx_bytes.decode("utf-8"))

                        chunks_to_process.append(chunk_idx)
                        if len(chunks_to_process) >= batch_size:
                            break
                    celery_logger.info(
                        f"Popped {len(retry_chunk_indices_bytes)} chunks from FAILED_RETRYABLE set for {table_name}."
                    )

        if len(chunks_to_process) < batch_size and new_pending_cursor < total_chunks:
            max_to_check_pending = batch_size - len(chunks_to_process) + 200

            candidate_indices = list(
                range(
                    new_pending_cursor,
                    min(new_pending_cursor + max_to_check_pending, total_chunks),
                )
            )

            if candidate_indices:
                statuses_found = REDIS_CLIENT.hmget(
                    chunk_statuses_key, [str(idx) for idx in candidate_indices]
                )

                current_scan_idx_for_pending = new_pending_cursor
                for i, chunk_idx_to_check in enumerate(candidate_indices):
                    if len(chunks_to_process) >= batch_size:
                        break

                    status_val = statuses_found[i]
                    if (
                        status_val is None
                        or status_val.decode("utf-8") == CHUNK_STATUS_PENDING
                    ):
                        chunks_to_process.append(chunk_idx_to_check)
                    current_scan_idx_for_pending = chunk_idx_to_check + 1

                new_pending_cursor = current_scan_idx_for_pending

        return chunks_to_process, None, new_pending_cursor

    def recover_stale_processing_subtasks(
        self, table_name: str, stale_threshold_seconds: int = 600
    ) -> int:
        """
        Scans chunks in PROCESSING_SUBTASKS and marks any that have been there
        longer than stale_threshold_seconds as FAILED_RETRYABLE so the dispatcher
        can re-dispatch them.

        Also recovers chunks in PROCESSING_SUBTASKS that have no timestamp entry
        (e.g. from before timestamp tracking was deployed, or from a failed write)
        — these are treated as immediately stale since their age is unknown.

        Returns the number of chunks recovered.
        """
        ts_key = self._get_processing_subtasks_timestamps_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        recovered = 0
        try:
            all_timestamps = REDIS_CLIENT.hgetall(ts_key)
            all_statuses = REDIS_CLIENT.hgetall(chunk_statuses_key)

            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = now - datetime.timedelta(seconds=stale_threshold_seconds)

            # First pass: recover chunks in PROCESSING_SUBTASKS with no timestamp entry.
            # These have unknown age (e.g. entered before timestamp tracking was deployed)
            # and should be treated as immediately stale.
            for chunk_idx_bytes, status_bytes in all_statuses.items():
                if status_bytes.decode("utf-8") != CHUNK_STATUS_PROCESSING_SUBTASKS:
                    continue
                if chunk_idx_bytes in all_timestamps:
                    continue  # Has a timestamp — handled in second pass
                chunk_idx_str = chunk_idx_bytes.decode("utf-8")
                try:
                    celery_logger.warning(
                        f"Chunk {chunk_idx_str} for '{table_name}' is in PROCESSING_SUBTASKS "
                        f"with no timestamp recorded (unknown age). Marking FAILED_RETRYABLE."
                    )
                    REDIS_CLIENT.hset(chunk_statuses_key, chunk_idx_str, CHUNK_STATUS_FAILED_RETRYABLE)
                    REDIS_CLIENT.sadd(retryable_set_key, chunk_idx_str)
                    recovered += 1
                except Exception as e_inner:
                    celery_logger.error(
                        f"Error recovering timestamp-less chunk {chunk_idx_str} for {table_name}: {e_inner}"
                    )

            # Second pass: recover chunks whose timestamp has exceeded the stale threshold.
            for chunk_idx_bytes, ts_bytes in all_timestamps.items():
                chunk_idx_str = chunk_idx_bytes.decode("utf-8")
                try:
                    entered_at = datetime.datetime.fromisoformat(ts_bytes.decode("utf-8"))
                    if entered_at > cutoff:
                        continue  # Still within the grace period

                    # Verify the chunk is still in PROCESSING_SUBTASKS (not already resolved)
                    current_status_bytes = REDIS_CLIENT.hget(chunk_statuses_key, chunk_idx_str)
                    if current_status_bytes is None:
                        REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                        continue
                    current_status = current_status_bytes.decode("utf-8")
                    if current_status != CHUNK_STATUS_PROCESSING_SUBTASKS:
                        REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                        continue

                    age_seconds = (now - entered_at).total_seconds()
                    celery_logger.warning(
                        f"Chunk {chunk_idx_str} for '{table_name}' has been in "
                        f"PROCESSING_SUBTASKS for {age_seconds:.0f}s (threshold {stale_threshold_seconds}s). "
                        f"Marking FAILED_RETRYABLE for re-dispatch."
                    )
                    REDIS_CLIENT.hset(chunk_statuses_key, chunk_idx_str, CHUNK_STATUS_FAILED_RETRYABLE)
                    REDIS_CLIENT.sadd(retryable_set_key, chunk_idx_str)
                    REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                    recovered += 1
                except Exception as e_inner:
                    celery_logger.error(
                        f"Error processing stale-chunk entry {chunk_idx_str} for {table_name}: {e_inner}"
                    )
        except Exception as e:
            celery_logger.error(
                f"Error in recover_stale_processing_subtasks for {table_name}: {e}"
            )
        return recovered

    def recover_stale_processing_chunks(
        self, table_name: str, stale_threshold_seconds: int = 600
    ) -> int:
        """
        Scans chunks in PROCESSING state and marks any that have been there
        longer than stale_threshold_seconds as FAILED_RETRYABLE so the dispatcher
        can re-dispatch them.

        Also recovers chunks in PROCESSING that have no timestamp entry (e.g. from
        before timestamp tracking was deployed) — treated as immediately stale.

        Returns the number of chunks recovered.
        """
        ts_key = self._get_processing_timestamps_key(table_name)
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        recovered = 0
        try:
            all_timestamps = REDIS_CLIENT.hgetall(ts_key)
            all_statuses = REDIS_CLIENT.hgetall(chunk_statuses_key)

            now = datetime.datetime.now(datetime.timezone.utc)
            cutoff = now - datetime.timedelta(seconds=stale_threshold_seconds)

            # First pass: recover PROCESSING chunks with no timestamp (unknown age → stale).
            for chunk_idx_bytes, status_bytes in all_statuses.items():
                if status_bytes.decode("utf-8") != CHUNK_STATUS_PROCESSING:
                    continue
                if chunk_idx_bytes in all_timestamps:
                    continue
                chunk_idx_str = chunk_idx_bytes.decode("utf-8")
                try:
                    celery_logger.warning(
                        f"Chunk {chunk_idx_str} for '{table_name}' is in PROCESSING "
                        f"with no timestamp recorded (unknown age). Marking FAILED_RETRYABLE."
                    )
                    REDIS_CLIENT.hset(chunk_statuses_key, chunk_idx_str, CHUNK_STATUS_FAILED_RETRYABLE)
                    REDIS_CLIENT.sadd(retryable_set_key, chunk_idx_str)
                    recovered += 1
                except Exception as e_inner:
                    celery_logger.error(
                        f"Error recovering no-timestamp PROCESSING chunk {chunk_idx_str} for {table_name}: {e_inner}"
                    )

            # Second pass: recover PROCESSING chunks whose timestamp is past the cutoff.
            for chunk_idx_bytes, ts_bytes in all_timestamps.items():
                chunk_idx_str = chunk_idx_bytes.decode("utf-8")
                try:
                    entered_at = datetime.datetime.fromisoformat(ts_bytes.decode("utf-8"))
                    if entered_at > cutoff:
                        continue

                    current_status_bytes = REDIS_CLIENT.hget(chunk_statuses_key, chunk_idx_str)
                    if current_status_bytes is None:
                        REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                        continue
                    current_status = current_status_bytes.decode("utf-8")
                    if current_status != CHUNK_STATUS_PROCESSING:
                        REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                        continue

                    age_seconds = (now - entered_at).total_seconds()
                    celery_logger.warning(
                        f"Chunk {chunk_idx_str} for '{table_name}' has been in "
                        f"PROCESSING for {age_seconds:.0f}s (threshold {stale_threshold_seconds}s). "
                        f"Marking FAILED_RETRYABLE for re-dispatch."
                    )
                    REDIS_CLIENT.hset(chunk_statuses_key, chunk_idx_str, CHUNK_STATUS_FAILED_RETRYABLE)
                    REDIS_CLIENT.sadd(retryable_set_key, chunk_idx_str)
                    REDIS_CLIENT.hdel(ts_key, chunk_idx_str)
                    recovered += 1
                except Exception as e_inner:
                    celery_logger.error(
                        f"Error processing stale-processing entry {chunk_idx_str} for {table_name}: {e_inner}"
                    )
        except Exception as e:
            celery_logger.error(
                f"Error in recover_stale_processing_chunks for {table_name}: {e}"
            )
        return recovered

    def set_dispatch_params(self, table_name: str, params: dict) -> None:
        """Store the parameters needed to re-dispatch process_table_in_chunks for recovery."""
        key = self._get_dispatch_params_key(table_name)
        try:
            REDIS_CLIENT.set(key, json.dumps(params), ex=self.expiry_time)
        except Exception as e:
            celery_logger.error(f"Error storing dispatch params for {table_name}: {e}")

    def get_dispatch_params(self, table_name: str) -> Optional[dict]:
        """Retrieve stored dispatch parameters for process_table_in_chunks."""
        key = self._get_dispatch_params_key(table_name)
        try:
            data = REDIS_CLIENT.get(key)
            if data:
                return json.loads(data)
        except Exception as e:
            celery_logger.error(f"Error retrieving dispatch params for {table_name}: {e}")
        return None

    def get_all_chunk_statuses(self, table_name: str) -> Optional[Dict[str, str]]:
        """Gets all chunk statuses for a table."""
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        try:
            statuses_bytes = REDIS_CLIENT.hgetall(chunk_statuses_key)
            return {
                k.decode("utf-8"): v.decode("utf-8") for k, v in statuses_bytes.items()
            }
        except Exception as e:
            celery_logger.error(
                f"Error getting all chunk statuses for {table_name}: {str(e)}"
            )
            return None

    def get_failed_chunk_details(
        self, table_name: str, chunk_index: int
    ) -> Optional[dict]:
        """Gets the failure details for a specific chunk."""
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        try:
            details_json = REDIS_CLIENT.hget(chunk_failed_details_key, str(chunk_index))
            if details_json:
                return json.loads(details_json.decode("utf-8"))
            return None
        except Exception as e:
            celery_logger.error(
                f"Error getting failed chunk details for {table_name}, chunk {chunk_index}: {str(e)}"
            )
            return None

    def get_all_failed_chunk_details(
        self, table_name: str
    ) -> Optional[Dict[str, dict]]:
        """Gets all failed chunk details for a table."""
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        try:
            details_bytes = REDIS_CLIENT.hgetall(chunk_failed_details_key)
            return {
                k.decode("utf-8"): json.loads(v.decode("utf-8"))
                for k, v in details_bytes.items()
            }
        except Exception as e:
            celery_logger.error(
                f"Error getting all failed chunk details for {table_name}: {str(e)}"
            )
            return None

    def get_active_workflows(self) -> List[Dict[str, Any]]:
        """Scans for all workflow keys for the current database and returns those not in a terminal state."""
        active_workflows = []

        pattern = f"{self.workflow_prefix}*"

        cursor = "0"
        while cursor != 0:
            cursor, keys = REDIS_CLIENT.scan(cursor=cursor, match=pattern, count=100)
            for key_bytes in keys:
                key_str = key_bytes.decode("utf-8")

                table_name_part = key_str[len(self.workflow_prefix) :]
                if ":" in table_name_part:
                    continue

                try:
                    workflow_data = self.get_workflow_data(table_name_part)
                    if workflow_data:
                        terminal_statuses = ["completed", "failed", "ERROR"]
                        if workflow_data.status.lower() not in [
                            s.lower() for s in terminal_statuses
                        ]:
                            wf_dict = asdict(workflow_data)
                            if wf_dict.get(
                                "latest_completed_chunk_info"
                            ) and not isinstance(
                                wf_dict["latest_completed_chunk_info"], dict
                            ):
                                wf_dict["latest_completed_chunk_info"] = asdict(
                                    wf_dict["latest_completed_chunk_info"]
                                )
                            elif wf_dict.get("latest_completed_chunk_info") is None:
                                wf_dict["latest_completed_chunk_info"] = None

                            active_workflows.append(wf_dict)
                except Exception as e:
                    celery_logger.error(
                        f"Error processing workflow key {key_str}: {str(e)}"
                    )
        return active_workflows
