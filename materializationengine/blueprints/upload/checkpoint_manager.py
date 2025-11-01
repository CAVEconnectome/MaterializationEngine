import datetime
import hashlib
import json
import random
import time
from dataclasses import asdict, dataclass, field, fields
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import redis
from celery.utils.log import get_task_logger

from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)


REDIS_CLIENT = redis.StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=1,
)

CHUNK_STATUS_PENDING = "PENDING"
CHUNK_STATUS_PROCESSING = "PROCESSING"
CHUNK_STATUS_PROCESSING_SUBTASKS = "PROCESSING_SUBTASKS"
CHUNK_STATUS_COMPLETED = "COMPLETED"
CHUNK_STATUS_FAILED_RETRYABLE = "FAILED_RETRYABLE"
CHUNK_STATUS_FAILED_PERMANENT = "FAILED_PERMANENT"
CHUNK_STATUS_ERROR = "ERROR"


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

    @property
    def progress(self) -> float:
        """Calculate progress percentage."""
        if self.total_chunks <= 0:
            return 0.0
        return (self.completed_chunks / self.total_chunks) * 100


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

    def get_bbox_hash(self, bbox: Union[np.ndarray, List]) -> str:
        """Generate hash for bounding box."""
        bbox_list = bbox.tolist() if isinstance(bbox, np.ndarray) else bbox
        return hashlib.md5(json.dumps(bbox_list).encode()).hexdigest()

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
        """Update workflow data."""
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
        """Deletes chunk_statuses, chunk_failed_details, and failed_retryable_set keys for the table."""
        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        try:
            REDIS_CLIENT.delete(
                chunk_statuses_key, chunk_failed_details_key, retryable_set_key
            )
            celery_logger.info(
                f"Reset chunk statuses, details, and retryable set for table: {table_name}"
            )
        except Exception as e:
            celery_logger.error(
                f"Error resetting chunk data for {table_name}: {str(e)}"
            )

    def set_chunk_status(
        self,
        table_name: str,
        chunk_index: int,
        status: str,
        status_payload: Optional[dict] = None,
    ):
        """
        Sets the status of a chunk and updates workflow aggregates.
        """
        if status_payload is None:
            status_payload = {}

        chunk_statuses_key = self._get_chunk_statuses_key(table_name)
        chunk_failed_details_key = self._get_chunk_failed_details_key(table_name)
        retryable_set_key = self._get_retryable_chunks_set_key(table_name)
        workflow_key = self._get_workflow_key(table_name)

        max_retries = 3
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

                    pipe.multi()

                    pipe.hset(chunk_statuses_key, str(chunk_index), status)
                    pipe.expire(chunk_statuses_key, self.expiry_time)

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

                        if workflow_data.start_time:
                            start_dt = datetime.datetime.fromisoformat(
                                workflow_data.start_time
                            )
                            now_dt = datetime.datetime.fromisoformat(current_time_iso)
                            elapsed_seconds = (now_dt - start_dt).total_seconds()

                            if elapsed_seconds > 0:
                                current_rows_processed = updated_workflow_fields.get(
                                    "rows_processed", workflow_data.rows_processed
                                )
                                if "rows_processed" in status_payload:
                                    current_rows_processed = (
                                        workflow_data.rows_processed
                                        + status_payload["rows_processed"]
                                    )
                                else:
                                    current_rows_processed = (
                                        workflow_data.rows_processed
                                    )

                                rows_per_second = (
                                    current_rows_processed / elapsed_seconds
                                )
                                updated_workflow_fields["processing_rate"] = (
                                    f"{rows_per_second * 60:.2f} rows/minute"
                                )

                                if (
                                    workflow_data.total_row_estimate
                                    and workflow_data.total_row_estimate > 0
                                ):
                                    remaining_rows = (
                                        workflow_data.total_row_estimate
                                        - current_rows_processed
                                    )
                                    if remaining_rows > 0 and rows_per_second > 0:
                                        seconds_left = remaining_rows / rows_per_second
                                        estimated_completion_dt = (
                                            now_dt
                                            + datetime.timedelta(seconds=seconds_left)
                                        )
                                        updated_workflow_fields[
                                            "estimated_completion"
                                        ] = estimated_completion_dt.isoformat()
                                    elif remaining_rows <= 0:
                                        updated_workflow_fields[
                                            "estimated_completion"
                                        ] = current_time_iso

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
                time.sleep(random.uniform(0.1, 0.5) * (attempt + 1))
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
