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
    status: str = "initializing"

    total_chunks: int = 0
    completed_chunks: int = 0
    submitted_chunks: int = 0
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

    last_processed_chunk: Optional[ChunkInfo] = None
    mat_info_idx: int = 0
    chunking_strategy: str = "grid"
    used_chunk_size: int = 1024

    last_error: Optional[str] = None
    last_failed_chunk_index: Optional[int] = None
    last_failure_time: Optional[str] = None

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
        self.workflow_prefix = f"workflow:{database}:"
        self.expiry_time = 86400 * 7  # 7 days for all Redis keys

    def get_bbox_hash(self, bbox: Union[np.ndarray, List]) -> str:
        """Generate hash for bounding box."""
        bbox_list = bbox.tolist() if isinstance(bbox, np.ndarray) else bbox
        return hashlib.md5(json.dumps(bbox_list).encode()).hexdigest()

    def get_workflow_data(self, table_name: str) -> Optional[WorkflowData]:
        """Get the complete workflow data for a table."""
        key = f"{self.workflow_prefix}{table_name}"
        workflow_json = REDIS_CLIENT.get(key)

        if workflow_json:
            try:
                data_dict = json.loads(workflow_json)

                last_chunk = data_dict.get("last_processed_chunk")
                if last_chunk:
                    data_dict["last_processed_chunk"] = ChunkInfo(**last_chunk)

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

    def initialize_workflow(self, table_name: str, task_id: str) -> WorkflowData:
        """Initialize or reset workflow data."""
        key = f"{self.workflow_prefix}{table_name}"

        workflow_data = WorkflowData(table_name=table_name, task_id=task_id)

        REDIS_CLIENT.set(key, json.dumps(asdict(workflow_data)), ex=self.expiry_time)
        return workflow_data

    def update_workflow(
        self, table_name: str, min_enclosing_bbox: Optional[np.ndarray] = None, **kwargs
        ) -> bool:
        """Update workflow data."""
        key = f"{self.workflow_prefix}{table_name}"

        workflow_data = self.get_workflow_data(table_name)
        if not workflow_data:
            workflow_data = WorkflowData(table_name=table_name, task_id="unknown")

        if (
            "last_processed_chunk" in kwargs
            and kwargs["last_processed_chunk"] is not None
        ):
            last_chunk = kwargs["last_processed_chunk"]
            workflow_data.last_processed_chunk = ChunkInfo(
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
                index=kwargs.get("last_chunk_index", 0),
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
                and field_name not in ["last_processed_chunk", "increment_completed"]
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

    def increment_completed(
        self, table_name: str, rows_processed: int = 0, 
        last_processed_chunk: Dict = None, last_chunk_index: int = None
    ) -> bool:
        """Increment completed chunks counter and update metrics atomically."""
        key = f"{self.workflow_prefix}{table_name}"

        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                workflow_json = REDIS_CLIENT.get(key)
                if not workflow_json:
                    return False

                data_dict = json.loads(workflow_json)

                if last_processed_chunk is not None:
                    min_corner = last_processed_chunk.get("min_corner")
                    max_corner = last_processed_chunk.get("max_corner")
                    index = last_processed_chunk.get("index", last_chunk_index)
                    
                    data_dict["last_processed_chunk"] = {
                        "min_corner": min_corner,
                        "max_corner": max_corner,
                        "index": index,
                        "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                    }

                last_chunk = data_dict.get("last_processed_chunk")
                if last_chunk:
                    data_dict["last_processed_chunk"] = ChunkInfo(**last_chunk)

                workflow_data = WorkflowData(**data_dict)

                workflow_data.completed_chunks += 1
                workflow_data.rows_processed += rows_processed
                workflow_data.updated_at = datetime.datetime.now(
                    datetime.timezone.utc
                ).isoformat()

                if workflow_data.start_time:
                    start_time = datetime.datetime.fromisoformat(
                        workflow_data.start_time
                    )
                    now = datetime.datetime.now(datetime.timezone.utc)
                    elapsed_seconds = (now - start_time).total_seconds()

                    if elapsed_seconds > 0:
                        rows_per_second = workflow_data.rows_processed / elapsed_seconds
                        workflow_data.processing_rate = (
                            f"{rows_per_second * 60:.2f} rows/minute"
                        )

                        if workflow_data.total_row_estimate:
                            remaining_rows = (
                                workflow_data.total_row_estimate
                                - workflow_data.rows_processed
                            )
                            if remaining_rows > 0 and rows_per_second > 0:
                                seconds_left = remaining_rows / rows_per_second
                                estimated_completion = now + datetime.timedelta(
                                    seconds=seconds_left
                                )
                                workflow_data.estimated_completion = (
                                    estimated_completion.isoformat()
                                )

                REDIS_CLIENT.set(
                    key, json.dumps(asdict(workflow_data)), ex=self.expiry_time
                )

                percentage = workflow_data.progress
                celery_logger.info(
                    f"Updated workflow for {table_name}: "
                    f"{workflow_data.completed_chunks}/{workflow_data.total_chunks} "
                    f"({percentage:.1f}%)"
                )

                return True

            except (redis.WatchError, json.JSONDecodeError, TypeError) as e:
                retry_count += 1
                celery_logger.warning(
                    f"Redis error when updating metrics (attempt {retry_count}/{max_retries}): {str(e)}"
                )
                time.sleep(random.uniform(0.1, 0.5) * retry_count)

            except Exception as e:
                retry_count += 1
                error_type = type(e).__name__
                celery_logger.error(
                    f"Error incrementing metrics (attempt {retry_count}/{max_retries}): {error_type} - {str(e)}"
                )
                if retry_count >= max_retries:
                    return False
                time.sleep(0.5)

        return False
    
    def get_active_workflows(self) -> List[Dict[str, Any]]:
        """Get a list of all active workflow data."""
        active_workflows = []

        keys = REDIS_CLIENT.keys(f"{self.workflow_prefix}*")

        for key in keys:
            workflow_json = REDIS_CLIENT.get(key)
            if workflow_json:
                try:
                    data_dict = json.loads(workflow_json)
                    table_name = key.decode("utf-8").replace(self.workflow_prefix, "")
                    data_dict["table_name"] = table_name
                    active_workflows.append(data_dict)
                except json.JSONDecodeError:
                    continue

        return active_workflows

    def get_checkpoint_state(self, table_name: str) -> Tuple[int, int]:
        """Get current batch and chunk indices."""
        workflow_data = self.get_workflow_data(table_name)
        if workflow_data:
            return (
                workflow_data.mat_info_idx,
                (
                    workflow_data.last_processed_chunk.index
                    if workflow_data.last_processed_chunk
                    else 0
                ),
            )
        return (0, 0)
    
    def record_chunk_failure(self, table_name: str, chunk_index: int, error: str = None) -> bool:
        """Record a chunk failure."""
        try:
            error_msg = f"Chunk {chunk_index} failed: {error}" if error else f"Chunk {chunk_index} failed"
            self.update_workflow(
                table_name=table_name,
                last_error=error_msg,
                last_failed_chunk_index=chunk_index,
                last_failure_time=datetime.datetime.now(datetime.timezone.utc).isoformat()
            )
            return True
        except Exception as e:
            celery_logger.error(f"Error recording chunk failure: {str(e)}")
            return False
