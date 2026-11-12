"""The export's deployment settings, from app config, in one place."""

import dataclasses
import math
import re
from dataclasses import dataclass, field
from typing import Mapping, Optional

from materializationengine.workflows.lake_export.sizing import GiB, SizingConfig

WORKER_CPUS = 2
WORKER_MEMORY = "11Gi"  # the spike's 2-stream workers peaked at 6.2-6.9 GiB


def _int(config: Mapping, key: str, default: Optional[int]) -> Optional[int]:
    value = config.get(key)
    return default if value in (None, "") else int(value)


def memory_bytes(quantity: str) -> int:
    """Bytes in a Kubernetes memory quantity ("11Gi", "8000Mi", "12G", "1073741824")."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]i?)?\s*", str(quantity))
    if not match:
        raise ValueError(f"not a memory quantity: {quantity!r}")
    number, unit = float(match.group(1)), match.group(2) or ""
    scale = {"": 1, "K": 10**3, "M": 10**6, "G": 10**9, "T": 10**12,
             "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}[unit]
    return int(number * scale)


@dataclass(frozen=True)
class ExportSettings:
    output_bucket: str = ""
    sizing: SizingConfig = field(default_factory=SizingConfig)
    max_streams: int = 4  # Postgres streams at once across the job (DELTALAKE_EXPORT_MAX_STREAMS)
    max_tasks: int = 64  # Ray tasks the driver keeps in flight (ranges count too)
    range_retries: int = 3
    max_row_group_size: Optional[int] = None
    target_partition_size_mb: int = 256
    small_table_threshold_mb: int = 200
    optimize_target_size: Optional[int] = None
    optimize_max_spill_size: Optional[int] = None
    worker_cpus: int = WORKER_CPUS
    worker_memory: str = WORKER_MEMORY
    max_workers: Optional[int] = None  # default: enough for max_streams

    @classmethod
    def from_config(cls, config: Mapping) -> "ExportSettings":
        worker_cpus = _int(config, "DELTALAKE_RAY_WORKER_CPUS", WORKER_CPUS)
        worker_memory = config.get("DELTALAKE_RAY_WORKER_MEMORY") or WORKER_MEMORY
        sizing = SizingConfig.from_config(config)
        # a worker runs one range per CPU, and its buffers share its memory
        if not config.get("DELTALAKE_EXPORT_STREAMS_PER_WORKER"):
            sizing = dataclasses.replace(sizing, streams_per_worker=worker_cpus)
        if not config.get("DELTALAKE_EXPORT_WORKER_MEMORY_BUDGET_BYTES"):
            sizing = dataclasses.replace(sizing, worker_memory_budget=max(GiB, memory_bytes(worker_memory) - 2 * GiB))
        return cls(
            output_bucket=(config.get("DELTALAKE_OUTPUT_BUCKET") or "").rstrip("/"),
            sizing=sizing,
            max_streams=_int(config, "DELTALAKE_EXPORT_MAX_STREAMS", cls.max_streams),
            max_tasks=_int(config, "DELTALAKE_EXPORT_MAX_TASKS", cls.max_tasks),
            range_retries=_int(config, "DELTALAKE_EXPORT_RANGE_RETRIES", cls.range_retries),
            max_row_group_size=_int(config, "DELTALAKE_MAX_ROW_GROUP_SIZE", None),
            target_partition_size_mb=_int(config, "DELTALAKE_TARGET_PARTITION_SIZE_MB", cls.target_partition_size_mb),
            small_table_threshold_mb=_int(config, "DELTALAKE_SMALL_TABLE_THRESHOLD_MB", cls.small_table_threshold_mb),
            optimize_target_size=_int(config, "DELTALAKE_OPTIMIZE_TARGET_SIZE_BYTES", None),
            optimize_max_spill_size=_int(config, "DELTALAKE_OPTIMIZE_MAX_SPILL_SIZE_BYTES", None),
            worker_cpus=worker_cpus,
            worker_memory=worker_memory,
            max_workers=_int(config, "DELTALAKE_RAY_MAX_WORKERS", None),
        )

    def workers_for(self, max_streams: Optional[int] = None) -> int:
        """Workers for a job: DELTALAKE_RAY_MAX_WORKERS, else enough to run its streams."""
        if self.max_workers:
            return self.max_workers
        return max(1, math.ceil((max_streams or self.max_streams) / max(1, self.worker_cpus)))
