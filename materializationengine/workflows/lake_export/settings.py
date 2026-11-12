"""The export's deployment settings, from app config, in one place."""

from dataclasses import dataclass, field
from typing import Mapping, Optional

from materializationengine.workflows.lake_export.sizing import SizingConfig


def _int(config: Mapping, key: str, default: Optional[int]) -> Optional[int]:
    value = config.get(key)
    return default if value in (None, "") else int(value)


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

    @classmethod
    def from_config(cls, config: Mapping) -> "ExportSettings":
        return cls(
            output_bucket=(config.get("DELTALAKE_OUTPUT_BUCKET") or "").rstrip("/"),
            sizing=SizingConfig.from_config(config),
            max_streams=_int(config, "DELTALAKE_EXPORT_MAX_STREAMS", cls.max_streams),
            max_tasks=_int(config, "DELTALAKE_EXPORT_MAX_TASKS", cls.max_tasks),
            range_retries=_int(config, "DELTALAKE_EXPORT_RANGE_RETRIES", cls.range_retries),
            max_row_group_size=_int(config, "DELTALAKE_MAX_ROW_GROUP_SIZE", None),
            target_partition_size_mb=_int(config, "DELTALAKE_TARGET_PARTITION_SIZE_MB", cls.target_partition_size_mb),
            small_table_threshold_mb=_int(config, "DELTALAKE_SMALL_TABLE_THRESHOLD_MB", cls.small_table_threshold_mb),
            optimize_target_size=_int(config, "DELTALAKE_OPTIMIZE_TARGET_SIZE_BYTES", None),
            optimize_max_spill_size=_int(config, "DELTALAKE_OPTIMIZE_MAX_SPILL_SIZE_BYTES", None),
        )
