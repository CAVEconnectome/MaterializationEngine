"""How a table's export is divided, from its size.

A range costs about the same whatever its size: a connection, a staging table per lake, a
commit per lake when it is published (~1.2 s on minnie), at least one file per partition
per flush in every lake its ids don't line up with, and an optimize task per partition
group afterwards. Splitting pays only when ranges are large, so a table is split only
past DELTALAKE_EXPORT_SPLIT_MIN_BYTES, into ranges of about DELTALAKE_EXPORT_RANGE_BYTES;
smaller tables keep one range -- one stream, as the export always had.

A stream's buffer is sized so a range flushes once when it fits the worker's memory
budget (each flush writes into every partition of a lake the range's ids don't line up
with, so flushes multiply files). Measured on minnie: a range-task process holds ~0.5 GiB
before data, and a stream peaks near 5x its buffer -- one filling, one waiting for the
writer, ~3x while one is written (DELTA_LAKE_PARALLEL_EXPORT.md, Scaling with table size).
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

MiB = 2**20
GiB = 2**30


@dataclass(frozen=True)
class SizingConfig:
    range_bytes: int = GiB
    split_min_bytes: int = 2 * GiB
    streams_per_worker: int = 2
    worker_memory_budget: int = 9 * GiB
    process_base_bytes: int = GiB // 2
    stream_peak_factor: float = 5.0
    min_flush_bytes: int = 64 * MiB
    # Optimize: parquet bytes (as the Delta log records them) per partition-group task,
    # and the size under which a lake is optimized whole by one task.
    optimize_group_bytes: int = 256 * MiB
    whole_lake_max_bytes: int = 256 * MiB

    @classmethod
    def from_config(cls, config: Dict) -> "SizingConfig":
        """From app config keys DELTALAKE_EXPORT_*; missing or empty keys keep the defaults."""
        keys = {
            "range_bytes": "DELTALAKE_EXPORT_RANGE_BYTES",
            "split_min_bytes": "DELTALAKE_EXPORT_SPLIT_MIN_BYTES",
            "streams_per_worker": "DELTALAKE_EXPORT_STREAMS_PER_WORKER",
            "worker_memory_budget": "DELTALAKE_EXPORT_WORKER_MEMORY_BUDGET_BYTES",
            "optimize_group_bytes": "DELTALAKE_EXPORT_OPTIMIZE_GROUP_BYTES",
            "whole_lake_max_bytes": "DELTALAKE_EXPORT_WHOLE_LAKE_MAX_BYTES",
        }
        return cls(**{field: int(config[key]) for field, key in keys.items() if config.get(key)})


@dataclass(frozen=True)
class TableSizing:
    estimated_bytes: int
    ranges: int
    flush_bytes: int

    @property
    def split(self) -> bool:
        return self.ranges > 1


def _round_down(n: float, unit: int) -> int:
    return int(n // unit * unit)


def _round_up(n: float, unit: int) -> int:
    return int(math.ceil(n / unit) * unit)


def max_flush_bytes(cfg: SizingConfig) -> int:
    """The largest buffer that keeps a worker's streams within its memory budget."""
    streams = cfg.streams_per_worker
    per_stream = (cfg.worker_memory_budget - streams * cfg.process_base_bytes) / (streams * cfg.stream_peak_factor)
    return max(cfg.min_flush_bytes, _round_down(per_stream, 64 * MiB))


def size_table(row_count: int, bytes_per_row: int, cfg: SizingConfig = SizingConfig()) -> TableSizing:
    """Ranges and buffer for a table of row_count rows of about bytes_per_row each.

    bytes_per_row is estimate_bytes_per_row's (Postgres's heap bytes per row), a
    conservative stand-in for Arrow's: minnie's synapses are ~147 Arrow bytes per row.
    """
    estimated = max(0, int(row_count or 0)) * max(1, int(bytes_per_row or 1))
    ranges = 1 if estimated < cfg.split_min_bytes else math.ceil(estimated / cfg.range_bytes)
    # One flush per range when it fits: a quarter's headroom for ranges' uneven sizes.
    fits = _round_up(estimated / ranges * 1.25, 64 * MiB)
    flush = max(cfg.min_flush_bytes, min(max_flush_bytes(cfg), fits))
    return TableSizing(estimated_bytes=estimated, ranges=ranges, flush_bytes=flush)


@dataclass(frozen=True)
class PartitionStat:
    """One partition of a lake: its value as the Delta log records it (None: the null
    partition), and its files and bytes."""

    value: Optional[str]
    files: int
    bytes: int


@dataclass(frozen=True)
class OptimizeTask:
    """Partitions one task optimizes in one commit; values None means the whole lake."""

    values: Optional[tuple]
    files: int
    bytes: int


def plan_optimize(stats: Sequence[PartitionStat], partitioned: bool,
                  cfg: SizingConfig = SizingConfig()) -> List[OptimizeTask]:
    """A lake's optimize as tasks, largest first.

    An unpartitioned or small lake is one task. Otherwise partitions are packed into
    groups of up to optimize_group_bytes (first fit, largest partitions first; a partition
    larger than that is a task alone), and the null partition, which needs its own filter,
    is always a task alone. Largest first, so the long tasks start while there is other
    work to run beside them.
    """
    files = sum(s.files for s in stats)
    total = sum(s.bytes for s in stats)
    if not stats:
        return []
    if not partitioned or total <= cfg.whole_lake_max_bytes:
        return [OptimizeTask(values=None, files=files, bytes=total)]

    tasks = [OptimizeTask(values=(None,), files=s.files, bytes=s.bytes) for s in stats if s.value is None]
    groups: List[List[PartitionStat]] = []
    for s in sorted((s for s in stats if s.value is not None), key=lambda s: -s.bytes):
        for group in groups:
            if sum(g.bytes for g in group) + s.bytes <= cfg.optimize_group_bytes:
                group.append(s)
                break
        else:
            groups.append([s])
    tasks += [OptimizeTask(values=tuple(g.value for g in group), files=sum(g.files for g in group),
                           bytes=sum(g.bytes for g in group)) for group in groups]
    return sorted(tasks, key=lambda t: -t.bytes)
