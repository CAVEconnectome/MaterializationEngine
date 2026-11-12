"""Optimizing a lake a few partitions per task.

Each task z-orders (or compacts) its partitions with the export's writer settings -- bloom
filters and row-group size, which only optimize applies -- and commits them in its own
transaction; delta-rs commits concurrent optimizes of disjoint partitions without
conflict (about 80 at once per lake on GCS in the spike). Tasks of one lake can therefore
run on every worker at once, where optimize_deltalake works through a whole lake's
partitions in one process.

A task then deletes the files its commit replaced: it knows them (its partitions' files
before, minus the lake's files after), so the deletes are spread over every task instead
of one vacuum listing the whole lake at the end. Nothing reads a lake while it is being
exported (the catalog lists it only once complete), so no reader can still need them.
sweep() runs the export's full vacuum afterwards, for files no commit refers to: failed
range attempts' staging, or a task that died between its commit and its deletes.
"""

import time
from collections import defaultdict
from typing import Dict, List, Optional, Sequence

from materializationengine.workflows.lake_export import storage
from materializationengine.workflows.lake_export.sizing import PartitionStat


def writer_properties(spec):
    """The export's optimize writer settings for a spec (as optimize_deltalake builds them)."""
    from deltalake.writer import BloomFilterProperties, ColumnProperties, WriterProperties

    properties: Dict = {}
    if spec.bloom_filter_columns:
        bloom = BloomFilterProperties(set_bloom_filter_enabled=True, fpp=spec.bloom_filter_fpp or 0.001)
        properties["column_properties"] = {c: ColumnProperties(bloom_filter_properties=bloom)
                                           for c in spec.bloom_filter_columns}
    if spec.max_row_group_size:
        properties["max_row_group_size"] = int(spec.max_row_group_size)
    return WriterProperties(**properties) if properties else None


def _files(dt, partition_column: Optional[str]) -> List[Dict]:
    """The lake's files: path, bytes, and partition value (str, or None for null)."""
    import pyarrow as pa

    key = f"partition.{partition_column}" if partition_column else None
    files = []
    for a in pa.table(dt.get_add_actions(flatten=True)).to_pylist():
        value = a.get(key) if key else None
        files.append({"path": a["path"], "bytes": a["size_bytes"], "value": None if value is None else str(value)})
    return files


def partition_stats(lake_uri: str, partition_column: Optional[str]) -> List[PartitionStat]:
    """Files and bytes per partition, from the Delta log (one stat for an unpartitioned lake)."""
    from deltalake import DeltaTable

    per: Dict = defaultdict(lambda: [0, 0])
    for f in _files(DeltaTable(lake_uri), partition_column):
        per[f["value"]][0] += 1
        per[f["value"]][1] += f["bytes"]
    return [PartitionStat(value=v, files=n, bytes=b) for v, (n, b) in per.items()]


def _filters(partition_column: Optional[str], values: Optional[Sequence]):
    if values is None:
        return None
    if list(values) == [None]:
        # delta-rs cannot encode None in a filter; it matches the null partition with "".
        return [(partition_column, "=", "")]
    if None in values:
        raise ValueError("the null partition must be optimized in a task of its own")
    return [(partition_column, "in", [str(v) for v in values])]


def optimize_partitions(
    lake_uri: str,
    spec,
    partition_column: Optional[str],
    values: Optional[Sequence],
    max_concurrent_tasks: int = 1,
    target_size: Optional[int] = None,
    max_spill_size: Optional[int] = None,
) -> Dict:
    """Optimize the given partitions (values None: the whole lake) in one commit, then
    delete the files it replaced. Re-running it is safe; files a run left behind (it died
    between its commit and its deletes) are no longer referenced, and sweep() removes them."""
    from deltalake import DeltaTable

    started = time.monotonic()
    wanted = None if values is None else {None if v is None else str(v) for v in values}
    before = {f["path"] for f in _files(DeltaTable(lake_uri), partition_column)
              if wanted is None or f["value"] in wanted}
    options = {"partition_filters": _filters(partition_column, values), "writer_properties": writer_properties(spec),
               "max_concurrent_tasks": max_concurrent_tasks}
    if target_size:
        options["target_size"] = int(target_size)
    if max_spill_size:
        options["max_spill_size"] = int(max_spill_size)
    optimizer = DeltaTable(lake_uri).optimize
    if spec.zorder_columns:
        metrics = optimizer.z_order(spec.zorder_columns, **options)
    else:
        metrics = optimizer.compact(**options)
    after = {f["path"] for f in _files(DeltaTable(lake_uri), partition_column)}
    replaced = before - after
    storage.delete_paths(lake_uri, replaced)
    return {"partitions": None if values is None else len(values), "files_removed": metrics["numFilesRemoved"],
            "files_added": metrics["numFilesAdded"], "deleted": len(replaced),
            "seconds": time.monotonic() - started}


def sweep(lake_uri: str) -> None:
    """The export's vacuum: delete every file the lake's log no longer refers to,
    including staging that was never published."""
    from deltalake import DeltaTable

    DeltaTable(lake_uri).vacuum(dry_run=False, retention_hours=0, enforce_retention_duration=False, full=True)
