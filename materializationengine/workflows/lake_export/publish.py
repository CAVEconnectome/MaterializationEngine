"""Publishing a staged range: its files added to the lake in one commit.

A range is written into a staging table inside the lake (ranges.export_range); publishing
reads that table's add actions -- each file's size, partition values and min/max/null
statistics -- and commits them to the lake in one transaction, their paths prefixed with
the staging directory. Readers then see the whole range or none of it. The commit is
tagged with the export job and the range (custom commit metadata), so a job that restarts
can tell which ranges each lake already has (published_ranges) and publish only the rest.

A lake must have one publisher at a time: the export's driver publishes, one thread per
lake. Once committed, the staging table's _delta_log is deleted; the lake owns the files.
A staging table that is never published (a failed attempt) holds files no commit refers
to; the lake's final vacuum removes them.
"""

import json
import math
from typing import Dict, List, Optional, Sequence, Set, Tuple

from materializationengine.workflows.lake_export import storage

JOB_KEY = "export_job"
RANGE_KEY = "export_range"


def _stat_value(v):
    if isinstance(v, float) and not math.isfinite(v):
        return None
    if isinstance(v, (bytes, bytearray)):
        return None  # binary min/max are not representable in JSON statistics
    return v.isoformat() if hasattr(v, "isoformat") else v


def stats_json(action: Dict) -> str:
    """A Delta add action's statistics, from get_add_actions(flatten=True)'s columns."""

    def section(prefix):
        out = {}
        for k, v in action.items():
            if k.startswith(prefix):
                v = _stat_value(v)
                if v is not None:
                    out[k[len(prefix):]] = v
        return out

    return json.dumps({"numRecords": action["num_records"], "minValues": section("min."),
                       "maxValues": section("max."), "nullCount": section("null_count.")})


def staged_add_actions(stage_uri: str, prefix: str) -> Tuple[object, list]:
    """The staging table's schema, and its files as add actions with paths under prefix."""
    import pyarrow as pa
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

    staged = DeltaTable(stage_uri)
    actions = []
    for a in pa.table(staged.get_add_actions(flatten=True)).to_pylist():
        mtime = a["modification_time"]
        actions.append(AddAction(
            f"{prefix}/{a['path']}",
            a["size_bytes"],
            {k[len("partition."):]: (None if v is None else str(v)) for k, v in a.items() if k.startswith("partition.")},
            int(mtime.timestamp() * 1000) if hasattr(mtime, "timestamp") else int(mtime),
            True,
            stats_json(a),
        ))
    return staged.schema(), actions


def publish(lake_uri: str, stage: str, partition_by: Optional[Sequence[str]], range_index: int,
            job_id: str) -> int:
    """Add the range staged at lake_uri/stage to the lake in one commit; files added.

    A range with no rows has no staging table and publishes nothing. The caller must be
    the lake's only publisher.
    """
    from deltalake import DeltaTable
    from deltalake.transaction import CommitProperties, create_table_with_add_actions

    stage_uri = f"{lake_uri}/{stage}"
    if not DeltaTable.is_deltatable(stage_uri):
        return 0
    schema, actions = staged_add_actions(stage_uri, stage)
    props = CommitProperties(custom_metadata={JOB_KEY: job_id, RANGE_KEY: str(int(range_index))})
    partition_by = list(partition_by) if partition_by else None
    if DeltaTable.is_deltatable(lake_uri):
        DeltaTable(lake_uri).create_write_transaction(
            actions, mode="append", schema=schema, partition_by=partition_by, commit_properties=props)
    else:
        create_table_with_add_actions(
            lake_uri, schema, actions, mode="append", partition_by=partition_by, commit_properties=props)
    storage.delete_prefix(lake_uri, f"{stage}/_delta_log")
    return len(actions)


def published_ranges(lake_uri: str, job_id: str) -> Set[int]:
    """Ranges this job has already published to the lake (empty if there is no lake)."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(lake_uri):
        return set()
    return {int(h[RANGE_KEY]) for h in DeltaTable(lake_uri).history()
            if h.get(JOB_KEY) == job_id and h.get(RANGE_KEY) is not None}


def export_jobs(lake_uri: str) -> List[str]:
    """Every job that has published a range to the lake, oldest first."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(lake_uri):
        return []
    jobs = []
    for h in reversed(DeltaTable(lake_uri).history()):
        job = h.get(JOB_KEY)
        if job is not None and job not in jobs:
            jobs.append(job)
    return jobs
