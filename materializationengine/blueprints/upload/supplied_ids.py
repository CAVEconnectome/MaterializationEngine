"""Supervoxel and root ids that an upload's source already has.

A source may carry <field>_supervoxel_id and <field>_root_id columns for its bound
spatial points. They go straight into the segmentation table (a second processed CSV,
imported like the annotation table), and the upload skips the lookups they make
unnecessary:

- a field with supplied supervoxels needs no spatial lookup: its root ids are looked
  up from those supervoxels, chunk by chunk over id ranges of the table;
- a field with supplied root ids has every one checked against the chunkedgraph at the
  job's materialization timestamp (is_latest_roots, one read per distinct root), and
  only the stale ones are looked up again.

Every root is checked, whatever time the user says the roots were looked up at: the
table is published as correct at the job's timestamp (segmentation_table_metadata's
last_updated), and the live update only fixes roots that expire after that, so a root
already stale then would stay wrong. The time the user gives (root_ids_lookup) is kept
with the job, and the pre-upload sample check uses it.
"""

import datetime
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from materializationengine.blueprints.upload.processor import (
    ROOT_SUFFIX,
    SUPERVOXEL_SUFFIX,
    SchemaProcessor,
    exact_ids,
)


def maps_segmentation_columns(column_mapping: Optional[Dict[str, str]]) -> bool:
    """Whether a column mapping gives a column for any supervoxel or root id field."""
    return any(
        value and (key.endswith(SUPERVOXEL_SUFFIX) or key.endswith(ROOT_SUFFIX))
        for key, value in (column_mapping or {}).items()
    )


def processor_for(file_metadata: Dict[str, Any]) -> SchemaProcessor:
    meta = file_metadata["metadata"]
    return SchemaProcessor(
        meta["schema_type"],
        reference_table=meta.get("reference_table") or None,
        column_mapping=file_metadata.get("column_mapping") or {},
        ignored_columns=file_metadata.get("ignored_columns") or [],
    )


def supplied_segmentation(file_metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """What the job's params record about supplied ids, or None when there are none.

    columns: the segmentation table's columns, in the order of its processed CSV.
    supplied: the columns the source supplies.
    fields: {point field: {"supervoxel_id": column, "root_id": column}}.
    lookup_fields: the fields the spatial lookup still does (no supplied supervoxels).
    resolve_fields: the fields resolve_roots checks (some id supplied).
    """
    processor = processor_for(file_metadata)
    if not processor.writes_segmentation:
        return None
    supplied = processor.supplied_segmentation_columns
    fields = {
        field: {"supervoxel_id": sv_col, "root_id": root_col}
        for field, (sv_col, root_col) in processor.segmentation_columns.items()
    }
    return {
        "columns": processor.segmentation_column_order,
        "supplied": supplied,
        "fields": fields,
        "lookup_fields": processor.lookup_fields,
        "resolve_fields": [
            field
            for field, cols in fields.items()
            if cols["supervoxel_id"] in supplied or cols["root_id"] in supplied
        ],
        "root_ids_lookup": file_metadata["metadata"].get("root_ids_lookup"),
    }


def lookup_time(root_ids_lookup: Optional[Dict[str, Any]]) -> Optional[datetime.datetime]:
    """The time the supplied roots should be correct at, by the user's account: the
    time given, the end of the range given, or None (not known)."""
    lookup = root_ids_lookup or {}
    value = lookup.get("at") if lookup.get("mode") == "at" else (
        lookup.get("end") if lookup.get("mode") == "between" else None
    )
    if not value:
        return None
    ts = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    return ts if ts.tzinfo else ts.replace(tzinfo=datetime.timezone.utc)


def sample_check(rows: pd.DataFrame, file_metadata: Dict[str, Any], chunkedgraph) -> Optional[Dict[str, Any]]:
    """Check the supplied ids of a few rows against the chunkedgraph (a caveclient
    ChunkedGraphClient), before the upload starts.

    Per point field: whether the supplied supervoxels have roots; whether the supplied
    roots are the roots of the supplied supervoxels at the user's time (or now, when
    not known); whether the roots are still current now. A mismatch nearly always
    means a column was mapped to the wrong field. None when no ids are supplied.
    """
    processor = processor_for(file_metadata)
    if not processor.writes_segmentation:
        return None
    supplied = set(processor.supplied_segmentation_columns)
    at = lookup_time(file_metadata["metadata"].get("root_ids_lookup"))
    fields = []
    for field, (sv_col, root_col) in processor.segmentation_columns.items():
        has_sv, has_root = sv_col in supplied, root_col in supplied
        if not (has_sv or has_root):
            continue
        result: Dict[str, Any] = {"field": field, "rows": len(rows)}
        try:
            svs = exact_ids(rows[processor.column_mapping[sv_col]], sv_col) if has_sv else None
            roots = exact_ids(rows[processor.column_mapping[root_col]], root_col) if has_root else None
            if has_sv:
                given = svs > 0
                looked = (
                    np.asarray(chunkedgraph.get_roots(svs[given], timestamp=at), dtype=np.int64)
                    if given.any()
                    else np.array([], dtype=np.int64)
                )
                result["supervoxels"] = int(given.sum())
                result["supervoxels_with_root"] = int(np.count_nonzero(looked))
                if has_root:
                    pair = roots[given] > 0
                    result["roots_compared"] = int(pair.sum())
                    result["roots_matching"] = int((looked[pair] == roots[given][pair]).sum())
            if has_root:
                current = roots[roots > 0]
                result["roots"] = int(len(current))
                result["roots_current_now"] = (
                    int(np.asarray(chunkedgraph.is_latest_roots(current)).sum()) if len(current) else 0
                )
        except Exception as e:  # a wrong column gives ids the chunkedgraph refuses
            result["error"] = f"{type(e).__name__}: {e}"
        fields.append(result)
    return {"time_stamp": at.isoformat() if at else None, "fields": fields}
