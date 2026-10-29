"""Finish the segmentation rows of ids the upload's source supplied, one id range at a time.

The rows were imported from the source (blueprints/upload/supplied_ids.py), so the
segmentation table is in id order and a range of ids is one primary key range scan.
For each point field with a supplied id, in a range:

1. supervoxels left blank in a field whose supervoxels were supplied are looked up at
   the point's position, as the spatial lookup does;
2. supplied root ids are checked at the job's materialization timestamp: one that is
   not a root (wrong layer) or no longer the latest root is stale;
3. root ids that are missing or stale are looked up from the supervoxels, at that same
   timestamp.

Only rows that changed are written back.
"""

import datetime
import logging
from typing import Dict, List

import numpy as np
import pandas as pd
from sqlalchemy import text

logger = logging.getLogger(__name__)

# Supervoxels or roots per chunkedgraph call.
DEFAULT_ROOT_BATCH_SIZE = 10000


def _batches(n: int, size: int):
    for start in range(0, n, size):
        yield slice(start, min(start + size, n))


def current_roots(cg_client, roots: np.ndarray, time_stamp, batch_size: int) -> np.ndarray:
    """Whether each root id (> 0) is a root that is still the latest at time_stamp.

    One read per distinct root. An id at a lower layer, a supervoxel or chunk id put in
    a root column by mistake, is not current.
    """
    roots = np.asarray(roots, dtype=np.uint64)
    unique = np.unique(roots[roots > 0])
    if len(unique) == 0:
        return np.zeros(len(roots), dtype=bool)
    root_ext = cg_client.root_ext
    is_root = root_ext.get_chunk_layers(unique) == root_ext._layer_count
    latest = np.zeros(len(unique), dtype=bool)
    candidates = np.nonzero(is_root)[0]
    for part in _batches(len(candidates), batch_size):
        index = candidates[part]
        latest[index] = root_ext.is_latest_roots(unique[index], time_stamp=time_stamp)
    return np.isin(roots, unique[latest])


def lookup_roots(cg_client, supervoxels: np.ndarray, time_stamp, column: str, batch_size: int) -> np.ndarray:
    from materializationengine.workflows.spatial_lookup import _get_roots_zero_for_missing

    # each distinct supervoxel once (points in one cell often share a supervoxel)
    unique, inverse = np.unique(np.asarray(supervoxels, dtype=np.uint64), return_inverse=True)
    roots = np.zeros(len(unique), dtype=np.uint64)
    for part in _batches(len(unique), batch_size):
        roots[part] = _get_roots_zero_for_missing(cg_client, unique[part], time_stamp, column)
    return roots[inverse.reshape(-1)]


def lookup_supervoxels(engine, info: dict, field: dict, ids: np.ndarray) -> np.ndarray:
    """The supervoxel at each id's point (0 where the point is missing or unlabeled)."""
    from materializationengine.cloudvolume_gateway import cloudvolume_cache
    from materializationengine.workflows.spatial_lookup import lookup_supervoxel_ids

    quote = engine.dialect.identifier_preparer.quote
    position = quote(field["position"])
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT id, ST_X({position}), ST_Y({position}), ST_Z({position}) "
                f"FROM {quote(info['annotation_table'])} "
                f"WHERE id = ANY(:ids) AND {position} IS NOT NULL ORDER BY id"
            ),
            {"ids": [int(i) for i in ids]},
        ).fetchall()
    found = np.zeros(len(ids), dtype=np.int64)
    if not rows:
        return found
    located = np.array([r[0] for r in rows], dtype=np.int64)
    points = np.array([r[1:] for r in rows], dtype=float)
    cv = cloudvolume_cache.get_cv(info["segmentation_source"])
    labels = lookup_supervoxel_ids(cv, points, info["coord_resolution"]).astype(np.int64)
    order = np.argsort(ids, kind="stable")
    where = order[np.searchsorted(ids, located, sorter=order)]
    found[where] = labels
    return found


def _write(engine, table: str, ids: np.ndarray, values: Dict[str, np.ndarray]) -> None:
    quote = engine.dialect.identifier_preparer.quote
    names = list(values)
    params = {"ids": [int(i) for i in ids]}
    arrays = ["CAST(:ids AS bigint[])"]
    for i, name in enumerate(names):
        params[f"c{i}"] = [int(v) for v in values[name]]
        arrays.append(f"CAST(:c{i} AS bigint[])")
    columns = ", ".join(quote(n) for n in names)
    sets = ", ".join(f"{quote(n)} = v.{quote(n)}" for n in names)
    with engine.begin() as conn:
        conn.execute(
            text(
                f"UPDATE {quote(table)} AS t SET {sets} "
                f"FROM unnest({', '.join(arrays)}) AS v(id, {columns}) WHERE t.id = v.id"
            ),
            params,
        )


def resolve_id_range(
    database: str,
    info: dict,
    first_id: int,
    last_id: int,
    batch_size: int = DEFAULT_ROOT_BATCH_SIZE,
) -> Dict[str, int]:
    """Do one id range [first_id, last_id]; returns what it counted.

    info: segmentation_table, annotation_table, pcg_table_name, segmentation_source,
    coord_resolution, materialization_time_stamp (ISO), and fields: {point field:
    {supervoxel_id, root_id, position (columns), supervoxels_supplied, roots_supplied}}.
    """
    from materializationengine.database import db_manager
    from materializationengine.kvdb_gateway import kvdb_cache

    engine = db_manager.get_engine(database)
    quote = engine.dialect.identifier_preparer.quote
    fields = info["fields"]
    columns: List[str] = [c for f in fields.values() for c in (f["supervoxel_id"], f["root_id"])]
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                f"SELECT id, {', '.join(quote(c) for c in columns)} "
                f"FROM {quote(info['segmentation_table'])} "
                f"WHERE id BETWEEN :first AND :last ORDER BY id"
            ),
            {"first": int(first_id), "last": int(last_id)},
        ).fetchall()
    counts = {
        "rows": len(rows),
        "rows_updated": 0,
        "supervoxels_looked_up": 0,
        "roots_checked": 0,
        "roots_stale": 0,
        "roots_looked_up": 0,
    }
    if not rows:
        return counts
    data = pd.DataFrame(rows, columns=["id", *columns]).fillna(0).astype(np.int64)
    ids = data["id"].to_numpy()
    time_stamp = datetime.datetime.fromisoformat(info["materialization_time_stamp"])
    cg_client = kvdb_cache.get_client(info["pcg_table_name"])

    changed = np.zeros(len(ids), dtype=bool)
    updates: Dict[str, np.ndarray] = {}
    for field in fields.values():
        sv_col, root_col = field["supervoxel_id"], field["root_id"]
        supervoxels = data[sv_col].to_numpy().copy()
        roots = data[root_col].to_numpy().copy()
        if field.get("supervoxels_supplied"):
            missing = supervoxels == 0
            if missing.any():
                supervoxels[missing] = lookup_supervoxels(engine, info, field, ids[missing])
                counts["supervoxels_looked_up"] += int(np.count_nonzero(supervoxels[missing]))
        if field.get("roots_supplied"):
            supplied = roots > 0
            stale = supplied & ~current_roots(cg_client, roots, time_stamp, batch_size)
            counts["roots_checked"] += int(supplied.sum())
            counts["roots_stale"] += int(stale.sum())
            roots[stale] = 0
        needed = (roots == 0) & (supervoxels > 0)
        if needed.any():
            roots[needed] = lookup_roots(
                cg_client, supervoxels[needed], time_stamp, sv_col, batch_size
            ).astype(np.int64)
            counts["roots_looked_up"] += int(needed.sum())
        field_changed = (supervoxels != data[sv_col].to_numpy()) | (roots != data[root_col].to_numpy())
        if field_changed.any():
            updates[sv_col] = supervoxels
            updates[root_col] = roots
            changed |= field_changed
    if changed.any():
        _write(engine, info["segmentation_table"], ids[changed], {c: v[changed] for c, v in updates.items()})
        counts["rows_updated"] = int(changed.sum())
    return counts
