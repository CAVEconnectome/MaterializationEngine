"""A record of the root ID updates each run of update_root_ids makes: one append-only Delta
table per annotation table, plus a small _manifest table listing every run.

Off unless ROOT_ID_UPDATE_LOG is set, for everything or for chosen datastacks and tables
(see enabled).

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/{datastack}/{annotation_table}/   the Delta table
        _delta_log/                          one commit per run with updates
        run_id=20261009T173330.002540Z/      that run's updates (SCHEMA), a few Parquet files
        _manifest/                           Delta table, one row per run (MANIFEST_SCHEMA),
                                             including runs that changed nothing and runs that
                                             failed, so the windows chain; Delta readers skip it

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/_staging/{datastack}/{annotation_table}/{run_id}/
        part-{root_column}-{task_id}.parquet one per get_new_root_ids task while the run is in
        _run.json                            progress; outside the table so mirrors never see them

run_id is the run's materialization timestamp (20261009T173330.002540Z), the one timestamp
every lookup in the run used, shared by every table updated in that run. It is the partition
(not stored in the data files), fixed-width so it sorts like the timestamp. Every row also
carries its run's window, start_time_stamp (the previous run's lookup time) and end_time_stamp
(this run's); repeated within a file they cost almost nothing (about 0.1% of the data), and the
log's per-file min/max of them let any Delta reader skip whole runs without the manifest.

Each run's rows are sorted by (root_column, id), with id delta-encoded and row groups of
ROW_GROUP_ROWS, so a reader fetches only the row groups and columns a query needs. Moving a
snapshot taken at V to the roots at a later run's end T:

    pl.scan_delta(uri)
      .filter((pl.col("end_time_stamp") > V) & (pl.col("end_time_stamp") <= T))
      .select("id", "root_column", "new_root_id", "end_time_stamp")
      .sort("end_time_stamp").group_by("id", "root_column").last()   # then join onto the snapshot

(to go back from V to an earlier T: T < end_time_stamp <= V, the first row per id, old_root_id).
Roots are known only at run ends: a T inside a run's window (start_time_stamp < T <
end_time_stamp) cannot be answered from the log, and a run whose start is not the previous
run's end is a gap.

The table is append-only (delta.appendOnly, enforced by the writer), so it can be mirrored and
kept up to date by fetching only what is new:

    gcloud storage rsync -r gs://.../root_id_updates/{datastack}/{table} ./{table}
        # files are never rewritten: each sync copies only new files (manifest included)
    DeltaTable("./{table}").to_pandas(), pl.read_delta(...), duckdb delta_scan(...)
    manifest(uri)    # the runs, e.g. filtered on end_time_stamp, with their table_version

The Delta log keeps Delta's default retention (30 days): the manifest, not the log, is the
record of the runs, so only time travel and added_files(after_version) are limited to that.

Never OPTIMIZE, z-order, vacuum or overwrite these tables: each run is deduplicated and
written as a few large files before its single commit.

Writing is best effort: failures are logged and never stop the root ID update.
"""

import datetime
import json
import re
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from celery.utils.log import get_task_logger
from pyarrow import fs

from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)

# The update columns, as get_new_root_ids writes them to the staging chunk files.
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("root_column", pa.string()),
        ("supervoxel_id", pa.int64()),
        ("old_root_id", pa.int64()),
        ("new_root_id", pa.int64()),
    ]
)
TS = pa.timestamp("us", tz="UTC")
# The table's columns: the updates, their run's window, and run_id (the partition, not stored
# in the data files).
TABLE_SCHEMA = pa.schema(
    list(SCHEMA) + [("start_time_stamp", TS), ("end_time_stamp", TS), ("run_id", pa.string())]
)
PARTITION = "run_id"
# Each run's rows, in this order: queries by id read few row groups, and id delta-encodes well.
SORT_KEYS = [("root_column", "ascending"), ("id", "ascending")]
ROW_GROUP_ROWS = 16384
TABLE_PROPERTIES = {"delta.appendOnly": "true"}
MANIFEST_DIR = "_manifest"
# One row per finished run, written after its data commit (if any).
MANIFEST_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("start_time_stamp", TS),  # last_updated_time_stamp: the window start
        ("end_time_stamp", TS),  # materialization_time_stamp: the window end, the lookups' time
        ("recorded_at", TS),
        ("rows", pa.int64()),
        ("chunk_files", pa.int64()),
        ("duplicate_rows_removed", pa.int64()),  # null when not counted (a streamed run)
        ("table_version", pa.int64()),  # the run's data commit; null if it changed nothing
        ("succeeded", pa.bool_()),  # whether its root update workflow completed; null if unknown
        ("lookup_all_root_ids", pa.bool_()),
        ("find_all_expired_roots", pa.bool_()),
        ("datastack", pa.string()),
        ("annotation_table", pa.string()),
        ("segmentation_table", pa.string()),
        ("pcg_table", pa.string()),
    ]
)
TARGET_FILE_BYTES = 256 * 1024 * 1024
# zstd: about 18% smaller than write_deltalake's default (snappy) on these columns, for the
# same read speed; higher levels gained nothing more. Set explicitly because delta-rs's
# defaults differ by operation (optimize wrote zstd, append writes snappy).
COMPRESSION = {"compression": "ZSTD", "compression_level": 3}


def _writer_properties(updates=False):
    """zstd for every file; the update table's files also get small row groups and a
    delta-encoded id (sorted ids encode to about a third of their plain size)."""
    from deltalake import ColumnProperties, WriterProperties

    if not updates:
        return WriterProperties(**COMPRESSION)
    return WriterProperties(
        **COMPRESSION,
        max_row_group_size=ROW_GROUP_ROWS,
        column_properties={"id": ColumnProperties(encoding="DELTA_BINARY_PACKED", dictionary_enabled=False)},
    )


# Above this many chunk files (a dense lookup), finalize streams them in groups of this size
# into the one commit instead of reading the whole run into memory; duplicates are then
# removed, and rows sorted, within each group only (since the expired-root dedup, duplicates
# come only from retries).
STREAM_GROUP_FILES = 5000
RUN_FILE = "_run.json"
# The commit metadata each run carries (all strings; read back with DeltaTable.history()).
COMMIT_KEYS = (
    "run_id", "start_time_stamp", "end_time_stamp", "rows", "chunk_files", "duplicate_rows_removed",
    "lookup_all_root_ids", "find_all_expired_roots", "datastack", "annotation_table",
    "segmentation_table", "pcg_table",
)
# part-<root_column>-<celery task id>.parquet, as write_chunk names them
_CHUNK_FILE = re.compile(r"^part-(?P<root_column>[A-Za-z0-9_]+)-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\.parquet$")
_RUN_ID = re.compile(r"^\d{8}T\d{6}\.\d{6}Z$")


def enabled(mat_metadata: dict) -> bool:
    """Whether to record this table's updates.

    ROOT_ID_UPDATE_LOG is either a bool (every table of every datastack), or a mapping of
    datastack to the annotation tables to record, "*" meaning all of that datastack's:

        ROOT_ID_UPDATE_LOG = {"minnie65_phase3_v1": ["synapses_pni_2"], "zheng_ca3": "*"}

    From the environment it may be "true"/"false" or that mapping as JSON.
    """
    value = get_config_param("ROOT_ID_UPDATE_LOG", False)
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("{"):
            try:
                value = json.loads(text)
            except ValueError:
                celery_logger.warning(f"ROOT_ID_UPDATE_LOG is not valid JSON: {text!r}")
                return False
        else:
            return text.lower() in ("1", "true", "yes")
    if not isinstance(value, dict):
        return bool(value)
    tables = value.get(mat_metadata.get("datastack"))
    if tables in ("*", ["*"]):
        return True
    if isinstance(tables, str):
        tables = [tables]
    return mat_metadata.get("annotation_table_name") in (tables or [])


def run_id(materialization_time_stamp: str) -> str:
    """20261007T150102.123456Z for '2026-10-07 15:01:02.123456' (or its ISO form)."""
    ts = datetime.datetime.fromisoformat(str(materialization_time_stamp))
    return ts.strftime("%Y%m%dT%H%M%S.%fZ")


def _base(mat_metadata: dict) -> Optional[str]:
    bucket = get_config_param("MATERIALIZATION_DUMP_BUCKET")
    if not bucket:
        return None
    return f"{str(bucket).rstrip('/')}/root_id_updates"


def table_uri(mat_metadata: dict) -> Optional[str]:
    """This annotation table's Delta table of root ID updates, or None without a dump bucket."""
    base = _base(mat_metadata)
    if base is None:
        return None
    return f"{base}/{mat_metadata['datastack']}/{mat_metadata['annotation_table_name']}"


def staging_uri(mat_metadata: dict) -> Optional[str]:
    """Where this run's chunk files collect until finalize commits them, or None."""
    base = _base(mat_metadata)
    if base is None:
        return None
    return "/".join(
        [base, "_staging", mat_metadata["datastack"], mat_metadata["annotation_table_name"],
         run_id(mat_metadata["materialization_time_stamp"])]
    )


def _utc(value) -> Optional[datetime.datetime]:
    if not value:
        return None
    ts = value if isinstance(value, datetime.datetime) else datetime.datetime.fromisoformat(str(value))
    return ts.replace(tzinfo=datetime.timezone.utc) if ts.tzinfo is None else ts.astimezone(datetime.timezone.utc)


def updates_frame(root_ids_df, old_roots, root_id_col: str, supervoxel_col: str) -> pa.Table:
    """Rows for one chunk: root_ids_df holds the new roots in root_id_col, old_roots the
    values that column had before the lookup."""
    n = len(root_ids_df)

    def ints(values):
        return pa.array([None if v is None else int(v) for v in values], type=pa.int64())

    return pa.table(
        {
            "id": ints(root_ids_df["id"]),
            "root_column": pa.array([root_id_col.rsplit("_", 2)[0]] * n, type=pa.string()),
            "supervoxel_id": ints(root_ids_df[supervoxel_col]),
            "old_root_id": ints(old_roots),
            "new_root_id": ints(root_ids_df[root_id_col]),
        },
        schema=SCHEMA,
    )


def _filesystem(uri: str):
    filesystem, path = fs.FileSystem.from_uri(uri)
    return filesystem, path


def write_chunk(mat_metadata: dict, table: pa.Table, task_id: str) -> Optional[str]:
    try:
        uri = staging_uri(mat_metadata)
        if uri is None or table.num_rows == 0:
            return None
        filesystem, path = _filesystem(uri)
        filesystem.create_dir(path, recursive=True)
        root_column = table.column("root_column")[0].as_py()
        file_path = f"{path}/part-{root_column}-{task_id}.parquet"
        pq.write_table(table, file_path, filesystem=filesystem)
        return file_path
    except Exception as e:
        celery_logger.warning(f"Could not record root ID updates for {mat_metadata.get('annotation_table_name')}: {e}")
        return None


def record_chunk(mat_metadata: dict, root_ids_df, old_roots, root_id_col: str, supervoxel_col: str, task_id: str):
    """Write one get_new_root_ids task's updates (after they are committed to the database)."""
    try:
        table = updates_frame(root_ids_df, old_roots, root_id_col, supervoxel_col)
    except Exception as e:
        celery_logger.warning(f"Could not record root ID updates for {mat_metadata.get('annotation_table_name')}: {e}")
        return None
    return write_chunk(mat_metadata, table, task_id)


def _run_info(mat_metadata: dict) -> dict:
    return {
        "datastack": mat_metadata.get("datastack"),
        "aligned_volume": mat_metadata.get("aligned_volume"),
        "annotation_table": mat_metadata.get("annotation_table_name"),
        "segmentation_table": mat_metadata.get("segmentation_table_name"),
        "pcg_table": mat_metadata.get("pcg_table_name"),
        "materialization_time_stamp": str(mat_metadata.get("materialization_time_stamp")),
        "last_updated_time_stamp": mat_metadata.get("last_updated_time_stamp"),
        "lookup_all_root_ids": bool(mat_metadata.get("lookup_all_root_ids", False)),
        "find_all_expired_roots": bool(mat_metadata.get("find_all_expired_roots", False)),
    }


def _write_run_file(filesystem, path: str, info: dict):
    filesystem.create_dir(path, recursive=True)
    with filesystem.open_output_stream(f"{path}/{RUN_FILE}") as out:
        out.write(json.dumps(info, indent=1).encode())


def start(mat_metadata: dict) -> None:
    """Describe the run in its staging folder before its chunks are written."""
    try:
        uri = staging_uri(mat_metadata)
        if uri is None:
            return
        filesystem, path = _filesystem(uri)
        _write_run_file(filesystem, path, {**_run_info(mat_metadata), "state": "running"})
    except Exception as e:
        celery_logger.warning(f"Could not start the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")


def _app_id(run: str) -> str:
    return f"root_id_update_log:{run}"


def is_committed(table_folder_uri: str, run: str) -> bool:
    """Whether the run is already in the table (each commit carries an app transaction)."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(table_folder_uri):
        return False
    return DeltaTable(table_folder_uri).transaction_version(_app_id(run)) is not None


def _dedupe(rows: pa.Table) -> pa.Table:
    import polars as pl

    return pl.from_arrow(rows).unique(maintain_order=True).to_arrow().cast(SCHEMA)


def _with_run_columns(rows: pa.Table, run: str, start, end) -> pa.Table:
    """The run's rows in table order, with its window and run_id."""
    rows = rows.cast(SCHEMA).sort_by(SORT_KEYS)
    n = rows.num_rows
    for name, value, type_ in (("start_time_stamp", start, TS), ("end_time_stamp", end, TS), ("run_id", run, pa.string())):
        rows = rows.append_column(name, pa.repeat(pa.scalar(value, type=type_), n))
    return rows


def _commit_version(table_folder_uri: str, run: str) -> Optional[int]:
    """The table version of the run's data commit, or None if it has none."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(table_folder_uri):
        return None
    for commit in DeltaTable(table_folder_uri).history():
        if commit.get("run_id") == run:
            return int(commit["version"])
    return None


def append_run(table_folder_uri: str, run: str, rows, info: dict, n_rows: int = None,
               duplicate_rows_removed=0) -> Optional[int]:
    """Commit one run's (already deduplicated) update rows to the table as one commit, unless
    it is already there. *rows* is a pa.Table, or an iterable of them (streamed into the same
    commit; then pass *n_rows*). *info* is the run's _run.json content. Returns the version of
    the run's commit (the existing one if it was already committed)."""
    from deltalake import CommitProperties, Transaction, write_deltalake

    if is_committed(table_folder_uri, run):
        return _commit_version(table_folder_uri, run)
    start = _utc(info.get("last_updated_time_stamp"))
    end = _utc(info.get("materialization_time_stamp"))
    if isinstance(rows, pa.Table):
        n_rows = rows.num_rows
        data = _with_run_columns(rows, run, start, end)
    else:
        parts = (_with_run_columns(t, run, start, end) for t in rows)
        data = pa.RecordBatchReader.from_batches(
            TABLE_SCHEMA, (batch for t in parts for batch in t.to_batches())
        )
    # Also kept as commit metadata (history()); the manifest is the copy meant for queries.
    metadata = {
        "run_id": run,
        "start_time_stamp": start.isoformat() if start else "",
        "end_time_stamp": end.isoformat() if end else "",
        "rows": "" if n_rows is None else str(n_rows),
        "chunk_files": str(info.get("chunk_files", "")),
        "duplicate_rows_removed": "" if duplicate_rows_removed is None else str(duplicate_rows_removed),
        "lookup_all_root_ids": str(bool(info.get("lookup_all_root_ids", False))).lower(),
        "find_all_expired_roots": str(bool(info.get("find_all_expired_roots", False))).lower(),
        "datastack": str(info.get("datastack") or ""),
        "annotation_table": str(info.get("annotation_table") or ""),
        "segmentation_table": str(info.get("segmentation_table") or ""),
        "pcg_table": str(info.get("pcg_table") or ""),
    }
    write_deltalake(
        table_folder_uri,
        data,
        mode="append",
        partition_by=[PARTITION],
        configuration=TABLE_PROPERTIES,
        target_file_size=TARGET_FILE_BYTES,
        writer_properties=_writer_properties(updates=True),
        commit_properties=CommitProperties(
            custom_metadata=metadata, app_transactions=[Transaction(app_id=_app_id(run), version=1)]
        ),
    )
    return _commit_version(table_folder_uri, run)


def _manifest_uri(table_folder_uri: str) -> str:
    return f"{table_folder_uri.rstrip('/')}/{MANIFEST_DIR}"


def manifest(table_folder_uri: str) -> pa.Table:
    """The table's runs (MANIFEST_SCHEMA), oldest first; empty if none are recorded."""
    from deltalake import DeltaTable

    uri = _manifest_uri(table_folder_uri)
    if not DeltaTable.is_deltatable(uri):
        return MANIFEST_SCHEMA.empty_table()
    return DeltaTable(uri).to_pyarrow_table().sort_by("run_id")


def _int(value) -> Optional[int]:
    return None if value in (None, "") else int(value)


def append_manifest_row(table_folder_uri: str, run: str, info: dict, rows: int, table_version: Optional[int],
                        succeeded: Optional[bool], duplicate_rows_removed=None, recorded_at=None) -> bool:
    """List the run in the table's manifest, unless it is already listed. Returns whether a
    row was added."""
    from deltalake import DeltaTable, write_deltalake

    uri = _manifest_uri(table_folder_uri)
    if DeltaTable.is_deltatable(uri):
        if run in DeltaTable(uri).to_pyarrow_table(columns=["run_id"]).column("run_id").to_pylist():
            return False
    row = {
        "run_id": run,
        "start_time_stamp": _utc(info.get("last_updated_time_stamp")),
        "end_time_stamp": _utc(info.get("materialization_time_stamp")),
        "recorded_at": recorded_at or datetime.datetime.now(datetime.timezone.utc),
        "rows": int(rows or 0),
        "chunk_files": _int(info.get("chunk_files")) or 0,
        "duplicate_rows_removed": _int(duplicate_rows_removed),
        "table_version": table_version,
        "succeeded": succeeded,
        "lookup_all_root_ids": bool(info.get("lookup_all_root_ids", False)),
        "find_all_expired_roots": bool(info.get("find_all_expired_roots", False)),
        "datastack": info.get("datastack"),
        "annotation_table": info.get("annotation_table"),
        "segmentation_table": info.get("segmentation_table"),
        "pcg_table": info.get("pcg_table"),
    }
    write_deltalake(uri, pa.Table.from_pylist([row], schema=MANIFEST_SCHEMA), mode="append",
                    configuration=TABLE_PROPERTIES, writer_properties=_writer_properties())
    return True


def _info_from_commit(commit: dict) -> dict:
    return {
        "last_updated_time_stamp": commit.get("start_time_stamp") or None,
        "materialization_time_stamp": commit.get("end_time_stamp") or None,
        "chunk_files": commit.get("chunk_files"),
        "lookup_all_root_ids": commit.get("lookup_all_root_ids") == "true",
        "find_all_expired_roots": commit.get("find_all_expired_roots") == "true",
        **{k: commit.get(k) or None for k in ("datastack", "annotation_table", "segmentation_table", "pcg_table")},
    }


def repair_manifest(table_folder_uri: str) -> list:
    """List every data commit that the manifest is missing (e.g. its manifest write failed),
    from the commit's metadata. Returns the run_ids added."""
    added = []
    for commit in runs(table_folder_uri):
        if append_manifest_row(table_folder_uri, commit["run_id"], _info_from_commit(commit),
                               rows=_int(commit.get("rows")), table_version=commit["version"], succeeded=None,
                               duplicate_rows_removed=commit.get("duplicate_rows_removed")):
            added.append(commit["run_id"])
    return added


def _read_files(filesystem, paths) -> pa.Table:
    if not paths:
        return SCHEMA.empty_table()
    return pa.concat_tables([pq.read_table(p, filesystem=filesystem).cast(SCHEMA) for p in paths])


def _delete_dir(filesystem, path: str):
    try:
        filesystem.delete_dir(path)
    except (OSError, FileNotFoundError):
        pass


def finalize(mat_metadata: dict, succeeded: Optional[bool] = True) -> None:
    """Once the run is over: commit its deduplicated updates (if any) as one commit, list the
    run in the manifest (also when it changed nothing, or failed), and delete its staging
    folder. Safe to run again (neither the commit nor the manifest row is repeated)."""
    try:
        staging = staging_uri(mat_metadata)
        if staging is None:
            return
        table_folder = table_uri(mat_metadata)
        run = staging.rsplit("/", 1)[1]
        filesystem, path = _filesystem(staging)
        listing = filesystem.get_file_info(fs.FileSelector(path, allow_not_found=True))
        chunks = [f for f in listing if f.type == fs.FileType.File and _CHUNK_FILE.match(f.base_name)]
        info = {**_run_info(mat_metadata), "chunk_files": len(chunks)}
        n_rows, removed, version = 0, 0, _commit_version(table_folder, run)
        if chunks and version is None:
            paths = [f.path for f in chunks]
            if len(paths) <= STREAM_GROUP_FILES:
                rows = _read_files(filesystem, paths)
                unique = _dedupe(rows)
                n_rows, removed = unique.num_rows, rows.num_rows - unique.num_rows
                version = append_run(table_folder, run, unique, info, duplicate_rows_removed=removed)
            else:
                # Row count from the files' footers; duplicates removed per group, not counted.
                n_rows = sum(pq.ParquetFile(p, filesystem=filesystem).metadata.num_rows for p in paths)
                removed = None
                groups = (
                    _dedupe(_read_files(filesystem, paths[i:i + STREAM_GROUP_FILES]))
                    for i in range(0, len(paths), STREAM_GROUP_FILES)
                )
                version = append_run(table_folder, run, groups, info, n_rows=n_rows, duplicate_rows_removed=None)
        elif version is not None:
            commit = next(c for c in runs(table_folder) if c["run_id"] == run)
            n_rows, removed = _int(commit.get("rows")), commit.get("duplicate_rows_removed")
        append_manifest_row(table_folder, run, info, rows=n_rows, table_version=version, succeeded=succeeded,
                            duplicate_rows_removed=removed)
        celery_logger.info(f"Recorded {n_rows} root ID updates of run {run} in {table_folder}")
        _delete_dir(filesystem, path)
    except Exception as e:
        celery_logger.warning(f"Could not finish the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")


def runs(table_folder_uri: str) -> list:
    """Every data commit, oldest first: its commit metadata plus the table version (within the
    log's retention; the manifest keeps every run for good)."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(table_folder_uri):
        return []
    out = []
    for commit in DeltaTable(table_folder_uri).history():
        if commit.get("run_id"):
            out.append({"version": commit["version"], **{k: commit.get(k) for k in COMMIT_KEYS}})
    return sorted(out, key=lambda c: c["version"])


def added_files(table_folder_uri: str, after_version: int = -1) -> list:
    """The data files (relative to the table) added by the commits after *after_version*,
    while that version is still within the log's retention (rsync works regardless)."""
    from deltalake import DeltaTable

    table = DeltaTable(table_folder_uri)
    now = set(table.get_add_actions(flatten=True).column("path").to_pylist())
    if after_version < 0:
        return sorted(now)
    before = set(DeltaTable(table_folder_uri, version=after_version).get_add_actions(flatten=True).column("path").to_pylist())
    return sorted(now - before)


def _info_from_manifest_row(row: dict) -> dict:
    return {
        "last_updated_time_stamp": row["start_time_stamp"],
        "materialization_time_stamp": row["end_time_stamp"],
        "chunk_files": row["chunk_files"],
        **{k: row[k] for k in ("lookup_all_root_ids", "find_all_expired_roots", "datastack",
                               "annotation_table", "segmentation_table", "pcg_table")},
    }


def rebuild_table(source_uri: str, target_uri: str, dry_run: bool = True) -> list:
    """Write a copy of an earlier root ID update table at *target_uri* in the current layout:
    one commit per run, oldest first, its rows sorted and carrying their run's window, and the
    source's manifest rows (runs that changed nothing included) with the new table versions.
    The source may have a manifest or not (the 2026-10-09 tables had none, and an
    end_time_stamp column). It is left untouched. Safe to run again: runs already in the
    target are skipped, so a second run copies only what the source gained meanwhile.
    Returns one report per run."""
    from deltalake import DeltaTable

    source = DeltaTable(source_uri)
    commits = {c["run_id"]: c for c in runs(source_uri)}
    listed = {r["run_id"]: r for r in manifest(source_uri).to_pylist()}
    report = []
    for run in sorted(set(commits) | set(listed)):
        commit, row = commits.get(run), listed.get(run)
        entry = {"run_id": run, "source_version": commit["version"] if commit else None,
                 "rows": _int(commit.get("rows")) if commit else 0}
        if dry_run:
            report.append({**entry, "result": "would copy"})
            continue
        info = _info_from_manifest_row(row) if row else _info_from_commit(commit)
        removed = row["duplicate_rows_removed"] if row else _int(commit.get("duplicate_rows_removed"))
        n_rows, version = 0, None
        if commit:
            rows = source.to_pyarrow_table(columns=list(SCHEMA.names), filters=[("run_id", "=", run)]).cast(SCHEMA)
            n_rows = rows.num_rows
            version = append_run(target_uri, run, rows, info, duplicate_rows_removed=removed)
        append_manifest_row(target_uri, run, info, rows=n_rows, table_version=version,
                            succeeded=row["succeeded"] if row else (None if listed else True),
                            duplicate_rows_removed=removed, recorded_at=row["recorded_at"] if row else None)
        report.append({**entry, "rows": n_rows, "target_version": version, "result": "copied"})
    return report
