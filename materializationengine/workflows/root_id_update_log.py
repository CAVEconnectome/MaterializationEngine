"""A record of the root ID updates each run of update_root_ids makes, as one append-only Delta
table per annotation table.

Off unless ROOT_ID_UPDATE_LOG is set, for everything or for chosen datastacks and tables
(see enabled).

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/{datastack}/{annotation_table}/   the Delta table
        _delta_log/                          one commit per run, with the run's details as
                                             commit metadata (DeltaTable(...).history())
        run_id=20261009T173330.002540Z/      that run's updates, a few Parquet files

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/_staging/{datastack}/{annotation_table}/{run_id}/
        part-{root_column}-{task_id}.parquet one per get_new_root_ids task while the run is in
        _run.json                            progress; outside the table so mirrors never see them

run_id is the run's materialization timestamp (20261009T173330.002540Z), the one timestamp
every lookup in the run used, shared by every table updated in that run. Rows hold only what
varies per update plus that timestamp (SCHEMA); the window start and the rest of the run's
details are in the commit metadata (COMMIT_KEYS).

The table is append-only (delta.appendOnly, enforced by the writer) and keeps its whole log,
so it can be mirrored and kept up to date by fetching only what is new:

    gcloud storage rsync -r gs://.../root_id_updates/{datastack}/{table} ./{table}
        # files are never rewritten: each sync copies only new commits and their files
    added_files(uri, after_version=N)
        # or exactly the files the commits after version N added
    DeltaTable("./{table}").to_pandas(), pl.read_delta(...), duckdb delta_scan(...)

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
# The table's columns: the updates, the run's lookup timestamp, and run_id (the partition).
TABLE_SCHEMA = pa.schema(
    list(SCHEMA)
    + [("end_time_stamp", pa.timestamp("us", tz="UTC")), ("run_id", pa.string())]
)
PARTITION = "run_id"
TABLE_PROPERTIES = {
    "delta.appendOnly": "true",
    # Incremental sync replays every commit: keep the whole log (delta-rs would otherwise
    # delete log entries older than 30 days when it checkpoints).
    "delta.enableExpiredLogCleanup": "false",
    "delta.logRetentionDuration": "interval 36500 days",
}
TARGET_FILE_BYTES = 256 * 1024 * 1024
# Above this many chunk files (a dense lookup), finalize streams them in groups of this size
# into the one commit instead of reading the whole run into memory; duplicates are then
# removed within each group only (since the expired-root dedup, they come only from retries).
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


def _with_run_columns(rows: pa.Table, run: str, end) -> pa.Table:
    n = rows.num_rows
    return rows.cast(SCHEMA).append_column(
        "end_time_stamp", pa.array([end] * n, type=TABLE_SCHEMA.field("end_time_stamp").type)
    ).append_column("run_id", pa.array([run] * n, type=pa.string()))


def append_run(table_folder_uri: str, run: str, rows, info: dict, n_rows: int = None,
               duplicate_rows_removed=0) -> bool:
    """Commit one run's (already deduplicated) update rows to the table as one commit, unless
    it is already there. *rows* is a pa.Table, or an iterable of them (streamed into the same
    commit; then pass *n_rows*). *info* is the run's _run.json content. Returns whether a
    commit was made."""
    from deltalake import CommitProperties, PostCommitHookProperties, Transaction, write_deltalake

    if is_committed(table_folder_uri, run):
        return False
    end = _utc(info.get("materialization_time_stamp"))
    if isinstance(rows, pa.Table):
        n_rows = rows.num_rows
        data = _with_run_columns(rows, run, end)
    else:
        parts = (_with_run_columns(t, run, end) for t in rows)
        data = pa.RecordBatchReader.from_batches(
            TABLE_SCHEMA, (batch for t in parts for batch in t.to_batches())
        )
    start = _utc(info.get("last_updated_time_stamp"))
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
        commit_properties=CommitProperties(
            custom_metadata=metadata, app_transactions=[Transaction(app_id=_app_id(run), version=1)]
        ),
        post_commithook_properties=PostCommitHookProperties(cleanup_expired_logs=False),
    )
    return True


def _read_files(filesystem, paths) -> pa.Table:
    if not paths:
        return SCHEMA.empty_table()
    return pa.concat_tables([pq.read_table(p, filesystem=filesystem).cast(SCHEMA) for p in paths])


def _delete_dir(filesystem, path: str):
    try:
        filesystem.delete_dir(path)
    except (OSError, FileNotFoundError):
        pass


def finalize(mat_metadata: dict) -> None:
    """Once every chunk is written: commit the run's deduplicated updates to the table as one
    commit, then delete its staging folder. Safe to run again (the commit is not repeated)."""
    try:
        staging = staging_uri(mat_metadata)
        if staging is None:
            return
        table_folder = table_uri(mat_metadata)
        run = staging.rsplit("/", 1)[1]
        filesystem, path = _filesystem(staging)
        listing = filesystem.get_file_info(fs.FileSelector(path, allow_not_found=True))
        chunks = [f for f in listing if f.type == fs.FileType.File and _CHUNK_FILE.match(f.base_name)]
        if chunks and not is_committed(table_folder, run):
            info = {**_run_info(mat_metadata), "chunk_files": len(chunks)}
            paths = [f.path for f in chunks]
            if len(paths) <= STREAM_GROUP_FILES:
                rows = _read_files(filesystem, paths)
                unique = _dedupe(rows)
                append_run(table_folder, run, unique, info, duplicate_rows_removed=rows.num_rows - unique.num_rows)
                celery_logger.info(f"Recorded {unique.num_rows} root ID updates of run {run} in {table_folder}")
            else:
                # Row count from the files' footers; duplicates removed per group, not counted.
                total = sum(pq.ParquetFile(p, filesystem=filesystem).metadata.num_rows for p in paths)
                groups = (
                    _dedupe(_read_files(filesystem, paths[i:i + STREAM_GROUP_FILES]))
                    for i in range(0, len(paths), STREAM_GROUP_FILES)
                )
                append_run(table_folder, run, groups, info, n_rows=total, duplicate_rows_removed=None)
                celery_logger.info(f"Recorded about {total} root ID updates of run {run} in {table_folder} (streamed)")
        _delete_dir(filesystem, path)
    except Exception as e:
        celery_logger.warning(f"Could not finish the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")


def runs(table_folder_uri: str) -> list:
    """Every committed run, oldest first: its commit metadata plus the table version."""
    from deltalake import DeltaTable

    if not DeltaTable.is_deltatable(table_folder_uri):
        return []
    out = []
    for commit in DeltaTable(table_folder_uri).history():
        if commit.get("run_id"):
            out.append({"version": commit["version"], **{k: commit.get(k) for k in COMMIT_KEYS}})
    return sorted(out, key=lambda c: c["version"])


def added_files(table_folder_uri: str, after_version: int = -1) -> list:
    """The data files (relative to the table) added by the commits after *after_version*:
    exactly what a mirror at that version needs, besides the new _delta_log entries."""
    from deltalake import DeltaTable

    table = DeltaTable(table_folder_uri)
    now = set(table.get_add_actions(flatten=True).column("path").to_pylist())
    if after_version < 0:
        return sorted(now)
    before = set(DeltaTable(table_folder_uri, version=after_version).get_add_actions(flatten=True).column("path").to_pylist())
    return sorted(now - before)


def migrate_runs_to_table(table_folder_uri: str, dry_run: bool = True) -> list:
    """Commit the per-run folders of the previous layout ({table}/{run_id}/: a Delta table plus
    _run.json, written 2026-10-09) into the table at *table_folder_uri*, one commit per run,
    oldest first. Leaves those folders in place; safe to run again. Returns one report per run.
    """
    from deltalake import DeltaTable

    table_folder_uri = table_folder_uri.rstrip("/")
    filesystem, base = _filesystem(table_folder_uri)
    report = []
    run_dirs = sorted(
        f.base_name
        for f in filesystem.get_file_info(fs.FileSelector(base, allow_not_found=True))
        if f.type == fs.FileType.Directory and _RUN_ID.match(f.base_name)
    )
    for run in run_dirs:
        source = f"{table_folder_uri}/{run}"
        entry = {"run_id": run, "source": source}
        try:
            with filesystem.open_input_stream(f"{base}/{run}/{RUN_FILE}") as f:
                info = json.loads(f.read())
        except (OSError, FileNotFoundError):
            report.append({**entry, "result": "no _run.json, skipped"})
            continue
        if not DeltaTable.is_deltatable(source):
            report.append({**entry, "result": "no updates, skipped"})
            continue
        if is_committed(table_folder_uri, run):
            report.append({**entry, "result": "already committed"})
            continue
        rows = _read_files(filesystem, [_filesystem(u)[1] for u in DeltaTable(source).file_uris()])
        unique = _dedupe(rows)
        removed = int(info.get("duplicate_rows_removed") or 0) + rows.num_rows - unique.num_rows
        entry.update(rows=unique.num_rows, duplicate_rows_removed=removed)
        if dry_run:
            report.append({**entry, "result": "would commit"})
            continue
        append_run(table_folder_uri, run, unique, info, duplicate_rows_removed=removed)
        report.append({**entry, "result": "committed"})
    return report
