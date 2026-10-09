"""A record of the root ID updates each run of update_root_ids makes, as Parquet and Delta Lake.

Off unless ROOT_ID_UPDATE_LOG is set, for everything or for chosen datastacks and tables
(see enabled). Updates are organized by table, then by run:

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/{datastack}/{annotation_table}/
        _manifest/                             append-only Delta table, one row per run
                                               with updates (see MANIFEST_SCHEMA)
        {run_id}/                              one run's updates: a Delta table
            part-{root_column}-{task_id}.parquet   one per get_new_root_ids task, until
                                                   the run finishes; then compacted into
                                                   a few files and deleted
            _delta_log/
            _run.json                          what is the same for every row

run_id is the run's materialization timestamp (20261009T141120.121393Z), shared by every
table updated in that run. Rows hold only what varies per update (see SCHEMA).

Records written before 2026-10-09 used {datastack}/{run_id}/{annotation_table}/; see
migrate_old_layout for copying them into this layout.
A retried task reuses its id and overwrites its own file; if a whole chunk is redone under
new task ids, its rows appear twice with the same values (deduplicate on root_column, id).

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

SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("root_column", pa.string()),
        ("supervoxel_id", pa.int64()),
        ("old_root_id", pa.int64()),
        ("new_root_id", pa.int64()),
    ]
)
RUN_FILE = "_run.json"
MANIFEST_DIR = "_manifest"
# One row per run with updates, appended by finalize; the way to list a table's updates.
MANIFEST_SCHEMA = pa.schema(
    [
        ("run_id", pa.string()),
        ("path", pa.string()),  # the run's folder, relative to the table folder
        ("start_time_stamp", pa.timestamp("us", tz="UTC")),  # last_updated_time_stamp: window start
        ("end_time_stamp", pa.timestamp("us", tz="UTC")),  # materialization_time_stamp: window end
        ("recorded_at", pa.timestamp("us", tz="UTC")),
        ("rows", pa.int64()),
        ("files", pa.int32()),
        ("root_columns", pa.list_(pa.string())),
        ("lookup_all_root_ids", pa.bool_()),
        ("find_all_expired_roots", pa.bool_()),
        ("duplicate_rows_removed", pa.int64()),  # set when a migrated run was deduplicated
    ]
)
# part-<root_column>-<celery task id>.parquet, as write_chunk names them
_CHUNK_FILE = re.compile(r"^part-(?P<root_column>[A-Za-z0-9_]+)-[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}\.parquet$")


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


def table_uri(mat_metadata: dict) -> Optional[str]:
    """The folder holding every recorded run of this table, or None without a dump bucket."""
    bucket = get_config_param("MATERIALIZATION_DUMP_BUCKET")
    if not bucket:
        return None
    return "/".join(
        [str(bucket).rstrip("/"), "root_id_updates", mat_metadata["datastack"], mat_metadata["annotation_table_name"]]
    )


def run_uri(mat_metadata: dict) -> Optional[str]:
    """The folder for this table's updates in this run, or None without a dump bucket."""
    base = table_uri(mat_metadata)
    if base is None:
        return None
    return f"{base}/{run_id(mat_metadata['materialization_time_stamp'])}"


def _utc(value) -> Optional[datetime.datetime]:
    if not value:
        return None
    ts = value if isinstance(value, datetime.datetime) else datetime.datetime.fromisoformat(str(value))
    return ts.replace(tzinfo=datetime.timezone.utc) if ts.tzinfo is None else ts.astimezone(datetime.timezone.utc)


def manifest_entry(run_info: dict, run_folder: str, duplicate_rows_removed: int = None) -> dict:
    """The manifest row for a finished run, from its _run.json content."""
    return {
        "run_id": run_folder,
        "path": run_folder,
        "start_time_stamp": _utc(run_info.get("last_updated_time_stamp")),
        "end_time_stamp": _utc(run_info.get("materialization_time_stamp")),
        "recorded_at": datetime.datetime.now(datetime.timezone.utc),
        "rows": int(run_info.get("rows") or 0),
        "files": int(run_info.get("files") or 0),
        "root_columns": list(run_info.get("root_columns") or []),
        "lookup_all_root_ids": bool(run_info.get("lookup_all_root_ids", False)),
        "find_all_expired_roots": bool(run_info.get("find_all_expired_roots", False)),
        "duplicate_rows_removed": duplicate_rows_removed,
    }


def append_manifest(table_folder_uri: str, entry: dict) -> bool:
    """Append *entry* to the table's manifest unless its run is already listed (so finalizing
    or migrating a run again adds nothing). Returns whether a row was added."""
    from deltalake import DeltaTable, write_deltalake

    uri = f"{table_folder_uri}/{MANIFEST_DIR}"
    if DeltaTable.is_deltatable(uri):
        listed = DeltaTable(uri).to_pyarrow_table(columns=["run_id"]).column("run_id").to_pylist()
        if entry["run_id"] in listed:
            return False
    write_deltalake(uri, pa.Table.from_pylist([entry], schema=MANIFEST_SCHEMA), mode="append")
    return True


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
        uri = run_uri(mat_metadata)
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
        "schema": [{"name": f.name, "type": str(f.type)} for f in SCHEMA],
    }


def _write_run_file(filesystem, path: str, manifest: dict):
    filesystem.create_dir(path, recursive=True)
    with filesystem.open_output_stream(f"{path}/{RUN_FILE}") as out:
        out.write(json.dumps(manifest, indent=1).encode())


def start(mat_metadata: dict) -> None:
    """Describe the run before its chunks are written, so a run that dies partway still
    explains its files."""
    try:
        uri = run_uri(mat_metadata)
        if uri is None:
            return
        filesystem, path = _filesystem(uri)
        _write_run_file(filesystem, path, {**_run_info(mat_metadata), "state": "running"})
    except Exception as e:
        celery_logger.warning(f"Could not start the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")


def finalize(mat_metadata: dict) -> None:
    """Once every chunk is written: make the folder a Delta table, compact its many small
    chunk files into a few large ones, delete the chunk files (vacuum), and complete _run.json.

    Safe to run again: counts already recorded are kept when the chunk files are gone.
    """
    try:
        uri = run_uri(mat_metadata)
        if uri is None:
            return
        filesystem, path = _filesystem(uri)
        listing = filesystem.get_file_info(fs.FileSelector(path, allow_not_found=True))
        names = {f.base_name for f in listing}
        chunks = [f for f in listing if f.type == fs.FileType.File and _CHUNK_FILE.match(f.base_name)]
        has_table = "_delta_log" in names
        if not chunks and not has_table and RUN_FILE not in names:
            return  # nothing was updated, and nothing started a record
        previous = {}
        if RUN_FILE in names:
            with filesystem.open_input_stream(f"{path}/{RUN_FILE}") as f:
                previous = json.loads(f.read())
        manifest = {
            **_run_info(mat_metadata),
            "state": "done",
            "chunk_files": len(chunks) or previous.get("chunk_files", 0),
            "root_columns": sorted({_CHUNK_FILE.match(f.base_name)["root_column"] for f in chunks})
            or previous.get("root_columns", []),
            "rows": 0,
        }
        for key in ("compaction", "vacuumed_files"):
            if key in previous:
                manifest[key] = previous[key]
        if chunks or has_table:
            from deltalake import DeltaTable, convert_to_deltalake

            if not has_table:
                convert_to_deltalake(uri)
            compacted = DeltaTable(uri).optimize.compact()
            if compacted.get("numFilesRemoved"):
                manifest["compaction"] = {k: compacted.get(k) for k in ("numFilesAdded", "numFilesRemoved")}
            # The chunk files now hold nothing the compacted files do not; nobody reads the
            # table while its run is still finishing, so no retention period is needed.
            deleted = DeltaTable(uri).vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
            # vacuum also lists files an earlier run already deleted; count only real deletions
            removed = {d.rsplit("/", 1)[-1] for d in deleted} & {f.base_name for f in chunks}
            if removed:
                manifest["vacuumed_files"] = manifest.get("vacuumed_files", 0) + len(removed)
            table = DeltaTable(uri)
            manifest.update(
                delta_table=uri,
                rows=sum(table.get_add_actions(flatten=True).column("num_records").to_pylist()),
                files=len(table.file_uris()),
            )
        _write_run_file(filesystem, path, manifest)
        if manifest.get("delta_table"):
            append_manifest(table_uri(mat_metadata), manifest_entry(manifest, uri.rsplit("/", 1)[1]))
        celery_logger.info(f"Recorded {manifest['rows']} root ID updates in {uri}")
    except Exception as e:
        celery_logger.warning(f"Could not finish the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")


_RUN_ID = re.compile(r"^\d{8}T\d{6}\.\d{6}Z$")


def migrate_old_layout(datastack_uri: str, dry_run: bool = True) -> list:
    """Copy records from the old {datastack}/{run_id}/{table}/ layout into
    {datastack}/{table}/{run_id}/, without exact duplicate rows, and list them in each
    table's manifest. The old folders are left as they are.

    *datastack_uri* is e.g. gs://bucket/root_id_updates/wclee_aedes_brain. Safe to run
    again: a run already in the new layout is not rewritten, and the manifest skips runs
    it already lists. Runs that recorded no updates (no Delta table) are reported, not
    copied. Returns one report dict per old run folder and table.
    """
    import polars as pl
    from deltalake import DeltaTable, write_deltalake

    datastack_uri = datastack_uri.rstrip("/")
    filesystem, base = _filesystem(datastack_uri)
    report = []
    run_dirs = sorted(
        f.base_name
        for f in filesystem.get_file_info(fs.FileSelector(base))
        if f.type == fs.FileType.Directory and _RUN_ID.match(f.base_name)
    )
    for run_folder in run_dirs:
        for t in filesystem.get_file_info(fs.FileSelector(f"{base}/{run_folder}")):
            if t.type != fs.FileType.Directory:
                continue
            table, old_uri = t.base_name, f"{datastack_uri}/{run_folder}/{t.base_name}"
            new_table_uri, new_uri = f"{datastack_uri}/{table}", f"{datastack_uri}/{table}/{run_folder}"
            entry = {"run_id": run_folder, "table": table, "old": old_uri, "new": new_uri}
            try:
                with filesystem.open_input_stream(f"{t.path}/{RUN_FILE}") as f:
                    run_info = json.loads(f.read())
            except (OSError, FileNotFoundError):
                report.append({**entry, "result": "no _run.json, skipped"})
                continue
            if not DeltaTable.is_deltatable(old_uri):
                report.append({**entry, "result": f"no updates (rows={run_info.get('rows', 0)}), skipped"})
                continue
            rows = DeltaTable(old_uri).to_pyarrow_table()
            unique = pl.from_arrow(rows).unique(maintain_order=True).to_arrow().cast(SCHEMA)
            removed = rows.num_rows - unique.num_rows
            entry.update(rows=rows.num_rows, unique_rows=unique.num_rows, duplicate_rows_removed=removed)
            if dry_run:
                report.append({**entry, "result": "would copy"})
                continue
            if DeltaTable.is_deltatable(new_uri):
                result = "already copied"
            else:
                write_deltalake(new_uri, unique, mode="error")
                result = "copied"
            new_info = {
                **run_info,
                "rows": unique.num_rows,
                "files": len(DeltaTable(new_uri).file_uris()),
                "delta_table": new_uri,
                "migrated_from": old_uri,
                "duplicate_rows_removed": removed,
            }
            _write_run_file(filesystem, _filesystem(new_uri)[1], new_info)
            added = append_manifest(new_table_uri, manifest_entry(new_info, run_folder, duplicate_rows_removed=removed))
            report.append({**entry, "result": result, "manifest_row_added": added})
    return report
