"""A record of the root ID updates each run of update_root_ids makes, as Parquet and Delta Lake.

Off unless ROOT_ID_UPDATE_LOG is set, for everything or for chosen datastacks and tables
(see enabled). Each annotation table updated in a run gets a folder

    {MATERIALIZATION_DUMP_BUCKET}/root_id_updates/{datastack}/{run_id}/{annotation_table}/
        part-{root_column}-{task_id}.parquet   one per get_new_root_ids task
        _delta_log/                            added once every chunk is done, when the
                                               chunks are compacted into a few files and
                                               then deleted
        _run.json                              what is the same for every row

run_id is the run's materialization timestamp, which every table in the run shares, so a
run's updates sit under one folder. Rows hold only what varies per update (see SCHEMA).
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
MANIFEST = "_run.json"
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


def run_uri(mat_metadata: dict) -> Optional[str]:
    """The folder for this table's updates in this run, or None without a dump bucket."""
    bucket = get_config_param("MATERIALIZATION_DUMP_BUCKET")
    if not bucket:
        return None
    return "/".join(
        [
            str(bucket).rstrip("/"),
            "root_id_updates",
            mat_metadata["datastack"],
            run_id(mat_metadata["materialization_time_stamp"]),
            mat_metadata["annotation_table_name"],
        ]
    )


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


def _manifest(mat_metadata: dict) -> dict:
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


def _write_manifest(filesystem, path: str, manifest: dict):
    filesystem.create_dir(path, recursive=True)
    with filesystem.open_output_stream(f"{path}/{MANIFEST}") as out:
        out.write(json.dumps(manifest, indent=1).encode())


def start(mat_metadata: dict) -> None:
    """Describe the run before its chunks are written, so a run that dies partway still
    explains its files."""
    try:
        uri = run_uri(mat_metadata)
        if uri is None:
            return
        filesystem, path = _filesystem(uri)
        _write_manifest(filesystem, path, {**_manifest(mat_metadata), "state": "running"})
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
        if not chunks and not has_table and MANIFEST not in names:
            return  # nothing was updated, and nothing started a record
        previous = {}
        if MANIFEST in names:
            with filesystem.open_input_stream(f"{path}/{MANIFEST}") as f:
                previous = json.loads(f.read())
        manifest = {
            **_manifest(mat_metadata),
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
        _write_manifest(filesystem, path, manifest)
        celery_logger.info(f"Recorded {manifest['rows']} root ID updates in {uri}")
    except Exception as e:
        celery_logger.warning(f"Could not finish the root ID update record for {mat_metadata.get('annotation_table_name')}: {e}")
