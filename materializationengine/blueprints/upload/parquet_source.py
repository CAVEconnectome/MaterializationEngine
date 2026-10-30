"""Uploads read from parquet: one file, a folder of files, or a Delta Lake table.

A folder is read recursively. Hive-style directories (``key=value/``) become columns
with those values, as pyarrow and Spark read them. Entries whose name starts with ``_``
or ``.`` are skipped (``_delta_log``, ``_SUCCESS``, ``.crc`` files).

A folder holding a ``_delta_log`` is read as a Delta table: only the files in the
table's current version, since files an optimize or overwrite replaced stay on disk
until a vacuum and would otherwise be read twice.

The work is split into parts of whole row groups from the parquet footers, so every
part knows its row count before it runs. Generated ids then need no counting pass.
"""

import functools
import io
import os
import posixpath
import struct
from urllib.parse import quote
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import unquote, urlparse

import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from materializationengine.blueprints.upload.source import SourceAccessError

# Rows per parquet part task: about the work of a 128 MB CSV part, about a minute.
DEFAULT_PARQUET_PART_ROWS = 1_500_000
FOOTER_THREADS = 32
# One request of a file's last bytes holds the footer of all but very wide files.
FOOTER_TAIL_BYTES = 64 * 1024


def is_parquet_source(uri: str) -> bool:
    """True for a .parquet file or a folder, False for a .csv file.

    A name with another extension is refused; end the location with "/" to read a
    folder whose name has a dot in it.
    """
    raw_path = urlparse((uri or "").strip()).path
    name = posixpath.basename(raw_path.rstrip("/")).lower()
    if raw_path.endswith("/") or name.endswith(".parquet") or "." not in name:
        return True
    if name.endswith(".csv"):
        return False
    raise ValueError(
        f"{name!r} is not a .csv or .parquet file; to read a folder, end the location with /"
    )


def _split(uri: str) -> Tuple[str, str, str]:
    parsed = urlparse(uri.strip())
    scheme = parsed.scheme.lower()
    if scheme == "file":
        # local paths, for tests; the upload API only accepts gs:// and s3://
        return "file", "", parsed.path.rstrip("/") or "/"
    if scheme not in ("gs", "s3"):
        raise ValueError(
            f"Unsupported location {uri!r}: use gs://bucket/path or s3://bucket/path"
        )
    if not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"{uri!r} does not name a file or folder")
    return scheme, parsed.netloc, parsed.path.strip("/")


def filesystem_for(uri: str) -> Tuple[pafs.FileSystem, str]:
    """A pyarrow filesystem that can read uri, and uri's path within it."""
    scheme, bucket, key = _split(uri)
    if scheme == "file":
        return pafs.LocalFileSystem(), key
    if scheme == "s3":
        return _s3_filesystem(bucket), f"{bucket}/{key}"
    return _gcs_filesystem(), f"{bucket}/{key}"


def _filesystem_path(uri: str) -> str:
    """The path of a file URI (as delta-rs lists them) within filesystem_for."""
    if "://" not in uri:
        return uri
    scheme, bucket, key = _split(uri)
    return key if scheme == "file" else f"{bucket}/{key}"


@functools.lru_cache(maxsize=32)
def _s3_filesystem(bucket: str) -> pafs.S3FileSystem:
    try:
        region = pafs.resolve_s3_region(bucket)
    except Exception:
        region = "us-east-1"
    return pafs.S3FileSystem(anonymous=True, region=region)


def _gcs_filesystem() -> pafs.GcsFileSystem:
    """GCS with the deployment's Google credentials, or anonymous without any."""
    try:
        import google.auth
        import google.auth.transport.requests

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
        )
        credentials.refresh(google.auth.transport.requests.Request())
        return pafs.GcsFileSystem(
            access_token=credentials.token,
            credential_token_expiration=credentials.expiry,
        )
    except Exception:
        return pafs.GcsFileSystem(anonymous=True)


def _storage_options(uri: str) -> Dict[str, str]:
    """Credentials for deltalake (delta-rs), matching filesystem_for."""
    scheme, bucket, _ = _split(uri)
    if scheme == "file":
        return {}
    if scheme == "s3":
        try:
            region = pafs.resolve_s3_region(bucket)
        except Exception:
            region = "us-east-1"
        return {"aws_skip_signature": "true", "aws_region": region}
    try:
        import google.auth
        import google.auth.transport.requests

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
        )
        credentials.refresh(google.auth.transport.requests.Request())
        return {"bearer_token": credentials.token}
    except Exception:
        return {}


def _tail_reader(uri: str):
    """A function (path, n) -> the last n bytes of a file, for reading footers.

    pyarrow's GCS filesystem serialized concurrent footer reads (315 files of a Delta
    export: 151 s); one suffix-range request each, 32 at a time, took 3 s.
    """
    scheme, bucket, _ = _split(uri)
    if scheme == "file":
        def tail(path, n):
            with open(path, "rb") as f:
                f.seek(0, os.SEEK_END)
                f.seek(max(f.tell() - n, 0))
                return f.read()
        return tail
    if scheme == "s3":
        from materializationengine.blueprints.upload.source import anonymous_s3_client

        client = anonymous_s3_client(bucket)

        def tail(path, n):
            key = path.split("/", 1)[1]
            return client.get_object(Bucket=bucket, Key=key, Range=f"bytes=-{n}")["Body"].read()
        return tail

    import requests

    try:
        import google.auth
        from google.auth.transport.requests import AuthorizedSession

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
        )
        session = AuthorizedSession(credentials)
    except Exception:
        session = requests.Session()  # public objects read anonymously

    def tail(path, n):
        key = path.split("/", 1)[1]
        url = (
            f"https://storage.googleapis.com/download/storage/v1/b/{bucket}/o/"
            f"{quote(key, safe='')}?alt=media"
        )
        response = session.get(url, headers={"Range": f"bytes=-{n}"}, timeout=60)
        if response.status_code in (401, 403):
            raise SourceAccessError(f"Permission denied reading gs://{path}")
        response.raise_for_status()
        return response.content
    return tail


def read_footer(tail, path: str) -> pq.FileMetaData:
    """A parquet file's metadata, from the end of the file only."""
    data = tail(path, FOOTER_TAIL_BYTES)
    if len(data) < 12 or data[-4:] != b"PAR1":
        raise ValueError(f"{path} is not a parquet file")
    footer_length = struct.unpack("<I", data[-8:-4])[0]
    if footer_length + 8 > len(data):
        data = tail(path, footer_length + 8)
    # pyarrow checks only the magic bytes at either end; offsets inside are not read
    return pq.ParquetFile(io.BytesIO(b"PAR1" + data[-(footer_length + 8):])).metadata


def _hidden(name: str) -> bool:
    return name.startswith("_") or name.startswith(".")


def list_parquet_files(uri: str) -> Dict[str, Any]:
    """The parquet files of uri, in a stable order, and what kind of source it is.

    Returns {"kind": "file" | "folder" | "delta", "root": path, "files": [paths],
    "partition_columns": [names] (delta only)}. Paths are within filesystem_for(uri).
    """
    fs, root = filesystem_for(uri)
    try:
        info = fs.get_file_info(root)
    except OSError as e:
        raise SourceAccessError(f"Cannot read {uri}: {e}") from e
    if info.type == pafs.FileType.File:
        return {"kind": "file", "root": posixpath.dirname(root), "files": [root]}
    if info.type != pafs.FileType.Directory:
        raise FileNotFoundError(f"{uri} does not exist")

    if fs.get_file_info(f"{root}/_delta_log").type == pafs.FileType.Directory:
        from deltalake import DeltaTable

        table_uri = root if _split(uri)[0] == "file" else uri.rstrip("/")
        try:
            table = DeltaTable(table_uri, storage_options=_storage_options(uri))
        except Exception as e:
            raise SourceAccessError(f"Cannot read the Delta table at {uri}: {e}") from e
        files = sorted(_filesystem_path(u) for u in table.file_uris())
        return {
            "kind": "delta",
            "root": root,
            "files": files,
            "partition_columns": list(table.metadata().partition_columns),
            "version": table.version(),
        }

    try:
        entries = fs.get_file_info(pafs.FileSelector(root, recursive=True))
    except OSError as e:
        raise SourceAccessError(f"Cannot list {uri}: {e}") from e
    files = sorted(
        e.path
        for e in entries
        if e.type == pafs.FileType.File
        and e.path.endswith(".parquet")
        and not any(_hidden(part) for part in posixpath.relpath(e.path, root).split("/"))
    )
    if not files:
        raise FileNotFoundError(f"No parquet files under {uri}")
    return {"kind": "folder", "root": root, "files": files}


def hive_partition_values(path: str, root: str) -> Dict[str, Any]:
    """{key: value} from the key=value directories between root and the file."""
    values = {}
    for part in posixpath.relpath(posixpath.dirname(path), root).split("/"):
        if "=" in part:
            key, _, raw = part.partition("=")
            values[unquote(key)] = _typed(unquote(raw))
    return values


def _typed(raw: str):
    if raw == "__HIVE_DEFAULT_PARTITION__":
        return None
    for cast in (int, float):
        try:
            return cast(raw)
        except ValueError:
            pass
    return raw


def plan_parquet_parts(
    uri: str, target_rows: int = DEFAULT_PARQUET_PART_ROWS
) -> Dict[str, Any]:
    """Split a parquet source into parts of about target_rows each.

    A part is whole row groups, or a slice of one row group bigger than target_rows
    (some writers put a whole file in one row group): a part must stay a short task.

    Returns {"kind", "root", "columns", "total_rows", "total_bytes", "files",
    "parts": [{"pieces": [piece, ...], "rows": n}, ...]}, where a piece is
    [path, [row group, ...]] or [path, [row group], first row, row count].
    """
    listing = list_parquet_files(uri)
    root = listing["root"]
    tail = _tail_reader(uri)

    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=FOOTER_THREADS) as pool:
        footers = list(pool.map(lambda path: read_footer(tail, path), listing["files"]))

    first_partitions = hive_partition_values(listing["files"][0], root)
    file_columns = list(footers[0].schema.to_arrow_schema().names)
    columns = file_columns + [c for c in first_partitions if c not in file_columns]

    target_rows = max(int(target_rows), 1)
    parts: List[Dict[str, Any]] = []
    pieces: List[List[Any]] = []
    rows = 0
    for path, metadata in zip(listing["files"], footers):
        groups = []
        for index in range(metadata.num_row_groups):
            group_rows = metadata.row_group(index).num_rows
            if group_rows == 0:
                continue  # writers emit an empty row group for an empty file
            if group_rows > target_rows:
                # close what is open, then one part per slice of this row group
                if groups:
                    pieces.append([path, groups])
                    groups = []
                if pieces:
                    parts.append({"pieces": pieces, "rows": rows})
                    pieces, rows = [], 0
                for first in range(0, group_rows, target_rows):
                    count = min(target_rows, group_rows - first)
                    parts.append({"pieces": [[path, [index], first, count]], "rows": count})
                continue
            if rows and rows + group_rows > target_rows:
                if groups:
                    pieces.append([path, groups])
                    groups = []
                parts.append({"pieces": pieces, "rows": rows})
                pieces, rows = [], 0
            groups.append(index)
            rows += group_rows
        if groups:
            pieces.append([path, groups])
    if pieces:
        parts.append({"pieces": pieces, "rows": rows})

    return {
        "kind": listing["kind"],
        "root": root,
        "columns": columns,
        "total_rows": sum(m.num_rows for m in footers),
        "total_bytes": sum(
            m.row_group(i).total_byte_size
            for m in footers
            for i in range(m.num_row_groups)
        ),
        "files": len(listing["files"]),
        "parts": parts,
    }


def iter_parquet_part(
    uri: str,
    root: str,
    pieces: List[List[Any]],
    batch_size: int,
    exact_columns: Sequence[str] = (),
) -> Iterator[pd.DataFrame]:
    """The rows of a part's row groups as DataFrames of at most batch_size rows.

    Hive partition values are added as columns, unless a file has a column by that name.
    exact_columns, integer columns with nulls, come as Python ints and None rather than
    float64, which cannot hold 64-bit ids exactly.
    """
    fs, _ = filesystem_for(uri)
    for piece in pieces:
        path, row_groups = piece[0], piece[1]
        first, count = (piece[2], piece[3]) if len(piece) == 4 else (0, None)
        partition_values = hive_partition_values(path, root)
        with fs.open_input_file(path) as f:
            parquet = pq.ParquetFile(f)
            position = 0  # rows of the row group before this batch
            for batch in parquet.iter_batches(batch_size=batch_size, row_groups=row_groups):
                start, position = position, position + batch.num_rows
                if count is not None:
                    # a slice of one row group: skip the rows before it, stop after it
                    if position <= first:
                        continue
                    if start >= first + count:
                        break
                    batch = batch.slice(max(first - start, 0), first + count - max(start, first))
                df = batch.to_pandas()
                for name in exact_columns:
                    if name in batch.schema.names:
                        df[name] = batch.column(name).to_pandas(integer_object_nulls=True)
                for key, value in partition_values.items():
                    if key not in df.columns:
                        df[key] = value
                yield df


def preview_parquet(uri: str, rows: int = 1) -> Dict[str, Any]:
    """Columns, the first rows as strings, and the size of a parquet source."""
    plan = plan_parquet_parts(uri, target_rows=10**12)
    first: Optional[pd.DataFrame] = None
    for df in iter_parquet_part(uri, plan["root"], plan["parts"][0]["pieces"][:1], rows):
        first = df
        break
    columns = plan["columns"]
    preview = [columns]
    if first is not None:
        # object dtype keeps each value's own type (an int column stays "1001", not "1001.0")
        for _, row in first.head(rows).reindex(columns=columns).astype(object).iterrows():
            preview.append(["" if pd.isna(v) else str(v) for v in row.tolist()])
    return {
        "kind": plan["kind"],
        "columns": columns,
        "preview_rows": preview,
        "total_rows": plan["total_rows"],
        "total_bytes": plan["total_bytes"],
        "files": plan["files"],
        "name": os.path.basename(_split(uri)[2]),
    }
