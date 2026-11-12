"""Deleting objects under a lake.

Through obstore (as lake_index reads lakes), whose object_store client retries transient
GCS errors with backoff. The spike's batch deletes through google-cloud-storage failed
whole jobs on "503 BATCH ... internal error" under 16 threads. Deletes are idempotent: a
path that is already gone is not an error, so a retried task can repeat its deletes.
"""

import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from typing import Iterable, List
from urllib.parse import unquote

logger = logging.getLogger(__name__)

_CHUNK = 100


def _store(uri: str):
    from materializationengine.blueprints.client.lake_index import store

    return store(uri)


def _is_local(uri: str) -> bool:
    return "://" not in uri or uri.startswith("file://")


def list_paths(uri: str, prefix: str = "") -> List[str]:
    """Paths of every object under uri/prefix, relative to uri."""
    import obstore

    if _is_local(uri) and not os.path.isdir(os.path.join(uri.removeprefix("file://"), prefix)):
        return []
    return [o["path"] for o in obstore.list(_store(uri), prefix.strip("/") or None).collect()]


def delete_paths(uri: str, paths: Iterable[str], threads: int = 8) -> int:
    """Delete paths relative to uri, in parallel chunks; returns how many were asked for.

    Paths as a Delta log records them are URL-encoded; they are decoded here.
    """
    import obstore

    keys = [unquote(p).lstrip("/") for p in paths]
    if not keys:
        return 0
    store = _store(uri)

    def delete(chunk):
        try:
            obstore.delete(store, chunk)
        except FileNotFoundError:
            for key in chunk:
                try:
                    obstore.delete(store, key)
                except FileNotFoundError:
                    pass

    chunks = [keys[i:i + _CHUNK] for i in range(0, len(keys), _CHUNK)]
    with ThreadPoolExecutor(min(threads, len(chunks))) as pool:
        list(pool.map(delete, chunks))
    return len(keys)


def delete_prefix(uri: str, prefix: str = "", threads: int = 8) -> int:
    """Delete everything under uri/prefix; returns objects deleted.

    On a local path the emptied directories are removed too (object stores have none).
    """
    paths = list_paths(uri, prefix)
    deleted = delete_paths(uri, paths, threads=threads)
    if _is_local(uri):
        shutil.rmtree(os.path.join(uri.removeprefix("file://"), prefix), ignore_errors=True)
    return deleted
