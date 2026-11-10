"""The planning index of a Delta Lake export: all a lake query needs to plan, in a few MB.

Planning a lake query used to load each copy's Delta log (deltalake, 0.2-0.8 s per copy
in-region) and the Parquet footer of every candidate file, which a worker then kept parsed
(~1.6 MB each for minnie v1926 synapses_pni_2: 111 row groups x 16 columns). A cold worker
spent ~2.7 s on its first lake request, and the cached footers pushed it over uwsgi's
reload-on-rss limit, so it soon started cold again.

The index holds, per copy:
  files       path, size, footer length, and min/max of every numeric column
  row groups  file, rows, byte offset and per-column compressed sizes, and min/max of the
              columns whose row groups are narrow (the sort key, z-order columns: where
              row-group statistics can prune; elsewhere they span the file and cannot)
It is built from the footers once per lake, stored next to the lake at
``<lake>/_cave_index/v1.arrows`` (Delta ignores ``_`` paths; the expiry removes it with the
version's prefix), and loaded by workers in one GET. A query then fetches a file's footer
in the same request as the column chunks it reads (it is needed to decode them), so no
extra round trip; only raw footer bytes are cached, within QUERY_LAKE_FOOTER_CACHE_MB.

Redis coordinates the workers (db 0, keys under ``cave:lakeidx:``):
  build lock   one worker in the cluster builds a missing index; the others wait for the
               sidecar (QUERY_LAKE_INDEX_WAIT_SECONDS) rather than all fetching every footer
  use tallies  daily sorted sets of lakes queried, and of (datastack, table, sort key), so a
               new worker prewarms the lakes that are used, and the same tables of the newest
               versions before anyone has queried them
Redis is a coordinator only: without it each worker builds or loads what it needs itself.
"""

import base64
import datetime
import io
import json
import logging
import os
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote

import numpy as np
import pyarrow as pa

logger = logging.getLogger(__name__)

INDEX_PATH = "_cave_index/v1.arrows"
MAGIC = b"CAVEIDX1"
SELECTIVE_SPAN = 0.25  # keep a column's row-group stats if a row group spans < this of its file
FOOTER_TAIL = 64 * 1024
_KEY = "cave:lakeidx"
_TALLY_DAYS = 7


def _config(name, default):
    from materializationengine.utils import get_config_param

    return get_config_param(name, default)


# --------------------------------------------------------------------------- the index

def _numeric(t: pa.DataType) -> bool:
    return pa.types.is_integer(t) or pa.types.is_floating(t)


def _extremes(t: pa.DataType):
    if pa.types.is_floating(t):
        return np.float64, -np.inf, np.inf
    return np.int64, np.iinfo(np.int64).min, np.iinfo(np.int64).max


@dataclass
class LakeIndex:
    uri: str
    schema: pa.Schema
    paths: List[str]
    sizes: np.ndarray          # int64 per file
    footer_lens: np.ndarray    # int64 per file: footer thrift length (+8 for length and magic)
    file_stats: Dict[str, Tuple[np.ndarray, np.ndarray]]
    rg_file: np.ndarray        # int32 per row group: its file
    rg_index: np.ndarray       # int32 per row group: its position in the file
    rg_rows: np.ndarray        # int64 per row group
    rg_start: np.ndarray       # int64 per row group: first byte of its first column chunk
    col_sizes: np.ndarray      # int32 [row group, column]: compressed size of each chunk; the
                               # chunks of a row group are contiguous, in column order
    rg_stats: Dict[str, Tuple[np.ndarray, np.ndarray]]
    built: Dict = field(default_factory=dict)

    def __post_init__(self):
        self.columns = [f.name for f in self.schema]
        self.column_pos = {c: i for i, c in enumerate(self.columns)}
        # rows of rg arrays for each file: rg_first[f] .. rg_first[f+1]
        counts = np.bincount(self.rg_file, minlength=len(self.paths))
        self.rg_first = np.concatenate([[0], np.cumsum(counts)]).astype(np.int64)

    def chunk_ranges(self, rows: np.ndarray, positions: List[int]) -> List[Tuple[int, int]]:
        """(start, end) of the chunks of columns `positions` in row groups `rows`."""
        sizes = self.col_sizes[rows].astype(np.int64)
        starts = self.rg_start[rows][:, None] + np.cumsum(sizes, axis=1) - sizes
        return [(int(a), int(a + b)) for a, b in zip(starts[:, positions].ravel(), sizes[:, positions].ravel())]

    @property
    def n_row_groups(self) -> int:
        return int(len(self.rg_file))

    @property
    def total_bytes(self) -> int:
        return int(self.sizes.sum())

    def nbytes(self) -> int:
        n = sum(a.nbytes for a in (self.sizes, self.footer_lens, self.rg_file, self.rg_index, self.rg_rows,
                                   self.rg_start, self.col_sizes))
        n += sum(lo.nbytes + hi.nbytes for lo, hi in list(self.file_stats.values()) + list(self.rg_stats.values()))
        return n + sum(len(p) for p in self.paths)

    def matches_catalog(self, lake: Dict) -> bool:
        """Whether this index describes the lake the catalog row lists (same files and bytes):
        a lake rewritten in place under the same uri would otherwise be read at stale offsets."""
        files, size = lake.get("files"), lake.get("bytes")
        return (files is None or int(files) == len(self.paths)) and (size is None or int(size) == self.total_bytes)

    # --------------------------------------------------------------- serialization

    def to_bytes(self) -> bytes:
        files = {"path": pa.array(self.paths, pa.string()), "size": self.sizes, "footer_len": self.footer_lens}
        for c, (lo, hi) in self.file_stats.items():
            files[f"min:{c}"], files[f"max:{c}"] = lo, hi
        groups = {"file": self.rg_file, "rg": self.rg_index, "rows": self.rg_rows, "start": self.rg_start}
        for i, c in enumerate(self.columns):
            groups[f"size:{c}"] = self.col_sizes[:, i]
        for c, (lo, hi) in self.rg_stats.items():
            groups[f"min:{c}"], groups[f"max:{c}"] = lo, hi
        meta = {"uri": self.uri, "schema": base64.b64encode(self.schema.serialize().to_pybytes()).decode(),
                "built": self.built}
        parts = [json.dumps(meta).encode(), _ipc(pa.table(files)), _ipc(pa.table(groups))]
        out = io.BytesIO()
        out.write(MAGIC)
        for p in parts:
            out.write(len(p).to_bytes(8, "little"))
            out.write(p)
        return out.getvalue()

    @classmethod
    def from_bytes(cls, data: bytes) -> "LakeIndex":
        if data[:len(MAGIC)] != MAGIC:
            raise ValueError("not a lake planning index")
        pos, parts = len(MAGIC), []
        for _ in range(3):
            n = int.from_bytes(data[pos:pos + 8], "little")
            parts.append(data[pos + 8:pos + 8 + n])
            pos += 8 + n
        meta = json.loads(parts[0])
        schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(meta["schema"])))
        files, groups = _read_ipc(parts[1]), _read_ipc(parts[2])
        names = [f.name for f in schema]

        def stats(t):
            return {c: (t.column(f"min:{c}").to_numpy(), t.column(f"max:{c}").to_numpy())
                    for c in names if f"min:{c}" in t.column_names}

        sizes = np.empty((groups.num_rows, len(names)), np.int32)
        for i, c in enumerate(names):
            sizes[:, i] = groups.column(f"size:{c}").to_numpy()
        return cls(uri=meta["uri"], schema=schema, paths=files.column("path").to_pylist(),
                   sizes=files.column("size").to_numpy(), footer_lens=files.column("footer_len").to_numpy(),
                   file_stats=stats(files), rg_file=groups.column("file").to_numpy(),
                   rg_index=groups.column("rg").to_numpy(), rg_rows=groups.column("rows").to_numpy(),
                   rg_start=groups.column("start").to_numpy(), col_sizes=sizes, rg_stats=stats(groups), built=meta.get("built") or {})


def _ipc(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as w:
        w.write_table(table)
    return sink.getvalue().to_pybytes()


def _read_ipc(data: bytes) -> pa.Table:
    with pa.ipc.open_stream(pa.py_buffer(data)) as r:
        return r.read_all()


# ------------------------------------------------------------------------------ build

def store(uri: str):
    """An obstore store rooted at a lake: GCS (application credentials) or local."""
    import obstore.store

    key = (os.getpid(), uri)
    with _stores_lock:
        if key in _stores:
            return _stores[key]
    if "://" in uri:
        s = obstore.store.from_url(uri)
    else:
        os.makedirs(uri, exist_ok=True)
        s = obstore.store.LocalStore(os.path.abspath(uri))
    with _stores_lock:
        _stores[key] = s
    return s


_stores: Dict = {}
_stores_lock = threading.Lock()


class Fetched(io.RawIOBase):
    """A Parquet file of which only some byte ranges were fetched."""

    def __init__(self, size, pieces):
        self.size, self.pieces, self.pos = size, pieces, 0

    def readable(self):
        return True

    def seekable(self):
        return True

    def tell(self):
        return self.pos

    def seek(self, offset, whence=0):
        self.pos = {0: offset, 1: self.pos + offset, 2: self.size + offset}[whence]
        return self.pos

    def readinto(self, buffer):
        n = min(len(buffer), self.size - self.pos)
        for start, data in self.pieces:
            if start <= self.pos and self.pos + n <= start + len(data):
                buffer[:n] = data[self.pos - start:self.pos - start + n]
                self.pos += n
                return n
        raise IOError(f"bytes {self.pos}-{self.pos + n} were not fetched")


def parse_footer(size: int, tail: bytes):
    """FileMetaData of a file of `size` bytes from its last len(tail) bytes."""
    import pyarrow.parquet as pq

    # pyarrow first reads the last 64 KiB whatever the footer's length: pad a short footer
    # with zeros in front (only the footer's own bytes are parsed)
    pad = min(size, FOOTER_TAIL) - len(tail)
    if pad > 0:
        tail = bytes(pad) + tail
    return pq.ParquetFile(Fetched(size, [(size - len(tail), tail)])).metadata


def fetch_footer(uri: str, path: str, size: int) -> bytes:
    """The footer of a lake file (thrift, length and magic): one GET, two if it is large."""
    import obstore

    s, key = store(uri), unquote(path)
    tail = bytes(obstore.get_range(s, key, start=max(0, size - FOOTER_TAIL), end=size))
    need = int.from_bytes(tail[-8:-4], "little") + 8
    if need > len(tail):
        tail = bytes(obstore.get_range(s, key, start=size - need, end=size))
    return tail[-need:]


def _file_entry(uri: str, path: str, size: int, columns: List[str]):
    footer = fetch_footer(uri, path, size)
    meta = parse_footer(size, footer)
    leaves = [meta.schema.column(i).name for i in range(meta.num_columns)]
    if leaves != columns:
        raise ValueError(f"{path}: columns {leaves} are not the lake's {columns}")
    n, k = meta.num_row_groups, len(columns)
    starts, sizes = np.zeros((n, k), np.int64), np.zeros((n, k), np.int64)
    rows = np.zeros(n, np.int64)
    lo = {c: [None] * n for c in columns}
    hi = {c: [None] * n for c in columns}
    for r in range(n):
        rg = meta.row_group(r)
        rows[r] = rg.num_rows
        for i, c in enumerate(columns):
            col = rg.column(i)
            starts[r, i] = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
            sizes[r, i] = col.total_compressed_size
            if i and starts[r, i] != starts[r, i - 1] + sizes[r, i - 1]:
                raise ValueError(f"{path}: the column chunks of row group {r} are not contiguous")
            if sizes[r, i] >= 2 ** 31:
                raise ValueError(f"{path}: a column chunk of row group {r} is over 2 GiB")
            st = col.statistics
            if st is not None and st.has_min_max:
                lo[c][r], hi[c][r] = st.min, st.max
    return len(footer), rows, starts, sizes, lo, hi


def build(uri: str, key_columns: Iterable[str] = ()) -> LakeIndex:
    """Build a lake's planning index from its Delta log and every file's footer."""
    import concurrent.futures as cf

    from deltalake import DeltaTable

    t0 = time.perf_counter()
    dt = DeltaTable(uri)
    adds = pa.table(dt.get_add_actions(flatten=True))
    schema = dt.schema().to_arrow()
    schema = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    paths = adds.column("path").to_pylist()
    sizes = np.array(adds.column("size_bytes").to_pylist(), np.int64)
    # the columns stored in the files, in file order: a Hive partition column (e.g.
    # post_pt_root_id_partition) is in the Delta schema but only in the file paths
    stored = None
    if paths:
        meta = parse_footer(int(sizes[0]), fetch_footer(uri, paths[0], int(sizes[0])))
        stored = [meta.schema.column(i).name for i in range(meta.num_columns)]
    by_name = {f.name: f for f in schema}
    names = stored if stored is not None else [f.name for f in schema]
    schema = pa.schema([pa.field(n, by_name[n].type, by_name[n].nullable) for n in names])
    columns = [f.name for f in schema]
    numeric = [f.name for f in schema if _numeric(f.type)]

    # footers are parsed and reduced as they arrive: a parsed footer is ~1.6 MB, so holding
    # all of a 300-file lake's at once would take a worker's whole memory budget
    with cf.ThreadPoolExecutor(int(_config("QUERY_LAKE_REQUESTS_IN_FLIGHT", 64)),
                               thread_name_prefix="lake-index") as pool:
        entries = list(pool.map(lambda ps: _file_entry(uri, ps[0], int(ps[1]), columns), zip(paths, sizes)))

    footer_lens = np.array([e[0] for e in entries], np.int64)
    rg_file = np.concatenate([np.full(len(e[1]), i, np.int32) for i, e in enumerate(entries)] or [np.zeros(0, np.int32)])
    rg_index = np.concatenate([np.arange(len(e[1]), dtype=np.int32) for e in entries] or [np.zeros(0, np.int32)])
    rg_rows = np.concatenate([e[1] for e in entries] or [np.zeros(0, np.int64)])
    rg_start = np.concatenate([e[2][:, 0] for e in entries]) if entries else np.zeros(0, np.int64)
    col_sizes = (np.concatenate([e[3] for e in entries]) if entries
                 else np.zeros((0, len(columns)), np.int64)).astype(np.int32)

    file_stats, rg_stats = {}, {}
    keys = set(key_columns)
    counts = np.array([len(e[1]) for e in entries], np.int64)
    firsts = np.concatenate([[0], np.cumsum(counts)[:-1]]) if len(counts) else counts
    has = counts > 0
    for c in numeric:
        dtype, low, high = _extremes(schema.field(c).type)
        lo = np.array([low if v is None else v for e in entries for v in e[4][c]], dtype)
        hi = np.array([high if v is None else v for e in entries for v in e[5][c]], dtype)
        f_lo, f_hi = np.full(len(paths), low, dtype), np.full(len(paths), high, dtype)
        if has.any():
            f_lo[has] = np.minimum.reduceat(lo, firsts[has])
            f_hi[has] = np.maximum.reduceat(hi, firsts[has])
        file_stats[c] = (f_lo, f_hi)
        if not len(lo):
            continue
        known = (lo != low) & (hi != high)
        file_span = (f_hi - f_lo).astype(np.float64)[rg_file]
        with np.errstate(divide="ignore", invalid="ignore"):
            ratio = np.where(file_span > 0, (hi - lo).astype(np.float64) / file_span, 0.0)
        if c in keys or (known.all() and float(np.median(ratio)) < SELECTIVE_SPAN):
            rg_stats[c] = (lo, hi)

    index = LakeIndex(uri=uri, schema=schema, paths=paths, sizes=sizes, footer_lens=footer_lens,
                      file_stats=file_stats, rg_file=rg_file, rg_index=rg_index, rg_rows=rg_rows,
                      rg_start=rg_start, col_sizes=col_sizes, rg_stats=rg_stats,
                      built={"at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                             "by": socket.gethostname(), "delta_version": dt.version(),
                             "seconds": round(time.perf_counter() - t0, 3)})
    logger.info("lake index of %s: %d files, %d row groups, row-group stats on %s, %.1f MB, built in %.2fs",
                uri, len(paths), index.n_row_groups, sorted(rg_stats), index.nbytes() / 1e6, time.perf_counter() - t0)
    return index


def write(index: LakeIndex) -> None:
    import obstore

    obstore.put(store(index.uri), INDEX_PATH, index.to_bytes())


def read(uri: str) -> Optional[LakeIndex]:
    """The lake's stored index, or None if there is none (or it cannot be read)."""
    import obstore

    try:
        data = bytes(obstore.get(store(uri), INDEX_PATH).bytes())
    except FileNotFoundError:
        return None
    except Exception as e:  # obstore raises its own NotFoundError (a FileNotFoundError subclass) on 404
        if "not found" in str(e).lower() or "404" in str(e):
            return None
        raise
    try:
        return LakeIndex.from_bytes(data)
    except Exception:
        logger.exception("lake index of %s is unreadable; it will be rebuilt", uri)
        return None


def build_and_store(uri: str, key_columns: Iterable[str] = ()) -> LakeIndex:
    """Build the index and store it next to the lake (the exporter calls this as a lake completes)."""
    index = build(uri, key_columns)
    write(index)
    return index


# ----------------------------------------------------------------------- coordination

def _redis():
    from materializationengine.redis_client import get_redis_client

    return get_redis_client(0)


def key_columns(lake: Dict) -> set:
    """Columns a copy is laid out by: its sort/partition key, z-order columns, and the x/y/z
    of the point it is Morton-ordered on."""
    spec = lake.get("spec") or {}
    keys = {lake.get("partition_by"), spec.get("partition_by")}
    keys |= set(spec.get("zorder_columns") or [])
    g = spec.get("source_geometry_column")
    if g:
        keys |= {f"{g}_x", f"{g}_y", f"{g}_z"}
    keys.discard(None)
    return keys


def _build_coordinated(lake: Dict, wait: bool = True) -> Optional[LakeIndex]:
    """Build a missing index, or wait for the worker that is already building it (without
    `wait`, None: a prewarming worker moves on rather than wait)."""
    uri = lake["uri"]
    lock, token = f"{_KEY}:build:{uri}", uuid.uuid4().hex
    try:
        mine = bool(_redis().set(lock, token, nx=True, ex=int(_config("QUERY_LAKE_INDEX_BUILD_SECONDS", 300))))
    except Exception as e:
        logger.warning("lake index: no redis lock for %s (%s); building here", uri, e)
        mine = True
    if not mine and not wait:
        return None
    if not mine:
        deadline = time.monotonic() + float(_config("QUERY_LAKE_INDEX_WAIT_SECONDS", 20))
        while time.monotonic() < deadline:
            time.sleep(0.25)
            index = read(uri)
            if index is not None:
                return index
            try:
                if not _redis().exists(lock):  # the builder gave up: take over
                    break
            except Exception:
                break
        logger.warning("lake index of %s: waited for another worker's build; building here", uri)
    try:
        index = build(uri, key_columns(lake))
        try:
            write(index)
        except Exception as e:  # a worker that cannot write still has the index for itself
            logger.warning("lake index of %s could not be stored: %s: %s", uri, type(e).__name__, e)
        return index
    finally:
        if mine:
            try:
                if _redis().get(lock) in (token, token.encode()):
                    _redis().delete(lock)
            except Exception:
                pass


class _Cache:
    """Per-process indexes by uri, least recently used dropped first; one load per uri at a time."""

    def __init__(self):
        from collections import OrderedDict

        self.items: "OrderedDict[str, LakeIndex]" = OrderedDict()
        self.lock = threading.Lock()
        self.loading: Dict[str, threading.Lock] = {}

    def peek(self, uri) -> Optional[LakeIndex]:
        with self.lock:
            return self.items.get(uri)

    def get(self, lake: Dict, build_missing: bool = True, wait: bool = True) -> Tuple[Optional[LakeIndex], str]:
        """(index, how it was found: memory, stored, built, missing, or building elsewhere)."""
        uri = lake["uri"]
        with self.lock:
            hit = self.items.get(uri)
            if hit is not None and hit.matches_catalog(lake):
                self.items.move_to_end(uri)
                return hit, "memory"
            gate = self.loading.setdefault(uri, threading.Lock())
        with gate:
            with self.lock:
                hit = self.items.get(uri)
                if hit is not None and hit.matches_catalog(lake):
                    return hit, "memory"
            index, how = read(uri), "stored"
            if index is not None and not index.matches_catalog(lake):
                logger.warning("lake index of %s does not match the catalog (%d files, %d bytes); rebuilding",
                               uri, len(index.paths), index.total_bytes)
                index = None
            if index is None:
                if not build_missing:
                    return None, "missing"
                index, how = _build_coordinated(lake, wait), "built"
                if index is None:
                    return None, "building elsewhere"
            self.put(index)
            return index, how

    def put(self, index: LakeIndex) -> None:
        with self.lock:
            self.items[index.uri] = index
            self.items.move_to_end(index.uri)
            limit = max(1, int(_config("QUERY_LAKE_INDEX_CACHE", 32)))
            while len(self.items) > limit:
                self.items.popitem(last=False)

    def drop(self, uri: str) -> None:
        with self.lock:
            self.items.pop(uri, None)

    def clear(self):
        with self.lock:
            self.items.clear()


cache = _Cache()


def get(lake: Dict) -> Tuple[LakeIndex, str]:
    return cache.get(lake)


def _day(offset: int = 0) -> str:
    return (datetime.datetime.now(datetime.timezone.utc).date() - datetime.timedelta(days=offset)).strftime("%Y%m%d")


def record_use(datastack: str, table: str, lakes: List[Dict]) -> None:
    """Tally the lakes a query read, for prewarming (best effort, never raises)."""
    try:
        day, ttl = _day(), 86400 * (_TALLY_DAYS + 1)
        p = _redis().pipeline(transaction=False)
        for lake in lakes:
            p.zincrby(f"{_KEY}:lakes:{day}", 1, lake["uri"])
            p.zincrby(f"{_KEY}:tables:{day}", 1, json.dumps([datastack, table, lake.get("partition_by")]))
        p.expire(f"{_KEY}:lakes:{day}", ttl)
        p.expire(f"{_KEY}:tables:{day}", ttl)
        p.execute()
    except Exception as e:
        logger.debug("lake index: could not tally use: %s", e)


def _top(kind: str, n: int) -> List[Tuple[str, float]]:
    if n <= 0:
        return []
    totals: Dict[str, float] = {}
    p = _redis().pipeline(transaction=False)
    for d in range(_TALLY_DAYS):
        p.zrevrange(f"{_KEY}:{kind}:{_day(d)}", 0, 4 * n, withscores=True)
    for rows in p.execute():
        for member, score in rows:
            member = member.decode() if isinstance(member, bytes) else member
            totals[member] = totals.get(member, 0.0) + score
    return sorted(totals.items(), key=lambda kv: -kv[1])[:n]


def prewarm_targets(datastack: str, versions: List[int], lakes_of) -> List[Tuple[Dict, bool]]:
    """Lakes a new worker should load, most useful first, as (catalog row, may build).

    The newest versions' copies of the tables queried most (by datastack, table and sort key,
    across versions, so a new version is warm before anyone has queried it), then the lakes
    queried most. lakes_of(version) is the catalog's {table: [rows]} for a version."""
    n_tables = int(_config("QUERY_LAKE_PREWARM_TABLES", 4))
    n_lakes = int(_config("QUERY_LAKE_PREWARM_LAKES", 8))
    targets, seen = [], set()
    try:
        hot_tables = [json.loads(m) for m, _ in _top("tables", n_tables * 4)]
        hot_lakes = [m for m, _ in _top("lakes", n_lakes)]
    except Exception as e:
        logger.warning("lake prewarm: no use tallies from redis (%s)", e)
        hot_tables, hot_lakes = [], []
    hot_tables = [(t, p) for ds, t, p in hot_tables if ds == datastack][:n_tables]
    by_uri = {}
    for v in versions:
        try:
            lakes = lakes_of(v)
        except Exception as e:
            logger.warning("lake prewarm: no catalog for %s v%s: %s", datastack, v, e)
            continue
        for rows in lakes.values():
            for row in rows:
                by_uri[row["uri"]] = row
        for table, part in hot_tables:
            for row in lakes.get(table, []):
                if row.get("partition_by") == part and row["uri"] not in seen:
                    targets.append((row, True))
                    seen.add(row["uri"])
    for uri in hot_lakes:
        if uri not in seen:
            # an older version's lake: load it if it is stored and still listed, never build it here
            targets.append((by_uri.get(uri, {"uri": uri}), False))
            seen.add(uri)
    return targets


def prewarm(datastack: str, versions: List[int], lakes_of, budget_seconds: float) -> List[str]:
    """Load (building if needed, under the cluster-wide lock) the lakes prewarm_targets names,
    until the time budget runs out. Returns how each was found."""
    t0, done = time.monotonic(), []
    for lake, may_build in prewarm_targets(datastack, versions, lakes_of):
        if time.monotonic() - t0 > budget_seconds:
            break
        try:
            index, how = cache.get(lake, build_missing=may_build, wait=False)
            done.append(f"{'/'.join(lake['uri'].rstrip('/').rsplit('/', 3)[-3:])}:{how}")
        except Exception as e:
            logger.warning("lake prewarm of %s failed: %s: %s", lake["uri"], type(e).__name__, e)
    return done
