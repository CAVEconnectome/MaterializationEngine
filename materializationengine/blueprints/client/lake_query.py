"""Answer queries of a frozen version from its Delta Lake exports instead of Postgres.

Every frozen query -- the version query endpoints and the frozen half of the live query
(execute_materialized_query) -- ends in query._execute_query_unguarded with the SQL that
QueryManager compiled. When the lake backend is on and the catalog
(deltalake_catalog) lists complete lakes for every table the query reads, that same SQL
runs on DuckDB over rows read from the lakes:

1. The Postgres SQL is transpiled (sqlglot) and its PostGIS pieces rewritten for the
   lake's columns, where a point column ``p`` is stored as ``p_x``, ``p_y``, ``p_z``:
   ``ST_X(p)`` -> ``p_x``; ``p &&& ST_3DMakeBox(lo, hi)`` -> BETWEEN on x, y and z;
   ``ST_AsText(p)`` -> PostGIS's ``POINT Z (x y z)`` text, so hash sampling is
   reproduced exactly. An unmerged version's ``(SELECT ... FROM t LEFT OUTER JOIN
   t__seg ...) AS t`` becomes the lake itself, which is that join already flattened.
2. The WHERE clause's simple predicates (=, IN, <, <=, >, >=, BETWEEN against literals)
   pick, of a table's copies (one per sort key), the one whose files -- by the min/max
   statistics in the Delta log -- add up to the fewest bytes, and then, by the Parquet
   footers' row-group statistics, the row groups that can hold a match.
3. Only the columns the query uses are fetched, by byte range, many requests at once.
   DuckDB then runs the query over them, so every predicate, projection, join, limit
   and offset means exactly what it means in Postgres.

The result becomes a DataFrame through the same conversion as the ADBC fetch
(query._adbc_table_to_dataframe), so a response does not depend on the backend.

A lake that cannot answer is an error, never a silent switch to Postgres: the plan is
to retire the frozen databases, so a query the lakes cannot serve has to be found and
fixed. Tables with no lake in the catalog (versions exported before the catalog, or
not exported) are routed to Postgres as before, and logged.

Modes, QUERY_LAKE_BACKEND (a request's X-Query-Backend header overrides it):
  off     Postgres only (the default).
  on      lakes wherever the catalog has them.
  shadow  Postgres answers; the lake runs too, and the timings and whether the two
          results agree are logged. For validating the lake path on real traffic.

Frozen lakes never change, so their Delta logs and Parquet footers are cached per
process (QUERY_LAKE_LOG_CACHE lakes, QUERY_LAKE_FOOTER_CACHE footers). A query that
would fetch more than QUERY_LAKE_MAX_BYTES (default 512 MiB) of column chunks is refused
with a request to narrow it, as Postgres's statement timeout refuses an expensive one.
"""

import concurrent.futures as cf
import io
import logging
import os
import random
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

import numpy as np
import pyarrow as pa

logger = logging.getLogger(__name__)

MODES = ("off", "shadow", "on")
HEADER = "X-Query-Backend"
_HEADER_MODES = {"lake": "on", "on": "on", "postgres": "off", "sql": "off", "off": "off", "shadow": "shadow"}
_FROZEN_DB = re.compile(r"^(?P<datastack>.+)__mat(?P<version>\d+)$")

# Columns a query of an unmerged version selects that lakes do not keep. The frozen
# build keeps only rows that are valid and were created before the version
# (create_frozen_database), so deleted and superceded_id are NULL in every row and
# created is not used by any query; merged versions drop all three. A lake answer
# leaves them out, as a merged version does.
CRUD_COLUMNS = ("created", "deleted", "superceded_id")

FOOTER_BYTES = 256 * 1024   # one request usually holds a file's whole footer
MERGE_GAP = 64 * 1024       # column chunks closer than this are fetched as one range
STREAM_ROW_GROUPS = 64      # row groups fetched per round when a LIMIT can stop early
ROW_GROUPS_PER_READ = 4     # row groups per ranged GET: bounds the fetched bytes held at once


class LakeQueryError(Exception):
    """A query the lakes cannot answer, reported to the client (status code included)."""

    def __init__(self, message: str, status: int = 500):
        super().__init__(message)
        self.status = status


def _config(name, default):
    from materializationengine.utils import get_config_param

    return get_config_param(name, default)


def mode() -> str:
    try:
        from flask import has_request_context, request

        if has_request_context():
            header = request.headers.get(HEADER, "").strip().lower()
            if header in _HEADER_MODES:
                return _HEADER_MODES[header]
    except Exception:
        pass
    value = str(_config("QUERY_LAKE_BACKEND", "off")).strip().lower()
    return value if value in MODES else "off"


def frozen_target(engine) -> Optional[Tuple[str, int]]:
    """(datastack, version) of a frozen version's database, else None."""
    m = _FROZEN_DB.match(getattr(engine.url, "database", None) or "")
    return (m["datastack"], int(m["version"])) if m else None


# --------------------------------------------------------------------------- catalog

_lakes_lock = threading.Lock()
_lakes_cache: Dict[Tuple[str, int], Tuple[float, Dict, str]] = {}
_LAKES_TTL = 60.0


def datastack_info(datastack: str) -> Tuple[str, str]:
    """(aligned volume, segmentation table suffix) of a datastack."""
    from materializationengine.info_client import get_relevant_datastack_info

    return get_relevant_datastack_info(datastack)


def version_lakes(datastack: str, version: int) -> Tuple[Dict[str, List[Dict]], str]:
    """The version's complete lakes by table, and its segmentation table suffix."""
    key = (datastack, version)
    now = time.monotonic()
    with _lakes_lock:
        hit = _lakes_cache.get(key)
        if hit and now - hit[0] < _LAKES_TTL:
            return hit[1], hit[2]
    from materializationengine import deltalake_catalog as cat

    aligned_volume, pcg = datastack_info(datastack)
    # a re-export writes a second complete set of copies; serve only the newest copy of each
    # sort order, so planning reads one Delta log per sort order rather than one per export
    newest: Dict[Tuple[str, Optional[str]], Dict] = {}
    for row in cat.list_exports(aligned_volume, datastack, version):
        key = (row["table_name"], row.get("partition_by"))
        if key not in newest or str(row.get("completed_at") or "") > str(newest[key].get("completed_at") or ""):
            newest[key] = row
    by_table: Dict[str, List[Dict]] = {}
    for (table, _), row in sorted(newest.items(), key=lambda kv: (kv[0][0], str(kv[0][1]))):
        by_table.setdefault(table, []).append(row)
    with _lakes_lock:
        _lakes_cache[key] = (now, by_table, pcg)
    return by_table, pcg


# ----------------------------------------------------------------------- SQL rewrite

_BOX = re.compile(
    r"(?P<col>[\w\.\"]+)\s*&&&\s*ST_3DMakeBox\(\s*'POINTZ\((?P<lo>[-\d\.eE ]+)\)'\s*,\s*'POINTZ\((?P<hi>[-\d\.eE ]+)\)'\s*\)",
    re.I)


@dataclass
class Plan:
    sql: str                              # DuckDB SQL over the logical table names
    tables: Dict[str, str]                # logical table -> name it is registered under
    columns: Dict[str, set]               # logical table -> lake columns the query uses
    predicates: Dict[str, List[Tuple]]    # logical table -> [(column, op, value)]
    samples: Dict[str, float]             # logical table -> TABLESAMPLE percent
    limit: Optional[int]
    offset: int
    streamable: bool                      # one table, no order/group/distinct/aggregate


def _box_to_between(sql: str) -> str:
    def sub(m):
        col = m["col"]
        lo, hi = m["lo"].split(), m["hi"].split()
        if len(lo) != 3 or len(hi) != 3:
            raise LakeQueryError(f"unsupported bounding box {m.group(0)!r}", 400)
        # &&& is a bounding-box overlap: inclusive on both ends, order of corners free
        parts = [f"{col}_{a} BETWEEN {min(float(l), float(h))!r} AND {max(float(l), float(h))!r}"
                 for a, l, h in zip("xyz", lo, hi)]
        return "(" + " AND ".join(parts) + ")"

    return _BOX.sub(sub, sql)


def _literal(node):
    from sqlglot import exp

    if isinstance(node, exp.Paren):
        return _literal(node.this)
    if isinstance(node, exp.Neg):
        v = _literal(node.this)
        return None if v is None else -v
    if isinstance(node, exp.Cast):
        return _literal(node.this)
    if isinstance(node, exp.Literal) and node.is_number:
        text = node.this
        try:
            return int(text)
        except ValueError:
            return float(text)
    return None


def _conjuncts(node):
    from sqlglot import exp

    if node is None:
        return []
    if isinstance(node, exp.Paren):
        return _conjuncts(node.this)
    if isinstance(node, exp.And):
        return _conjuncts(node.left) + _conjuncts(node.right)
    return [node]


_FLIP = {"gt": "lt", "ge": "le", "lt": "gt", "le": "ge", "eq": "eq"}


def _predicate(node):
    """(column node, op, value) for a conjunct the statistics can use, else None."""
    from sqlglot import exp

    ops = {exp.EQ: "eq", exp.GT: "gt", exp.GTE: "ge", exp.LT: "lt", exp.LTE: "le"}
    if isinstance(node, exp.In) and isinstance(node.this, exp.Column) and node.expressions:
        values = [_literal(v) for v in node.expressions]
        if all(v is not None for v in values):
            return node.this, "in", values
    if isinstance(node, exp.Between) and isinstance(node.this, exp.Column):
        lo, hi = _literal(node.args.get("low")), _literal(node.args.get("high"))
        if lo is not None and hi is not None:
            return node.this, "between", (lo, hi)
    for cls, op in ops.items():
        if isinstance(node, cls):
            left, right = node.left, node.right
            if isinstance(left, exp.Column) and _literal(right) is not None:
                return left, op, _literal(right)
            if isinstance(right, exp.Column) and _literal(left) is not None:
                return right, _FLIP[op], _literal(left)
    return None


def _physical_to_logical(name: str, lakes: Dict, pcg: str) -> Optional[str]:
    if name in lakes:
        return name
    suffix = f"__{pcg}"
    if pcg and name.endswith(suffix) and name[: -len(suffix)] in lakes:
        return name[: -len(suffix)]
    return None


def plan_query(sql: str, lakes: Dict[str, List[Dict]], pcg: str, lake_columns,
               count: bool = False) -> Plan:
    """Rewrite QueryManager's Postgres SQL for DuckDB over the lakes.

    lake_columns(table) returns the column names of a table's lakes. Raises
    LakeQueryError for SQL the lakes cannot answer.
    """
    import sqlglot
    from sqlglot import exp

    tree = sqlglot.parse_one(_box_to_between(sql), read="postgres")

    # TABLESAMPLE SYSTEM (p): remember p per table and sample row groups when fetching
    samples: Dict[str, float] = {}
    for ts in list(tree.find_all(exp.TableSample)):
        table = ts.parent if isinstance(ts.parent, exp.Table) else ts.find_ancestor(exp.Table)
        pct = _literal(ts.args.get("percent") or ts.args.get("size"))
        target = table.name if isinstance(table, exp.Table) else None
        if target is None or pct is None:
            raise LakeQueryError(f"unsupported TABLESAMPLE in {ts.sql()!r}", 400)
        samples[target] = float(pct)
        ts.pop()

    # an unmerged version's per-table subquery over the annotation and segmentation
    # tables is the lake itself
    for sub in list(tree.find_all(exp.Subquery)):
        inner = sub.this
        if not isinstance(inner, exp.Select) or not sub.alias:
            continue
        names = {t.name for t in inner.find_all(exp.Table)}
        logical = {_physical_to_logical(n, lakes, pcg) for n in names}
        if len(logical) != 1 or None in logical:
            continue
        if any(inner.args.get(k) for k in ("where", "group", "having", "order", "limit", "offset")):
            raise LakeQueryError(f"unsupported subquery over {names}", 400)
        (table,) = logical
        for n in names:
            if n in samples:
                samples[table] = samples.pop(n)
        sub.replace(exp.Table(this=exp.to_identifier(table), alias=exp.TableAlias(this=exp.to_identifier(sub.alias))))

    tables: Dict[str, str] = {}   # alias or name -> logical table
    for t in tree.find_all(exp.Table):
        logical = _physical_to_logical(t.name, lakes, pcg)
        if logical is None:
            raise LakeQueryError(f"{t.name} has no lake", 404)
        if t.name != logical:  # a segmentation table outside the pattern above
            raise LakeQueryError(f"unsupported query shape over {t.name}", 400)
        tables[t.alias_or_name] = logical

    available = {table: set(lake_columns(table)) for table in set(tables.values())}

    def resolve(column) -> Optional[str]:
        if column.table:
            return tables.get(column.table)
        owners = [t for t in available if column.name in available[t] or f"{column.name}_x" in available[t]]
        return owners[0] if len(owners) == 1 else (next(iter(available)) if len(available) == 1 else None)

    def point(column) -> bool:
        table = resolve(column)
        return table is not None and column.name not in available[table] and f"{column.name}_x" in available[table]

    def axis_column(column, axis):
        return exp.column(f"{column.name}_{axis}", table=column.table or None)

    def rewrite(node):
        if isinstance(node, (exp.Anonymous, exp.Func)):
            name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).upper()
            args = node.expressions if isinstance(node, exp.Anonymous) else [node.this] if node.this else []
            if name in ("ST_X", "ST_Y", "ST_Z") and len(args) == 1 and isinstance(args[0], exp.Column):
                return axis_column(args[0], name[-1].lower())
            if name == "ST_ASTEXT" and len(args) == 1 and isinstance(args[0], exp.Column) and point(args[0]):
                # PostGIS's text of a 3D point of integer coordinates: POINT Z (x y z)
                c = args[0]
                parts = [exp.Literal.string("POINT Z (")]
                for i, a in enumerate("xyz"):
                    if i:
                        parts.append(exp.Literal.string(" "))
                    parts.append(exp.Cast(this=axis_column(c, a), to=exp.DataType.build("BIGINT")))
                parts.append(exp.Literal.string(")"))
                return exp.Concat(expressions=parts)
        return node

    tree = tree.transform(rewrite)

    # leave out the columns lakes do not keep (see CRUD_COLUMNS)
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    keep = []
    for item in select.expressions:
        cols = list(item.find_all(exp.Column))
        dropped = [c for c in cols if c.name in CRUD_COLUMNS and resolve(c)
                   and c.name not in available[resolve(c)]]
        if dropped and len(dropped) == len(cols):
            continue
        if dropped:
            raise LakeQueryError(f"{dropped[0].name} is not kept in lakes", 400)
        keep.append(item)
    if not keep and not count:
        raise LakeQueryError("no column of the query is kept in lakes", 400)
    select.set("expressions", keep or [exp.Literal.number(1)])

    # every remaining column must exist in its lake
    columns: Dict[str, set] = {t: set() for t in available}
    for c in tree.find_all(exp.Column):
        table = resolve(c)
        if table is None:
            if any(isinstance(a, exp.Alias) and a.alias == c.name for a in select.expressions):
                continue  # a reference to an output alias
            raise LakeQueryError(f"column {c.sql()} is ambiguous or unknown", 400)
        if c.name not in available[table]:
            raise LakeQueryError(f"column {c.name} of {table} is not in its lake", 400)
        columns[table].add(c.name)

    predicates: Dict[str, List[Tuple]] = {t: [] for t in available}
    where = select.args.get("where")
    for node in _conjuncts(where.this if where is not None else None):
        p = _predicate(node)
        if p is not None and resolve(p[0]) is not None:
            predicates[resolve(p[0])].append((p[0].name, p[1], p[2]))

    limit_node, offset_node = select.args.get("limit"), select.args.get("offset")
    limit = _literal(limit_node.expression) if limit_node is not None and limit_node.expression is not None else None
    offset = _literal(offset_node.expression) if offset_node is not None else 0
    streamable = (len(tables) == 1 and not count and limit is not None
                  and not any(select.args.get(k) for k in ("order", "group", "having", "distinct"))
                  and not any(item.find(exp.AggFunc) for item in select.expressions))

    return Plan(sql=tree.sql(dialect="duckdb"), tables={t: t for t in available}, columns=columns,
                predicates=predicates, samples=samples, limit=limit, offset=offset or 0,
                streamable=streamable)


# ------------------------------------------------------------------- lake reading

def _may_match(lo, hi, op, value) -> bool:
    """Can a [lo, hi] range of a column hold a row satisfying `column op value`?"""
    if lo is None or hi is None:
        return True
    try:
        if op == "in":
            return any(lo <= v <= hi for v in value)
        if op == "eq":
            return lo <= value <= hi
        if op == "between":
            return hi >= value[0] and lo <= value[1]
        if op == "gt":
            return hi > value
        if op == "ge":
            return hi >= value
        if op == "lt":
            return lo < value
        if op == "le":
            return lo <= value
    except TypeError:  # statistics of another type (timestamps, strings): no pruning
        return True
    return True


@dataclass
class LakeFile:
    path: str
    size: int
    stats: Dict[str, Tuple]


@dataclass
class LakeLog:
    uri: str
    columns: List[str]
    files: List[LakeFile]


class _LRU:
    def __init__(self, size_param: str, default: int):
        self.size_param, self.default = size_param, default
        self.items: "OrderedDict" = OrderedDict()
        self.lock = threading.Lock()

    def get(self, key, load):
        with self.lock:
            if key in self.items:
                self.items.move_to_end(key)
                return self.items[key]
        value = load()
        with self.lock:
            self.items[key] = value
            limit = int(_config(self.size_param, self.default))
            while len(self.items) > max(1, limit):
                self.items.popitem(last=False)
        return value


# Per process, and uwsgi runs many: ltv7's api pods give 16 processes 1 GiB between them.
_logs = _LRU("QUERY_LAKE_LOG_CACHE", 64)
_footers = _LRU("QUERY_LAKE_FOOTER_CACHE", 128)
_stores: Dict[Tuple[int, str], object] = {}
_stores_lock = threading.Lock()


def lake_log(uri: str) -> LakeLog:
    def load():
        from deltalake import DeltaTable

        dt = DeltaTable(uri)
        adds = pa.table(dt.get_add_actions(flatten=True))
        schema = dt.schema().to_arrow()
        schema = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
        names = [f.name for f in schema]
        stats_cols = {n: (adds.column(f"min.{n}").to_pylist(), adds.column(f"max.{n}").to_pylist())
                      for n in names if f"min.{n}" in adds.column_names and f"max.{n}" in adds.column_names}
        paths, sizes = adds.column("path").to_pylist(), adds.column("size_bytes").to_pylist()
        files = [LakeFile(p, int(s), {n: (lo[i], hi[i]) for n, (lo, hi) in stats_cols.items()})
                 for i, (p, s) in enumerate(zip(paths, sizes))]
        return LakeLog(uri, names, files)

    return _logs.get(uri, load)


def _store(uri: str):
    """An obstore store rooted at a lake: GCS (application credentials) or local."""
    import obstore.store

    key = (os.getpid(), uri)
    with _stores_lock:
        if key in _stores:
            return _stores[key]
    if "://" in uri:
        store = obstore.store.from_url(uri)
    else:
        store = obstore.store.LocalStore(os.path.abspath(uri))
    with _stores_lock:
        _stores[key] = store
    return store


class _Fetched(io.RawIOBase):
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


def _footer(uri: str, f: LakeFile):
    """The parsed footer of a lake file, cached (only the parsed form is kept)."""
    import obstore
    import pyarrow.parquet as pq

    def load():
        store, path = _store(uri), unquote(f.path)
        tail = bytes(obstore.get_range(store, path, start=max(0, f.size - FOOTER_BYTES), end=f.size))
        need = int.from_bytes(tail[-8:-4], "little") + 8
        if need > len(tail):
            tail = bytes(obstore.get_range(store, path, start=f.size - need, end=f.size))
        return pq.ParquetFile(_Fetched(f.size, [(f.size - len(tail), tail)])).metadata

    return _footers.get((uri, f.path), load)


_pools: Dict[int, cf.ThreadPoolExecutor] = {}
_pools_lock = threading.Lock()
_read_slots: Dict[int, threading.BoundedSemaphore] = {}


def _reads_in_flight() -> threading.BoundedSemaphore:
    """Reads of row groups running at once in this process. Each holds up to
    ROW_GROUPS_PER_READ row groups of fetched bytes (~0.8 MB each at 10k rows), so this,
    not the thread pool, bounds a query's transient memory."""
    pid = os.getpid()
    with _pools_lock:
        if pid not in _read_slots:
            _read_slots[pid] = threading.BoundedSemaphore(int(_config("QUERY_LAKE_READS_IN_FLIGHT", 16)))
        return _read_slots[pid]


def _pool() -> cf.ThreadPoolExecutor:
    pid = os.getpid()  # never a pool inherited across a fork
    with _pools_lock:
        if pid not in _pools:
            _pools[pid] = cf.ThreadPoolExecutor(int(_config("QUERY_LAKE_REQUESTS_IN_FLIGHT", 64)),
                                                thread_name_prefix="lake-read")
        return _pools[pid]


@dataclass
class Chunk:
    file: LakeFile
    groups: List[int]
    bytes: int


def _row_group_stats(meta, r, index, column):
    i = index.get(column)
    if i is None:
        return None, None
    st = meta.row_group(r).column(i).statistics
    if st is None or not st.has_min_max:
        return None, None
    return st.min, st.max


def _plan_file(uri, f, predicates, columns, sample):
    meta = _footer(uri, f)
    index = {meta.schema.column(i).name: i for i in range(meta.num_columns)}
    groups = []
    for r in range(meta.num_row_groups):
        if all(_may_match(*_row_group_stats(meta, r, index, c), op, v) for c, op, v in predicates):
            if sample is None or random.random() * 100.0 < sample:
                groups.append(r)
    size = sum(meta.row_group(r).column(index[c]).total_compressed_size
               for r in groups for c in columns if c in index)
    return Chunk(f, groups, size)


def _row_mask(table: pa.Table, predicates: List[Tuple]):
    """Rows of `table` that satisfy the statistics predicates, or None for all of them.

    The predicates are top-level AND conjuncts of the query's WHERE, which DuckDB applies
    again in full, so dropping rows that fail one keeps every row the query can return.
    A predicate pyarrow cannot evaluate (a literal of another type) is left to DuckDB."""
    import pyarrow.compute as pc

    mask = None
    for name, op, value in predicates:
        if name not in table.column_names:
            continue
        col = table.column(name)
        try:
            if op == "in":
                m = pc.is_in(col, value_set=pa.array(value).cast(col.type))
            elif op == "between":
                lo, hi = (pa.scalar(v).cast(col.type) for v in value)
                m = pc.and_(pc.greater_equal(col, lo), pc.less_equal(col, hi))
            else:
                fn = {"eq": pc.equal, "gt": pc.greater, "ge": pc.greater_equal,
                      "lt": pc.less, "le": pc.less_equal}.get(op)
                if fn is None:
                    continue
                m = fn(col, pa.scalar(value).cast(col.type))
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, TypeError):
            continue
        mask = m if mask is None else pc.and_(mask, m)
    return mask


def _read_chunk(uri, chunk: Chunk, columns: List[str], predicates: List[Tuple] = ()) -> Optional[pa.Table]:
    import obstore
    import pyarrow.parquet as pq

    if not chunk.groups:
        return None
    meta = _footer(uri, chunk.file)
    index = {meta.schema.column(i).name: i for i in range(meta.num_columns)}
    present = [c for c in columns if c in index]
    ranges = []
    for r in chunk.groups:
        for c in present:
            col = meta.row_group(r).column(index[c])
            start = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
            ranges.append((start, start + col.total_compressed_size))
    merged = []
    for a, b in sorted(ranges):
        if merged and a - merged[-1][1] < MERGE_GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    data = obstore.get_ranges(_store(uri), unquote(chunk.file.path),
                              starts=[a for a, _ in merged], ends=[b for _, b in merged])
    pieces = [(a, memoryview(d)) for (a, _), d in zip(merged, data)]  # no copy of the fetched bytes
    reader = pq.ParquetFile(_Fetched(chunk.file.size, pieces), metadata=meta)
    # decode a row group at a time and keep only its matching rows, so a query never holds
    # the decoded row groups it reads, only what can survive its WHERE
    kept = []
    for r in chunk.groups:
        t = reader.read_row_group(r, columns=present)
        mask = _row_mask(t, predicates)
        if mask is not None:
            t = t.filter(mask)
        if t.num_rows:
            kept.append(t)
    del reader, pieces, data
    if not kept:
        return None
    return pa.concat_tables(kept) if len(kept) > 1 else kept[0]


def choose_lake(lakes: List[Dict], predicates: List[Tuple],
                columns: Optional[List[str]] = None) -> Tuple[Dict, List[LakeFile]]:
    """The copy whose row groups that can hold a match add up to the fewest bytes.

    File statistics alone cannot tell the copies apart once a filter touches every file
    (125 random root ids hit all ten files of every copy), yet only the copy sorted by the
    filtered column prunes row groups. So the copies sorted by a filtered column, and the
    cheapest copy by file bytes, are costed on their footers' row-group statistics."""
    # cold logs are one round trip chain each; load them side by side
    logs = list(_pool().map(lambda lake: lake_log(lake["uri"]), lakes)) if len(lakes) > 1 \
        else [lake_log(lake["uri"]) for lake in lakes]
    filtered = {c for c, _, _ in predicates}
    candidates = []
    for lake, log in zip(lakes, logs):
        files = [f for f in log.files
                 if all(_may_match(*f.stats.get(c, (None, None)), op, v) for c, op, v in predicates)]
        keyed = 0 if lake.get("partition_by") in filtered else 1
        candidates.append({"lake": lake, "files": files, "keyed": keyed,
                           "file_bytes": sum(f.size for f in files)})
    if len(candidates) == 1:
        return candidates[0]["lake"], candidates[0]["files"]
    by_files = min(candidates, key=lambda c: (c["file_bytes"], c["keyed"], len(c["files"])))
    if not predicates:
        return by_files["lake"], by_files["files"]
    costed = [c for c in candidates if c["keyed"] == 0 or c is by_files]
    cols = sorted(columns) if columns else sorted(filtered)
    for c in costed:
        futures = [_pool().submit(_plan_file, c["lake"]["uri"], f, predicates, cols, None) for f in c["files"]]
        c["rg_bytes"] = sum(fut.result().bytes for fut in futures)
    best = min(costed, key=lambda c: (c["rg_bytes"], c["keyed"], len(c["files"])))
    return best["lake"], best["files"]


@dataclass
class Fetch:
    table: str
    lake: Dict
    files: List[LakeFile]
    columns: List[str]
    predicates: List[Tuple]
    sample: Optional[float]

    def chunks(self):
        """Each candidate file's row groups, footers fetched concurrently, in file order."""
        futures = [_pool().submit(_plan_file, self.lake["uri"], f, self.predicates, self.columns, self.sample)
                   for f in self.files]
        for fut in futures:
            yield fut.result()

    def read(self, chunks: List[Chunk]) -> List[pa.Table]:
        slots = _reads_in_flight()

        def read_one(piece: Chunk):
            with slots:
                return _read_chunk(self.lake["uri"], piece, self.columns, self.predicates)

        pieces = [Chunk(c.file, c.groups[i:i + ROW_GROUPS_PER_READ], 0)
                  for c in chunks for i in range(0, len(c.groups), ROW_GROUPS_PER_READ)]
        futures = [_pool().submit(read_one, piece) for piece in pieces]
        return [t for t in (fut.result() for fut in futures) if t is not None and t.num_rows]


def _empty(log: LakeLog, columns: List[str]) -> pa.Table:
    from deltalake import DeltaTable

    schema = DeltaTable(log.uri).schema().to_arrow()
    schema = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    return pa.schema([schema.field(c) for c in columns]).empty_table()


def _concat(tables: List[pa.Table], log: LakeLog, columns: List[str]) -> pa.Table:
    if not tables:
        return _empty(log, columns)
    return pa.concat_tables(tables, promote_options="permissive")


# ---------------------------------------------------------------------- execution

def _duckdb():
    import duckdb

    con = duckdb.connect()
    con.execute(f"SET threads TO {int(_config('QUERY_LAKE_DUCKDB_THREADS', 4))}")
    return con


def _normalize(table: pa.Table) -> pa.Table:
    """DuckDB's Arrow types to the ones the ADBC conversion expects."""
    fields = []
    for f in table.schema:
        t = f.type
        if pa.types.is_string_view(t) or pa.types.is_large_string(t):
            t = pa.string()
        elif pa.types.is_decimal(t):
            t = pa.float64()
        fields.append(pa.field(f.name, t, f.nullable))
    schema = pa.schema(fields)
    return table if schema.equals(table.schema) else table.cast(schema)


def _run_plan(plan: Plan, lakes: Dict[str, List[Dict]], count: bool) -> Tuple[pa.Table, Dict]:
    budget = int(_config("QUERY_LAKE_MAX_BYTES", 512 * 1024 ** 2))
    fetches = {}
    for table in plan.tables:
        lake, files = choose_lake(lakes[table], plan.predicates[table], sorted(plan.columns[table]))
        fetches[table] = Fetch(table, lake, files, sorted(plan.columns[table]), plan.predicates[table],
                               plan.samples.get(table))
    facts = {"lakes": {t: f.lake["spec_name"] for t, f in fetches.items()}, "row_groups": 0, "bytes": 0}
    con = _duckdb()
    try:
        if plan.streamable:
            (fetch,) = fetches.values()
            return _run_streaming(con, plan, fetch, budget, facts), facts
        data = {}
        for table, fetch in fetches.items():
            chunks = list(fetch.chunks())
            size = sum(c.bytes for c in chunks)
            facts["row_groups"] += sum(len(c.groups) for c in chunks)
            facts["bytes"] += size
            if facts["bytes"] > budget:
                raise LakeQueryError(
                    f"the query would read {facts['bytes'] / 1e9:.1f} GB of {table}, more than the "
                    f"{budget / 1e9:.1f} GB a query may read: narrow it with a filter on an indexed "
                    f"column (a root id, the id or a bounding box)", 413)
            data[table] = _concat(fetch.read(chunks), lake_log(fetch.lake["uri"]), fetch.columns)
        for table, arrow in data.items():
            con.register(table, arrow)
        sql = f"SELECT count(*) AS count FROM ({plan.sql}) AS lake_count" if count else plan.sql
        return _normalize(con.sql(sql).to_arrow_table()), facts
    finally:
        con.close()
        _release_memory()


def _release_memory() -> None:
    """Hand freed query memory back to the OS. Arrow's pool and glibc both keep freed pages,
    so without this a uwsgi worker's rss ratchets up with every lake query it serves
    (measured in the image: 510 -> 623 MB over six identical reads; ~440 MB with this)."""
    try:
        pa.default_memory_pool().release_unused()
    except Exception:
        pass
    global _malloc_trim
    if _malloc_trim is None:
        try:
            import ctypes

            _malloc_trim = ctypes.CDLL("libc.so.6").malloc_trim
        except (OSError, AttributeError):
            _malloc_trim = False  # not glibc (macOS): nothing to trim
    if _malloc_trim:
        _malloc_trim(0)


_malloc_trim = None


def _run_streaming(con, plan: Plan, fetch: Fetch, budget: int, facts: Dict) -> pa.Table:
    """A LIMIT over one table without ordering: read row groups a round at a time and
    stop once enough rows have matched, rather than read every candidate first."""
    import sqlglot

    want = plan.limit + plan.offset
    tree = sqlglot.parse_one(plan.sql, read="duckdb")
    tree.set("offset", None)
    log = lake_log(fetch.lake["uri"])
    got: List[pa.Table] = []
    n = 0
    pending: List[Chunk] = []

    def flush():
        nonlocal n
        tables = fetch.read(pending)
        facts["row_groups"] += sum(len(c.groups) for c in pending)
        facts["bytes"] += sum(c.bytes for c in pending)
        pending.clear()
        if not tables:
            return
        con.register(fetch.table, _concat(tables, log, fetch.columns))
        part = con.sql(tree.limit(want - n).sql(dialect="duckdb")).to_arrow_table()
        con.unregister(fetch.table)
        if part.num_rows:
            got.append(part)
            n += part.num_rows

    groups = 0
    for chunk in fetch.chunks():
        if not chunk.groups:
            continue
        pending.append(chunk)
        groups += len(chunk.groups)
        if facts["bytes"] + sum(c.bytes for c in pending) > budget:
            raise LakeQueryError(
                f"the query would read more than the {budget / 1e9:.1f} GB a query may read: narrow it "
                f"with a filter on an indexed column (a root id, the id or a bounding box)", 413)
        if groups >= STREAM_ROW_GROUPS:
            flush()
            groups = 0
            if n >= want:
                break
    if pending and n < want:
        flush()
    if got:
        result = pa.concat_tables(got, promote_options="permissive")
    else:
        con.register(fetch.table, _empty(log, fetch.columns))
        result = con.sql(tree.limit(0).sql(dialect="duckdb")).to_arrow_table()
    return _normalize(result.slice(plan.offset, plan.limit))


def answer(sql: str, datastack: str, version: int, count: bool = False) -> Tuple[pa.Table, Dict]:
    """The lake answer to a compiled Postgres query of a frozen version."""
    lakes, pcg = version_lakes(datastack, version)
    if not lakes:
        raise LakeQueryError(f"{datastack} v{version} has no lakes", 404)

    def columns(table):
        cols = set()
        for lake in lakes[table]:
            cols |= set(lake_log(lake["uri"]).columns)
        return cols

    plan = plan_query(sql, lakes, pcg, columns, count=count)
    return _run_plan(plan, lakes, count)


def tables_with_lakes(sql: str, datastack: str, version: int) -> bool:
    """Whether every table the SQL reads has a complete lake (routing, not fallback)."""
    import sqlglot
    from sqlglot import exp

    lakes, pcg = version_lakes(datastack, version)
    if not lakes:
        return False
    names = {t.name for t in sqlglot.parse_one(_box_to_between(sql), read="postgres").find_all(exp.Table)}
    return bool(names) and all(_physical_to_logical(n, lakes, pcg) is not None for n in names)


def execute(engine, query, get_count: bool, on_postgres, to_dataframe):
    """Run a compiled query on the lakes or Postgres as the mode says (see module docstring)."""
    import pandas as pd

    current = mode()
    target = frozen_target(engine) if current != "off" else None
    if target is None:
        return on_postgres()
    statement = str(query.statement.compile(engine, compile_kwargs={"literal_binds": True}))
    try:
        routed = tables_with_lakes(statement, *target)
    except Exception:
        logger.exception("lake query: could not read the catalog of %s v%s", *target)
        routed = False
    if not routed:
        logger.info("lake query: %s v%s has no lake for this query, using Postgres", *target)
        return on_postgres()

    def lake():
        t0 = time.perf_counter()
        table, facts = answer(statement, *target, count=get_count)
        df = pd.DataFrame({"count": [int(table.column(0)[0].as_py())]}) if get_count else to_dataframe(table)
        facts["seconds"] = round(time.perf_counter() - t0, 4)
        facts["rows"] = len(df)
        return df, facts

    if current == "on":
        try:
            df, facts = lake()
        except LakeQueryError as e:
            from flask import abort

            logger.warning("lake query of %s v%s refused: %s", *target, e)
            abort(e.status, f"lake query: {e}")
        logger.info("lake query of %s v%s: %s", *target, facts)
        return df

    # shadow: Postgres answers, the lake is measured against it
    t0 = time.perf_counter()
    result = on_postgres()
    pg_seconds = time.perf_counter() - t0
    try:
        df, facts = lake()
        facts["postgres_seconds"] = round(pg_seconds, 4)
        facts["match"] = same_rows(df, result)
        logger.info("lake shadow of %s v%s: %s", *target, facts)
    except Exception as e:
        logger.warning("lake shadow of %s v%s failed: %s: %s", *target, type(e).__name__, e)
    return result


def same_rows(lake_df, pg_df) -> bool:
    """Whether two results hold the same rows, ignoring order and the columns lakes
    leave out (Postgres returns rows in no particular order)."""
    cols = [c for c in pg_df.columns if c not in CRUD_COLUMNS]
    if sorted(lake_df.columns) != sorted(cols) or len(lake_df) != len(pg_df):
        return False
    if not len(cols):
        return True
    a = lake_df[cols].sort_values(cols).reset_index(drop=True)
    b = pg_df[cols].sort_values(cols).reset_index(drop=True)
    try:
        import pandas as pd

        pd.testing.assert_frame_equal(a, b, check_dtype=True)
        return True
    except AssertionError:
        return False
