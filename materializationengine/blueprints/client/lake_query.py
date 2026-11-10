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

Planning reads only each copy's planning index (lake_index: file and row-group byte
ranges and statistics, stored next to the lake, a few MB for minnie's synapse copies),
and only of the copies laid out by a filtered column. A read fetches a file's footer in
the same request as its column chunks; raw footers are cached per process within
QUERY_LAKE_FOOTER_CACHE_MB, indexes up to QUERY_LAKE_INDEX_CACHE lakes. A query that
would fetch more than QUERY_LAKE_MAX_BYTES (default 512 MiB) of column chunks is refused
with a request to narrow it, as Postgres's statement timeout refuses an expensive one.
"""

import concurrent.futures as cf
import io
import logging
import os
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from urllib.parse import unquote

import numpy as np
import pyarrow as pa

from materializationengine.blueprints.client import lake_index as li

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


def _match(lo: np.ndarray, hi: np.ndarray, op: str, value) -> np.ndarray:
    """_may_match over arrays of minimums and maximums (an unknown bound is its type's extreme)."""
    everything = np.ones(len(lo), bool)
    values = list(value) if op in ("in", "between") else [value]
    ints = np.issubdtype(lo.dtype, np.integer)
    if any(isinstance(v, bool) or not isinstance(v, (int, float, np.integer, np.floating)) for v in values):
        return everything
    if ints and not all(isinstance(v, (int, np.integer)) for v in values):
        return everything  # a fractional literal against integers: let DuckDB decide
    try:
        if op == "in":
            v = np.unique(np.array(values, dtype=lo.dtype))
            if not len(v):
                return np.zeros(len(lo), bool)
            i = np.searchsorted(v, lo, side="left")
            return (i < len(v)) & (v[np.minimum(i, len(v) - 1)] <= hi)
        if op == "between":
            a, b = (np.array(x, dtype=lo.dtype) for x in value)
            return (hi >= a) & (lo <= b)
        x = np.array(value, dtype=lo.dtype)
        if op == "eq":
            return (lo <= x) & (hi >= x)
        if op == "gt":
            return hi > x
        if op == "ge":
            return hi >= x
        if op == "lt":
            return lo < x
        if op == "le":
            return lo <= x
    except (OverflowError, TypeError, ValueError):
        pass
    return everything


@dataclass
class Chunk:
    file: int           # the file's number in the lake's index
    groups: List[int]   # row groups of that file to read, by their number in the file
    bytes: int


def select(index: "li.LakeIndex", predicates: List[Tuple], columns: List[str],
           sample: Optional[float] = None) -> List[Chunk]:
    """The row groups, per file in file order, that can hold a row matching every predicate,
    from the index's file and row-group statistics alone (no I/O)."""
    keep_files = np.ones(len(index.paths), bool)
    for c, op, v in predicates:
        if c in index.file_stats:
            keep_files &= _match(*index.file_stats[c], op, v)
    keep = keep_files[index.rg_file]
    for c, op, v in predicates:
        if c in index.rg_stats and keep.any():
            keep &= _match(*index.rg_stats[c], op, v)
    if sample is not None:
        keep &= np.random.random(len(keep)) * 100.0 < sample
    positions = [index.column_pos[c] for c in columns if c in index.column_pos]
    rows = np.nonzero(keep)[0]
    if not len(rows):
        return []
    rg_bytes = index.col_sizes[rows][:, positions].astype(np.int64).sum(axis=1) if positions \
        else np.zeros(len(rows), np.int64)
    files = index.rg_file[rows]
    cuts = np.nonzero(np.diff(files))[0] + 1
    chunks = []
    for part, part_bytes in zip(np.split(np.arange(len(rows)), cuts), np.split(rg_bytes, cuts)):
        chunks.append(Chunk(int(files[part[0]]), index.rg_index[rows[part]].tolist(), int(part_bytes.sum())))
    return chunks


class _BytesLRU:
    """Raw footers by (lake, path), least recently used dropped first, within a byte budget."""

    def __init__(self, budget_param: str, default_mb: int):
        self.budget_param, self.default_mb = budget_param, default_mb
        self.items: "OrderedDict" = OrderedDict()
        self.size = 0
        self.lock = threading.Lock()

    def get(self, key) -> Optional[bytes]:
        with self.lock:
            if key in self.items:
                self.items.move_to_end(key)
                return self.items[key]
        return None

    def put(self, key, value: bytes) -> None:
        budget = int(float(_config(self.budget_param, self.default_mb)) * 1024 ** 2)
        if len(value) > budget:
            return
        with self.lock:
            if key in self.items:
                return
            self.items[key] = value
            self.size += len(value)
            while self.size > budget:
                _, old = self.items.popitem(last=False)
                self.size -= len(old)

    def clear(self):
        with self.lock:
            self.items.clear()
            self.size = 0


# Per process, and uwsgi runs many: a raw footer of minnie's synapse copies is ~220 KB
# (parsed, ~1.6 MB, so only the raw bytes are kept and parsed again per read: 1.5 ms).
_footers = _BytesLRU("QUERY_LAKE_FOOTER_CACHE_MB", 16)


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


def _footer_meta(index: "li.LakeIndex", f: int, tail: Optional[bytes] = None):
    """A file's parsed footer, from `tail` or the raw-footer cache or one GET (then cached)."""
    key = (index.uri, index.paths[f])
    size = int(index.sizes[f])
    if tail is None:
        tail = _footers.get(key)
    if tail is None:
        tail = li.fetch_footer(index.uri, index.paths[f], size)
    _footers.put(key, tail)
    return li.parse_footer(size, tail), tail


def _check(index: "li.LakeIndex", f: int, meta, groups: List[int]) -> None:
    """The footer agrees with the index (a lake rewritten under the same uri would not)."""
    first = int(index.rg_first[f])
    n = int(index.rg_first[f + 1]) - first
    bad = meta.num_row_groups != n
    if not bad and groups:
        col = meta.row_group(groups[0]).column(0)
        start = col.dictionary_page_offset if col.has_dictionary_page else col.data_page_offset
        bad = start != int(index.rg_start[first + groups[0]])
    if bad:
        li.cache.drop(index.uri)
        raise LakeQueryError(f"the planning index of {index.uri} does not match {index.paths[f]}; "
                             f"it will be rebuilt, retry the query", 503)


def _read_chunk(index: "li.LakeIndex", chunk: Chunk, columns: List[str], predicates: List[Tuple] = (),
                meta=None) -> Optional[pa.Table]:
    """Fetch and decode the chunk's column chunks, keeping only rows that can match. Without
    `meta`, the file's footer is fetched in the same request (it is needed to decode)."""
    import obstore
    import pyarrow.parquet as pq

    if not chunk.groups:
        return None
    f = chunk.file
    size = int(index.sizes[f])
    positions = [index.column_pos[c] for c in columns if c in index.column_pos]
    present = [index.columns[i] for i in positions]
    rows = int(index.rg_first[f]) + np.asarray(chunk.groups, np.int64)
    merged = []
    for a, b in sorted(index.chunk_ranges(rows, positions)):
        if merged and a - merged[-1][1] < MERGE_GAP:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    tail = None
    if meta is None:
        tail = _footers.get((index.uri, index.paths[f]))
    footer_range = [] if meta is not None or tail is not None else [(size - int(index.footer_lens[f]), size)]
    ranges = merged + footer_range
    data = obstore.get_ranges(li.store(index.uri), unquote(index.paths[f]),
                              starts=[a for a, _ in ranges], ends=[b for _, b in ranges])
    if footer_range:
        tail, data = bytes(data[-1]), data[:-1]
    if meta is None:
        meta, _ = _footer_meta(index, f, tail)
    _check(index, f, meta, chunk.groups)
    pieces = [(a, memoryview(d)) for (a, _), d in zip(merged, data)]  # no copy of the fetched bytes
    reader = pq.ParquetFile(li.Fetched(size, pieces), metadata=meta)
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


def _lake_columns(lake: Dict) -> Optional[set]:
    cols = lake.get("columns")
    if not cols:
        return None
    return {c["name"] if isinstance(c, dict) else str(c) for c in cols}


def choose_lake(lakes: List[Dict], predicates: List[Tuple], columns: List[str],
                sample: Optional[float] = None) -> Tuple[Dict, "li.LakeIndex", List[Chunk], str]:
    """The copy whose row groups that can hold a match add up to the fewest bytes, with them.

    Only the copies laid out by a filtered column can prune row groups (125 random root ids
    hit every file of every copy, yet only the copy sorted by that column has narrow row
    groups), so only their indexes are loaded and costed. A filter no copy is laid out by
    costs them all; no filter at all reads the smallest copy."""
    needed = set(columns)
    usable = [l for l in lakes if _lake_columns(l) is None or needed <= _lake_columns(l)] or lakes
    filtered = {c for c, _, _ in predicates}
    keyed = [l for l in usable if li.key_columns(l) & filtered]
    if not predicates:
        candidates = [min(usable, key=lambda l: (l.get("bytes") or float("inf"), l.get("spec_name") or ""))]
    else:
        candidates = keyed or usable
    loaded = list(_pool().map(li.get, candidates)) if len(candidates) > 1 else [li.get(candidates[0])]
    best = None
    for lake, (index, how) in zip(candidates, loaded):
        chunks = select(index, predicates, columns, sample)
        cost = (sum(c.bytes for c in chunks), 0 if lake in keyed else 1, len(chunks))
        if best is None or cost < best[0]:
            best = (cost, lake, index, chunks, how)
    return best[1], best[2], best[3], best[4]


@dataclass
class Fetch:
    table: str
    lake: Dict
    index: "li.LakeIndex"
    chunks: List[Chunk]
    columns: List[str]
    predicates: List[Tuple]

    def read(self, chunks: List[Chunk]) -> List[pa.Table]:
        slots = _reads_in_flight()
        pieces = [Chunk(c.file, c.groups[i:i + ROW_GROUPS_PER_READ], 0)
                  for c in chunks for i in range(0, len(c.groups), ROW_GROUPS_PER_READ)]
        per_file: Dict[int, int] = {}
        for p in pieces:
            per_file[p.file] = per_file.get(p.file, 0) + 1
        # a file read in several pieces: its footer is fetched and parsed once, alongside
        # them (submitted first, outside the read slots, so a piece waiting on it cannot
        # keep it from running); a file read in one piece fetches it with its chunks
        metas = {f: _pool().submit(_footer_meta, self.index, f) for f, n in per_file.items() if n > 1}

        def read_one(piece: Chunk):
            with slots:
                meta = metas[piece.file].result()[0] if piece.file in metas else None
                return _read_chunk(self.index, piece, self.columns, self.predicates, meta)

        futures = [_pool().submit(read_one, piece) for piece in pieces]
        return [t for t in (fut.result() for fut in futures) if t is not None and t.num_rows]


def _empty(index: "li.LakeIndex", columns: List[str]) -> pa.Table:
    return pa.schema([index.schema.field(c) for c in columns if c in index.column_pos]).empty_table()


def _concat(tables: List[pa.Table], index: "li.LakeIndex", columns: List[str]) -> pa.Table:
    if not tables:
        return _empty(index, columns)
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
    t0 = time.perf_counter()
    fetches, found = {}, {}
    for table in plan.tables:
        cols = sorted(plan.columns[table])
        lake, index, chunks, found[table] = choose_lake(lakes[table], plan.predicates[table], cols,
                                                        plan.samples.get(table))
        fetches[table] = Fetch(table, lake, index, chunks, cols, plan.predicates[table])
    facts = {"lakes": {t: f.lake["spec_name"] for t, f in fetches.items()}, "index": found,
             "plan_seconds": round(time.perf_counter() - t0, 4), "row_groups": 0, "bytes": 0,
             "_used": {t: f.lake for t, f in fetches.items()}}
    con = _duckdb()
    try:
        if plan.streamable:
            (fetch,) = fetches.values()
            return _run_streaming(con, plan, fetch, budget, facts), facts
        data = {}
        for table, fetch in fetches.items():
            chunks = fetch.chunks
            size = sum(c.bytes for c in chunks)
            facts["row_groups"] += sum(len(c.groups) for c in chunks)
            facts["bytes"] += size
            if facts["bytes"] > budget:
                raise LakeQueryError(
                    f"the query would read {facts['bytes'] / 1e9:.1f} GB of {table}, more than the "
                    f"{budget / 1e9:.1f} GB a query may read: narrow it with a filter on an indexed "
                    f"column (a root id, the id or a bounding box)", 413)
            data[table] = _concat(fetch.read(chunks), fetch.index, fetch.columns)
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
        con.register(fetch.table, _concat(tables, fetch.index, fetch.columns))
        part = con.sql(tree.limit(want - n).sql(dialect="duckdb")).to_arrow_table()
        con.unregister(fetch.table)
        if part.num_rows:
            got.append(part)
            n += part.num_rows

    groups = 0
    for chunk in fetch.chunks:
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
        con.register(fetch.table, _empty(fetch.index, fetch.columns))
        result = con.sql(tree.limit(0).sql(dialect="duckdb")).to_arrow_table()
    return _normalize(result.slice(plan.offset, plan.limit))


def answer(sql: str, datastack: str, version: int, count: bool = False) -> Tuple[pa.Table, Dict]:
    """The lake answer to a compiled Postgres query of a frozen version."""
    lakes, pcg = version_lakes(datastack, version)
    if not lakes:
        raise LakeQueryError(f"{datastack} v{version} has no lakes", 404)

    def columns(table):
        # the catalog lists each lake's columns; an older row without them needs its index
        cols = set()
        for lake in lakes[table]:
            cols |= _lake_columns(lake) or set(li.get(lake)[0].columns)
        return cols

    plan = plan_query(sql, lakes, pcg, columns, count=count)
    table, facts = _run_plan(plan, lakes, count)
    used = facts.pop("_used")
    for name, lake in used.items():
        _pool().submit(li.record_use, datastack, name, [lake])
    return table, facts


def prewarm(datastack: str, versions: List[int]) -> List[str]:
    """Fill a new worker's lake planning indexes (see lake_index.prewarm) and load the
    libraries a lake query uses, so its first lake request is not a cold one."""
    import duckdb  # noqa: F401
    import obstore  # noqa: F401
    import pyarrow.parquet  # noqa: F401
    import sqlglot

    sqlglot.parse_one("SELECT t.a FROM t WHERE t.a IN (1, 2)", read="postgres").sql(dialect="duckdb")
    _duckdb().close()
    budget = float(_config("QUERY_LAKE_PREWARM_SECONDS", 10))
    return li.prewarm(datastack, versions, lambda v: version_lakes(datastack, v)[0], budget)


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
