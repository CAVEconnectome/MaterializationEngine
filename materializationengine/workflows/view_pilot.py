"""The view pilot: build a frozen version's views from Postgres, DuckDB and Daft, and compare.

Each engine writes the view's rows to a flat Delta lake under
<DELTALAKE_OUTPUT_BUCKET>/<datastack>/v<version>/_pilot/<run_id>/<engine>/<view>, outside
what deltalake_catalog lists, so a pilot lake is never served or registered:

- postgres: SELECT * FROM the view in the frozen database (points as ST_X/Y/Z), streamed
  through ADBC, as the view export reads a view.
- duckdb: lake_views.lake_sql over the version's lakes (one copy of each table, from the
  catalog), on one node.
- daft: the same SQL with daft.sql, on the Ray cluster when one is running; Daft writes
  the lake itself.

Every lake is then summarised the same way (rows, and an order-independent checksum of
its rows with floats rounded, as engines sum in different orders), so agreeing checksums
mean the engines built the same view. Timings are for reading, computing and writing the
flat lake.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from materializationengine.blueprints.client import lake_views as lv

logger = logging.getLogger(__name__)

ENGINES = ("postgres", "duckdb", "daft")
FLOAT_DIGITS = 3


def view_definitions(engine) -> Tuple[Dict[str, str], Dict[str, List[Dict]]]:
    """Every view's definition (pg_get_viewdef) and columns ({"name", "type"}), by name."""
    rows = engine.execute("""
        SELECT c.relname, pg_get_viewdef(c.oid, true),
               (SELECT json_agg(json_build_object('name', a.attname, 'type', format_type(a.atttypid, a.atttypmod))
                                ORDER BY a.attnum)
                FROM pg_attribute a WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')
          AND c.relname NOT IN ('geography_columns', 'geometry_columns', 'raster_columns', 'raster_overviews')
    """).fetchall()
    defs, cols = {}, {}
    for name, definition, columns in rows:
        defs[name] = definition
        cols[name] = json.loads(columns) if isinstance(columns, str) else columns
    return defs, cols


def version_lakes(aligned_volume: str, datastack: str, version: int) -> Dict[str, Dict]:
    """One complete lake per table: the smallest copy (every copy holds every row)."""
    from materializationengine import deltalake_catalog as cat

    best = {}
    for row in cat.list_exports(aligned_volume, datastack, version):
        current = best.get(row["table_name"])
        if current is None or (row.get("bytes") or 0) < (current.get("bytes") or 0):
            best[row["table_name"]] = row
    return best


def lake_columns(uri: str) -> List[str]:
    from deltalake import DeltaTable

    return [f.name for f in DeltaTable(uri).schema().fields]


def postgres_select(view: str, columns: List[Dict]) -> str:
    out = []
    for c in columns:
        n = c["name"]
        if c["type"].startswith("geometry"):
            out += [f'ST_X("{n}")::int AS "{n}_x"', f'ST_Y("{n}")::int AS "{n}_y"', f'ST_Z("{n}")::int AS "{n}_z"']
        else:
            out.append(f'"{n}"')
    return f'SELECT {", ".join(out)} FROM "{view}"'


def _write(reader: pa.RecordBatchReader, uri: str) -> None:
    from deltalake import write_deltalake

    write_deltalake(uri, reader, mode="error")


def build_postgres(connection_string: str, view: str, columns: List[Dict], out_uri: str) -> None:
    import adbc_driver_postgresql.dbapi as pg_dbapi

    with pg_dbapi.connect(connection_string) as conn, conn.cursor() as cur:
        cur.execute(postgres_select(view, columns))
        _write(cur.fetch_record_batch(), out_uri)


def build_duckdb(sql: str, lakes: Dict[str, str], out_uri: str, threads: Optional[int] = None,
                 memory_limit: Optional[str] = None) -> None:
    import duckdb
    from deltalake import DeltaTable

    con = duckdb.connect()
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    for table, uri in lakes.items():
        con.register(table, DeltaTable(uri).to_pyarrow_dataset())
    _write(con.sql(sql).fetch_record_batch(), out_uri)


def build_daft(sql: str, lakes: Dict[str, str], out_uri: str) -> None:
    import daft
    from daft.session import Session

    sess = Session()
    for table, uri in lakes.items():
        sess.create_temp_table(table, daft.read_deltalake(uri))
    sess.sql(sql).write_deltalake(out_uri, mode="error")


def summarize(uri: str) -> Dict:
    """Rows and an order-independent checksum: integers as BIGINT, floats rounded to
    FLOAT_DIGITS, columns in name order, hashed per row and summed."""
    import duckdb
    from deltalake import DeltaTable

    dt = DeltaTable(uri)
    dataset = dt.to_pyarrow_dataset()
    con = duckdb.connect()
    con.register("t", dataset)
    terms = []
    for f in sorted(dataset.schema, key=lambda f: f.name):
        t, c = f.type, f'"{f.name}"'
        if pa.types.is_integer(t) or (pa.types.is_decimal(t) and t.scale == 0):
            terms.append(f"{c}::BIGINT")
        elif pa.types.is_floating(t) or pa.types.is_decimal(t):
            terms.append(f"round({c}::DOUBLE, {FLOAT_DIGITS})")
        else:
            terms.append(c)
    rows, checksum = con.execute(f"SELECT count(*), sum(hash({', '.join(terms)})::HUGEINT) FROM t").fetchone()
    files = dt.get_add_actions(flatten=True)
    size = sum(pa.table(files).column("size_bytes").to_pylist()) if pa.table(files).num_rows else 0
    return {"rows": int(rows), "checksum": str(checksum), "bytes": int(size)}


def plan_view(view: str, defs: Dict[str, str], cols: Dict[str, List[Dict]], lakes: Dict[str, Dict],
              pcg: str) -> lv.LakeView:
    lake_cols = {t: lake_columns(row["uri"]) for t, row in lakes.items()}
    return lv.lake_sql(view, defs, lake_cols, pcg, lv.point_names(cols, lake_cols))


def run_one(engine: str, view: str, out_uri: str, *, plan: Optional[lv.LakeView] = None,
            lakes: Optional[Dict[str, Dict]] = None, connection_string: Optional[str] = None,
            columns: Optional[List[Dict]] = None, duckdb_threads: Optional[int] = None,
            duckdb_memory: Optional[str] = None) -> Dict:
    """Build one view with one engine into out_uri; the result record (never raises)."""
    result = {"engine": engine, "view": view, "uri": out_uri}
    try:
        started = time.perf_counter()
        if engine == "postgres":
            build_postgres(connection_string, view, columns, out_uri)
        else:
            uris = {t: lakes[t]["uri"] for t in plan.lakes}
            if engine == "duckdb":
                build_duckdb(plan.sql, uris, out_uri, duckdb_threads, duckdb_memory)
            elif engine == "daft":
                build_daft(plan.sql, uris, out_uri)
            else:
                raise ValueError(f"unknown engine {engine!r}")
        result["seconds"] = round(time.perf_counter() - started, 3)
        result.update(summarize(out_uri))
    except Exception as e:
        logger.exception("%s: %s failed", view, engine)
        result["error"] = f"{type(e).__name__}: {e}"[:2000]
    return result


def compare(results: List[Dict]) -> Dict[str, Dict]:
    """Per view: whether every engine that finished agrees (rows and checksum)."""
    out = {}
    for view in sorted({r["view"] for r in results}):
        done = [r for r in results if r["view"] == view and "checksum" in r]
        sums = {(r["rows"], r["checksum"]) for r in done}
        out[view] = {"agree": len(sums) == 1 and len(done) > 1, "engines": sorted({r["engine"] for r in done}),
                     "failed": sorted(r["engine"] for r in results if r["view"] == view and "error" in r)}
    return out
