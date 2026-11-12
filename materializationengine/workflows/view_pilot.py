"""The view pilot: build a frozen version's views from Postgres, DuckDB and Daft, and compare.

Each engine writes the view's rows to a flat Delta lake under
<DELTALAKE_OUTPUT_BUCKET>/<datastack>/v<version>/_pilot/<run_id>/<engine>/<view>, outside
what deltalake_catalog lists, so a pilot lake is never served or registered:

- postgres: SELECT * FROM the view in the frozen database (points as ST_X/Y/Z), streamed
  through ADBC, as the view export reads a view.
- duckdb: lake_views.lake_sql over the version's lakes (one copy of each table, from the
  catalog), on one node, with DuckDB's own Parquet reader on the lakes' files; duckdb_arrow
  the same through pyarrow datasets, which on ltv7 spent most of its time reading. Both
  record DuckDB's seconds per operator type.
- daft: the same SQL with daft.sql, on the Ray cluster when one is running.

Each engine writes Snappy Parquet its own fastest way -- Daft from every worker at once,
DuckDB a file per thread on its node (then uploaded in parallel), Postgres's rows through
write_deltalake -- and the files are committed as a Delta table from their footers, read in
parallel (_to_delta). A single
writer would funnel a large view (minnie's connections) through one process and hide how
an engine scales out. results record the convert's seconds, and DuckDB's upload's.

Every lake is then summarised the same way (rows, and an order-independent checksum of
its rows with floats rounded, as engines sum in different orders), so agreeing checksums
mean the engines built the same view. "seconds" covers reading, computing, writing and
converting the flat lake.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import pyarrow as pa

from materializationengine.blueprints.client import lake_views as lv

logger = logging.getLogger(__name__)

ENGINES = ("postgres", "duckdb", "daft")
EXTRA_ENGINES = ("duckdb_arrow",)  # DuckDB reading the lakes through pyarrow, to compare read paths
FLOAT_DIGITS = 3
DUCKDB_EXTENSIONS = "/opt/duckdb_extensions"


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


def _gcs_token() -> str:
    """A bearer token from the pod's own credentials (GOOGLE_APPLICATION_CREDENTIALS)."""
    import google.auth
    import google.auth.transport.requests

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_only"])
    credentials.refresh(google.auth.transport.requests.Request())
    return credentials.token


def _operator_seconds(profile: Dict) -> Dict[str, float]:
    """Seconds per operator type (summed over threads) from DuckDB's JSON profile, plus
    the bytes it read (io.total_bytes_read, as "bytes_read")."""
    out: Dict[str, float] = {}

    def walk(node):
        name = node.get("type") or node.get("operator_type") or node.get("operator_name")
        timing = node.get("timing", node.get("operator_timing"))
        if name and timing is not None:
            out[name] = round(out.get(name, 0.0) + float(timing), 3)
        for child in node.get("children") or []:
            walk(child)

    for root in profile.get("operator") or [profile]:
        walk(root)
    out = dict(sorted(out.items(), key=lambda kv: -kv[1]))
    if (profile.get("io") or {}).get("total_bytes_read") is not None:
        out["bytes_read"] = int(profile["io"]["total_bytes_read"])
    return out


def _attach_lakes(con, lakes: Dict[str, str]) -> None:
    """Each lake as a DuckDB view over its files, read by DuckDB's own Parquet reader
    (httpfs, with a token from the pod's credentials, for gs://)."""
    import os

    from deltalake import DeltaTable

    files = {t: DeltaTable(uri).file_uris() for t, uri in lakes.items()}
    if any(f.startswith("gs://") for fs in files.values() for f in fs):
        if os.path.isdir(DUCKDB_EXTENSIONS):  # baked into the image (Dockerfile)
            con.execute(f"SET extension_directory = '{DUCKDB_EXTENSIONS}'")
        else:
            con.execute("INSTALL httpfs")
        con.execute("LOAD httpfs")
        con.execute(f"CREATE OR REPLACE SECRET gcs (TYPE gcs, BEARER_TOKEN '{_gcs_token()}')")
    for table, fs in files.items():
        listed = ", ".join("'" + f.replace("'", "''") + "'" for f in fs)
        con.execute(f'CREATE VIEW "{table}" AS SELECT * FROM read_parquet([{listed}])')


def _to_delta(uri: str, paths: Optional[List[str]] = None, threads: int = 32) -> float:
    """Make Parquet files written under uri a Delta table; its seconds.

    paths: the files to commit, as the writer reported them. Pass them whenever the writer
    can retry: a Daft task lost with its worker is run again and writes new files, and the
    lost task's finished files stay behind, so committing everything under uri counted
    some rows twice (minnie's connections_with_nuclei: 399M rows for 303M, after a worker
    was killed mid-build). Without paths, every file under uri (a writer that never
    retries, e.g. DuckDB's COPY).

    One listing for the files' sizes, every footer read in parallel, and one commit of
    their add actions (with the statistics Daft's own Delta writer records).
    convert_to_deltalake reads the footers one at a time: 12-17 s for minnie's
    connections_with_nuclei whether 4 or 16 workers wrote it."""
    from concurrent.futures import ThreadPoolExecutor

    import fsspec
    import pyarrow.parquet as pq
    from daft.io.delta_lake.delta_lake_write import make_deltalake_add_action
    from deltalake import Schema
    from deltalake.transaction import create_table_with_add_actions

    started = time.perf_counter()
    fs, root = fsspec.core.url_to_fs(uri)
    listed = {f["name"].rsplit("/", 1)[-1]: f for f in fs.ls(root, detail=True)
              if f["type"] == "file" and f["name"].endswith(".parquet")}
    if paths is None:
        files = list(listed.values())
    else:
        names = [p.rstrip("/").rsplit("/", 1)[-1] for p in paths]
        missing = [n for n in names if n not in listed]
        if missing:
            raise ValueError(f"the writer reported {len(missing)} file(s) not under {uri}, e.g. {missing[:3]}")
        files = [listed[n] for n in dict.fromkeys(names)]
    if not files:
        raise ValueError(f"no Parquet files under {uri}")

    def footer(f):
        with fs.open(f["name"], "rb", block_size=1 << 20) as fh:
            return pq.ParquetFile(fh).metadata

    with ThreadPoolExecutor(min(threads, len(files))) as pool:
        metadata = list(pool.map(footer, files))
    actions = [make_deltalake_add_action(f["name"].rsplit("/", 1)[-1], md, int(f["size"]), {})
               for f, md in zip(files, metadata)]
    schema = Schema.from_arrow(metadata[0].schema.to_arrow_schema())
    create_table_with_add_actions(uri, schema, actions, mode="error")
    return round(time.perf_counter() - started, 3)


def build_duckdb(sql: str, lakes: Dict[str, str], out_uri: str, threads: Optional[int] = None,
                 memory_limit: Optional[str] = None, reader: str = "native") -> Dict:
    """DuckDB on one node. reader "native": DuckDB's own Parquet reader on each lake's files;
    "arrow": the lakes as pyarrow datasets (DeltaTable.to_pyarrow_dataset). The result is
    written a file per thread (COPY ... PER_THREAD_OUTPUT), to local disk first for gs://
    and then uploaded in parallel. Returns DuckDB's seconds per operator type, and the
    upload's and convert's seconds."""
    import os
    import tempfile

    import duckdb
    from deltalake import DeltaTable

    scratch = tempfile.mkdtemp(prefix="view_pilot_")
    con = duckdb.connect()
    con.execute(f"SET temp_directory = '{os.path.join(scratch, 'spill')}'")  # spill beyond memory_limit
    if threads:
        con.execute(f"SET threads = {int(threads)}")
    if memory_limit:
        con.execute(f"SET memory_limit = '{memory_limit}'")
    if reader == "native":
        _attach_lakes(con, lakes)
    elif reader == "arrow":
        for table, uri in lakes.items():
            con.register(table, DeltaTable(uri).to_pyarrow_dataset())
    else:
        raise ValueError(f"unknown DuckDB reader {reader!r}")
    remote = out_uri.startswith("gs://")
    target = os.path.join(scratch, "out") if remote else out_uri
    profile_path = os.path.join(scratch, "profile.json")
    con.execute("SET enable_profiling = 'json'")
    con.execute(f"SET profiling_output = '{profile_path}'")
    con.execute(f"COPY ({sql}) TO '{target}' (FORMAT parquet, COMPRESSION snappy, PER_THREAD_OUTPUT, "
                f"ROW_GROUP_SIZE 1048576)")
    con.execute("SET enable_profiling = 'no_output'")
    try:
        with open(profile_path) as fh:
            out = {"operator_seconds": _operator_seconds(json.load(fh))}
    except (OSError, ValueError):
        out = {"operator_seconds": {}}
    if remote:
        import gcsfs

        started = time.perf_counter()
        local = [os.path.join(target, f) for f in sorted(os.listdir(target))]
        gcsfs.GCSFileSystem().put(local, [f"{out_uri}/{os.path.basename(f)}" for f in local])
        out["upload_seconds"] = round(time.perf_counter() - started, 3)
    out["convert_seconds"] = _to_delta(out_uri)
    return out


def build_daft(sql: str, lakes: Dict[str, str], out_uri: str) -> Dict:
    """Daft computes and writes Snappy Parquet from every worker (on the Ray cluster when one
    is running; its write_deltalake can't compress, so Parquet, then _to_delta of exactly the
    files Daft reports writing: a retried task's orphans are left out). Returns the commit's
    seconds and the files committed."""
    import daft
    from daft.session import Session

    sess = Session()
    for table, uri in lakes.items():
        sess.create_temp_table(table, daft.read_deltalake(uri))
    written = sess.sql(sql).write_parquet(out_uri, compression="snappy").to_pydict()["path"]
    return {"convert_seconds": _to_delta(out_uri, paths=written), "files": len(written)}


def summarize(uri: str) -> Dict:
    """Rows and an order-independent checksum: integers as BIGINT, floats rounded to
    FLOAT_DIGITS, columns in name order, hashed per row and summed."""
    import duckdb
    from deltalake import DeltaTable

    dt = DeltaTable(uri)
    con = duckdb.connect()
    _attach_lakes(con, {"t": uri})
    terms = []
    for f in sorted(dt.to_pyarrow_dataset().schema, key=lambda f: f.name):
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
            if engine in ("duckdb", "duckdb_arrow"):
                result.update(build_duckdb(
                    plan.sql, uris, out_uri, duckdb_threads, duckdb_memory,
                    reader="arrow" if engine == "duckdb_arrow" else "native"))
            elif engine == "daft":
                result.update(build_daft(plan.sql, uris, out_uri))
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
