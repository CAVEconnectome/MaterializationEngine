"""Which frozen tables have Delta Lake copies: one row per lake, in the aligned volume database.

Until this, nothing durable recorded an export -- only redis progress keys (24 h) and
the lakes themselves -- so neither the api nor a client could ask "does v2253's
synapses_pni_2 have a post_pt_root_id copy, and is it complete?". The catalog answers
that, and is what a lake-backed query path and the expiry cleanup will read.

It lives beside AnalysisVersion in the aligned volume database, not in the frozen
database, so it outlives the version's database. MaterializationEngine owns the table
and creates it when first needed (like the upload state database), so no
DynamicAnnotationDB migration is involved.

Lifecycle, written by run_deltalake_export: a row per output spec is `writing` before
any data is streamed, `complete` once the lake exists, its optimize has finished and its
row count matches the source (MaterializedMetadata for tables, the streamed count for
views), else `failed` with the reason. A later step marks rows `deleted` when the
version expires, before the files are removed, so queries stop using a lake first.

Catalog writes never fail an export: a lake missing from the catalog is only invisible
to the catalog's readers, which is safe; an export failed by a catalog hiccup is not.
"""

import json
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional

import sqlalchemy as sa

logger = logging.getLogger(__name__)

TABLE_NAME = "deltalake_exports"
STATUSES = ("writing", "complete", "failed", "deleted")

_metadata = sa.MetaData()
catalog = sa.Table(
    TABLE_NAME,
    _metadata,
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
    sa.Column("datastack", sa.String(100), nullable=False),
    sa.Column("version", sa.Integer, nullable=False),
    sa.Column("table_name", sa.String(100), nullable=False),
    sa.Column("spec_name", sa.String(200), nullable=False),
    sa.Column("uri", sa.Text, nullable=False),
    sa.Column("partition_by", sa.String(100)),
    sa.Column("spec", sa.JSON),  # the full DeltaLakeOutputSpec, as resolved
    sa.Column("status", sa.String(20), nullable=False),
    sa.Column("row_count", sa.BigInteger),
    sa.Column("bytes", sa.BigInteger),
    sa.Column("files", sa.Integer),
    sa.Column("columns", sa.JSON),  # [{"name": ..., "type": ...}] from the lake's schema
    sa.Column("error", sa.Text),
    sa.Column("job_id", sa.String(200)),
    sa.Column("exporter_version", sa.String(50)),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("completed_at", sa.DateTime(timezone=True)),
    sa.Column("deleted_at", sa.DateTime(timezone=True)),
    sa.UniqueConstraint("datastack", "version", "table_name", "spec_name", name="uq_deltalake_exports_lake"),
    sa.Index("ix_deltalake_exports_version", "datastack", "version"),
)

_ensured = set()
_ensure_lock = threading.Lock()


def _engine(aligned_volume: str):
    from materializationengine.database import db_manager

    engine = db_manager.get_engine(aligned_volume)
    key = str(engine.url)
    if key not in _ensured:
        with _ensure_lock:
            if key not in _ensured:
                _metadata.create_all(engine, tables=[catalog], checkfirst=True)
                _ensured.add(key)
    return engine


def _now():
    return datetime.now(timezone.utc)


def _exporter_version() -> Optional[str]:
    try:
        from materializationengine import __version__

        return str(__version__)
    except Exception:
        return None


def record_writing(aligned_volume: str, datastack: str, version: int, table_name: str,
                   lakes: List[Dict], job_id: Optional[str] = None) -> None:
    """Upsert a `writing` row for each lake: {"spec_name", "uri", "partition_by", "spec"}.

    A row for the same lake from an earlier export (whose lake was then deleted, since
    run_deltalake_export refuses to overwrite one) is reset rather than duplicated.
    """
    from sqlalchemy.dialects.postgresql import insert

    engine = _engine(aligned_volume)
    now = _now()
    with engine.begin() as conn:
        for lake in lakes:
            values = dict(datastack=datastack, version=int(version), table_name=table_name,
                          spec_name=lake["spec_name"], uri=lake["uri"], partition_by=lake.get("partition_by"),
                          spec=lake.get("spec"), status="writing", job_id=job_id,
                          exporter_version=_exporter_version(), created_at=now)
            reset = {k: values[k] for k in ("uri", "partition_by", "spec", "status", "job_id", "exporter_version",
                                            "created_at")}
            reset.update(row_count=None, bytes=None, files=None, columns=None, error=None,
                         completed_at=None, deleted_at=None)
            conn.execute(insert(catalog).values(**values).on_conflict_do_update(
                constraint="uq_deltalake_exports_lake", set_=reset))


def _set(aligned_volume: str, datastack: str, version: int, table_name: str, spec_name: str, **fields) -> None:
    engine = _engine(aligned_volume)
    with engine.begin() as conn:
        conn.execute(catalog.update().where(sa.and_(
            catalog.c.datastack == datastack, catalog.c.version == int(version),
            catalog.c.table_name == table_name, catalog.c.spec_name == spec_name)).values(**fields))


def lake_facts(uri: str) -> Dict:
    """Rows, bytes, files and columns of a finished lake, from its Delta log."""
    import pyarrow as pa
    from deltalake import DeltaTable

    dt = DeltaTable(uri)
    adds = pa.table(dt.get_add_actions(flatten=True))
    schema = dt.schema().to_arrow()
    schema = schema if isinstance(schema, pa.Schema) else pa.schema(schema)
    return {
        "row_count": int(sum(adds.column("num_records").to_pylist())) if adds.num_rows else 0,
        "bytes": int(sum(adds.column("size_bytes").to_pylist())) if adds.num_rows else 0,
        "files": adds.num_rows,
        "columns": [{"name": f.name, "type": str(f.type)} for f in schema],
    }


def record_finished(aligned_volume: str, datastack: str, version: int, table_name: str,
                    lakes: List[Dict], expected_rows: Optional[int]) -> List[str]:
    """Mark each lake complete if its row count matches expected_rows, else failed.

    Returns the names of lakes that did not check out. expected_rows None (unknown)
    accepts any count.
    """
    bad = []
    for lake in lakes:
        try:
            facts = lake_facts(lake["uri"])
        except Exception as e:
            bad.append(lake["spec_name"])
            _set(aligned_volume, datastack, version, table_name, lake["spec_name"], status="failed",
                 error=f"could not read the finished lake: {type(e).__name__}: {e}")
            continue
        if expected_rows is not None and facts["row_count"] != int(expected_rows):
            bad.append(lake["spec_name"])
            _set(aligned_volume, datastack, version, table_name, lake["spec_name"], status="failed",
                 error=f"lake has {facts['row_count']} rows, expected {expected_rows}", **facts)
            continue
        final = {"spec": lake["spec"]} if lake.get("spec") is not None else {}
        _set(aligned_volume, datastack, version, table_name, lake["spec_name"], status="complete",
             completed_at=_now(), error=None, **facts, **final)
    return bad


def record_failed(aligned_volume: str, datastack: str, version: int, table_name: str,
                  spec_names: List[str], error: str) -> None:
    for name in spec_names:
        _set(aligned_volume, datastack, version, table_name, name, status="failed", error=error[:4000])


def forget(aligned_volume: str, datastack: str, version: int, table_name: str, spec_names: List[str]) -> None:
    """Remove rows for lakes an export recorded but then did not write (e.g. the extra
    specs of a table found small enough for one lake)."""
    if not spec_names:
        return
    engine = _engine(aligned_volume)
    with engine.begin() as conn:
        conn.execute(catalog.delete().where(sa.and_(
            catalog.c.datastack == datastack, catalog.c.version == int(version),
            catalog.c.table_name == table_name, catalog.c.spec_name.in_(list(spec_names)))))


def list_exports(aligned_volume: str, datastack: str, version: Optional[int] = None,
                 table_name: Optional[str] = None, statuses=("complete",)) -> List[Dict]:
    """Catalog rows, newest version first; statuses None for every status."""
    engine = _engine(aligned_volume)
    q = catalog.select().where(catalog.c.datastack == datastack)
    if version is not None:
        q = q.where(catalog.c.version == int(version))
    if table_name is not None:
        q = q.where(catalog.c.table_name == table_name)
    if statuses:
        q = q.where(catalog.c.status.in_(list(statuses)))
    q = q.order_by(catalog.c.version.desc(), catalog.c.table_name, catalog.c.spec_name)
    with engine.connect() as conn:
        rows = conn.execute(q).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d.pop("id", None)
        for k in ("created_at", "completed_at", "deleted_at"):
            if d.get(k) is not None:
                d[k] = d[k].isoformat()
        for k in ("spec", "columns"):
            if isinstance(d.get(k), str):
                d[k] = json.loads(d[k])
        out.append(d)
    return out


def safely(fn, *args, **kwargs):
    """Run a catalog write, logging rather than raising on failure (see module docstring)."""
    try:
        return fn(*args, **kwargs)
    except Exception:
        logger.exception("Delta Lake catalog: %s failed", getattr(fn, "__name__", fn))
        return None
