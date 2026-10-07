"""Table maintenance: report how well tables are ordered by id, and reorder them with pg_repack.

Postgres stores rows in the order they were written. A table whose rows are not in id
order (bulk loads with their own ids, segmentation rows written in spatial order) makes
an id join either a hash join that spills to temp files once it outgrows work_mem, or a
merge join over randomly ordered index reads. Reordering by id makes the merge join
read both tables sequentially. pg_repack rewrites the table in id order while it stays
readable and writable, taking an exclusive lock only briefly at the start and the swap.

Runs on the workflow queue (the producer: non-preemptible and not evicted by the
cluster autoscaler), one repack per database at a time.
"""

import json
import os
import re
import subprocess
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from celery.utils.log import get_task_logger
from sqlalchemy import text

from materializationengine.celery_init import celery
from materializationengine.database import db_manager
from materializationengine.redis_client import SharedRedis

celery_logger = get_task_logger(__name__)

REDIS_CLIENT = SharedRedis(db=0)

PG_REPACK_BIN = os.environ.get("PG_REPACK_BIN", "/usr/lib/postgresql/18/bin/pg_repack")
STATUS_KEY_PREFIX = "table_maintenance:repack:"
LOCK_KEY_PREFIX = "table_maintenance:repack_lock:"
STATUS_TTL_SECONDS = 7 * 24 * 3600
LOCK_TTL_SECONDS = 48 * 3600
# Without force, refuse tables whose table + indexes are larger than this. pg_repack
# builds a full second copy of both before dropping the old one.
DEFAULT_MAX_TABLE_GB = 100

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_]+$")
_EXCLUDED_DATABASES = {"postgres", "cloudsqladmin", "template0", "template1", "template_postgis"}


class RepackRefused(Exception):
    """A precondition failed; nothing was changed."""


def _check_name(kind: str, name: str) -> str:
    if not name or not _SAFE_NAME.match(name):
        raise RepackRefused(f"invalid {kind} name {name!r}")
    return name


def check_database(database: str) -> str:
    """A database on this instance that holds annotation data (live or frozen)."""
    _check_name("database", database)
    if database in _EXCLUDED_DATABASES:
        raise RepackRefused(f"database {database!r} is not an annotation database")
    with db_manager.get_engine(database).connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = :d AND NOT datistemplate"), {"d": database}
        ).scalar()
    if not exists:
        raise RepackRefused(f"database {database!r} does not exist")
    return database


_ORDER_REPORT_SQL = """
SELECT c.relname AS table_name,
       CASE WHEN a.table_name IS NOT NULL THEN 'annotation'
            WHEN g.table_name IS NOT NULL THEN 'segmentation' ELSE 'other' END AS kind,
       c.reltuples::bigint AS rows,
       pg_table_size(c.oid) AS table_bytes,
       pg_indexes_size(c.oid) AS index_bytes,
       s.correlation AS id_correlation,
       COALESCE((SELECT bool_or(i.indisclustered) FROM pg_index i WHERE i.indrelid = c.oid), false) AS clustered_flag,
       st.n_tup_upd AS updates,
       st.n_tup_hot_upd AS hot_updates,
       GREATEST(st.last_analyze, st.last_autoanalyze) AS last_analyzed,
       c.reloptions AS reloptions
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
LEFT JOIN pg_stats s ON s.schemaname = 'public' AND s.tablename = c.relname AND s.attname = 'id'
LEFT JOIN pg_stat_user_tables st ON st.relid = c.oid
LEFT JOIN annotation_table_metadata a ON a.table_name = c.relname
LEFT JOIN segmentation_table_metadata g ON g.table_name = c.relname
WHERE c.relkind = 'r' AND c.reltuples >= :min_rows
  AND (a.table_name IS NOT NULL OR g.table_name IS NOT NULL)
ORDER BY c.reltuples DESC
"""


def table_order_report(
    database: str, min_rows: int = 100_000, max_correlation: Optional[float] = None
) -> List[Dict[str, Any]]:
    """Annotation and segmentation tables with their size and how well ordered by id they are.

    id_correlation is Postgres's statistic for how closely physical order follows id
    (1.0 = in order; None = not analyzed yet). With max_correlation, only tables at or
    below it (or never analyzed) are listed.
    """
    with db_manager.get_engine(check_database(database)).connect() as conn:
        rows = conn.execute(text(_ORDER_REPORT_SQL), {"min_rows": min_rows}).fetchall()
    report = []
    for r in rows:
        corr = r["id_correlation"]
        if max_correlation is not None and corr is not None and abs(corr) > max_correlation:
            continue
        report.append({
            "table_name": r["table_name"],
            "kind": r["kind"],
            "rows": r["rows"],
            "table_gb": round(r["table_bytes"] / 1e9, 2),
            "index_gb": round(r["index_bytes"] / 1e9, 2),
            "id_correlation": None if corr is None else round(float(corr), 3),
            "clustered_flag": r["clustered_flag"],
            "updates": r["updates"],
            "hot_updates": r["hot_updates"],
            "last_analyzed": r["last_analyzed"].isoformat() if r["last_analyzed"] else None,
            "reloptions": r["reloptions"],
        })
    return report


def _table_facts(conn, table: str) -> Dict[str, Any]:
    row = conn.execute(
        text(
            "SELECT c.reltuples::bigint, pg_total_relation_size(c.oid),"
            " (SELECT i.relname FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid"
            "   WHERE x.indrelid = c.oid AND x.indisprimary) AS pkey,"
            " (SELECT correlation FROM pg_stats WHERE schemaname='public' AND tablename=c.relname AND attname='id')"
            " FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'"
            " WHERE c.relname = :t AND c.relkind = 'r'"
        ),
        {"t": table},
    ).fetchone()
    if row is None:
        raise RepackRefused(f"table {table!r} not found")
    return {"rows": row[0], "total_bytes": row[1], "total_gb": round(row[1] / 1e9, 2), "primary_key": row[2],
            "id_correlation": None if row[3] is None else round(float(row[3]), 3)}


def _client_version() -> str:
    out = subprocess.run([PG_REPACK_BIN, "--version"], capture_output=True, text=True, check=True).stdout
    return out.strip().split()[-1]


def _ensure_extension(conn, dry_run: bool) -> Optional[str]:
    version = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname = 'pg_repack'")).scalar()
    if version is None and not dry_run:
        conn.execute(text("CREATE EXTENSION pg_repack"))
        version = conn.execute(text("SELECT extversion FROM pg_extension WHERE extname = 'pg_repack'")).scalar()
    return version


def build_repack_command(db_url, table: str, order_by: str, jobs: int, dry_run: bool, wait_timeout: int) -> List[str]:
    cmd = [
        PG_REPACK_BIN,
        "--no-superuser-check",  # Cloud SQL users are cloudsqlsuperuser, not superuser
        "--no-kill-backend",  # give up, rather than cancel queries holding the table
        f"--wait-timeout={int(wait_timeout)}",
        f"--host={db_url.host}",
        f"--port={db_url.port or 5432}",
        f"--username={db_url.username}",
        f"--dbname={db_url.database}",
        f'--table=public."{table}"',
        f"--order-by={order_by}",
    ]
    if jobs and jobs > 1:
        cmd.append(f"--jobs={int(jobs)}")
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def _set_status(job_id: str, **fields) -> Dict[str, Any]:
    key = f"{STATUS_KEY_PREFIX}{job_id}"
    raw = REDIS_CLIENT.get(key)
    status = json.loads(raw) if raw else {"job_id": job_id}
    status.update(fields, updated_at=datetime.now(timezone.utc).isoformat())
    REDIS_CLIENT.set(key, json.dumps(status, default=str), ex=STATUS_TTL_SECONDS)
    return status


def get_repack_status(job_id: str) -> Optional[Dict[str, Any]]:
    raw = REDIS_CLIENT.get(f"{STATUS_KEY_PREFIX}{job_id}")
    return json.loads(raw) if raw else None


def start_repack(database: str, table_name: str, **options) -> Dict[str, Any]:
    """Validate the request now (so the API can refuse it) and queue the repack."""
    check_database(database)
    _check_name("table", table_name)
    order_by = _check_name("order_by column", options.get("order_by", "id"))
    job_id = f"{database}.{table_name}.{uuid.uuid4().hex[:8]}"
    _set_status(job_id, state="queued", database=database, table_name=table_name, options=options)
    repack_table.apply_async(
        kwargs={"job_id": job_id, "database": database, "table_name": table_name, **options, "order_by": order_by}
    )
    return get_repack_status(job_id)


# acks_late=False: a repack whose worker died must be cleaned up (pg_repack leaves its
# trigger and half-built copy) before it is run again, so it is never redelivered.
@celery.task(name="workflow:repack_table", bind=True, acks_late=False)
def repack_table(
    self,
    job_id: str,
    database: str,
    table_name: str,
    order_by: str = "id",
    dry_run: bool = False,
    jobs: int = 2,
    fillfactor: Optional[int] = None,
    force: bool = False,
    max_table_gb: float = DEFAULT_MAX_TABLE_GB,
    wait_timeout: int = 60,
) -> Dict[str, Any]:
    """Reorder one table by order_by with pg_repack, then flag the primary key as clustered."""
    lock_key = f"{LOCK_KEY_PREFIX}{database}"
    if not REDIS_CLIENT.set(lock_key, job_id, nx=True, ex=LOCK_TTL_SECONDS):
        holder = REDIS_CLIENT.get(lock_key)
        return _set_status(job_id, state="refused", reason=f"another repack is running in {database}: {holder!r}")
    started = time.time()
    try:
        engine = db_manager.get_engine(database)
        with engine.connect() as conn:
            before = _table_facts(conn, table_name)
        _set_status(job_id, state="checking", before=before)
        if not before["primary_key"]:
            raise RepackRefused(f"table {table_name!r} has no primary key")
        if before["total_bytes"] > max_table_gb * 1e9 and not force:
            raise RepackRefused(
                f"table + indexes are {before['total_gb']} GB (> {max_table_gb} GB); pg_repack needs that much"
                " free disk again while it runs. Pass force=true once you have checked the instance has room."
            )

        client_version = _client_version()
        with engine.begin() as conn:
            extension_version = _ensure_extension(conn, dry_run)
            if extension_version and extension_version != client_version:
                raise RepackRefused(
                    f"pg_repack client {client_version} does not match the database extension {extension_version}"
                )
            if fillfactor and not dry_run:
                conn.execute(text(f'ALTER TABLE "{table_name}" SET (fillfactor = {int(fillfactor)})'))
        if extension_version is None:  # dry run without the extension installed
            return _set_status(
                job_id, state="dry run", would_install_extension=True, client_version=client_version,
                estimated_extra_disk_gb=before["total_gb"],
            )

        cmd = build_repack_command(engine.url, table_name, order_by, jobs, dry_run, wait_timeout)
        _set_status(job_id, state="running", command=" ".join(cmd), client_version=client_version)
        celery_logger.info(f"Repack {job_id}: {' '.join(cmd)}")
        result = subprocess.run(
            cmd, capture_output=True, text=True, env={**os.environ, "PGPASSWORD": engine.url.password or ""}
        )
        output = (result.stdout + result.stderr)[-4000:]
        if result.returncode != 0:
            return _set_status(job_id, state="failed", returncode=result.returncode, output=output,
                               seconds=round(time.time() - started))
        if dry_run:
            return _set_status(job_id, state="dry run", output=output, estimated_extra_disk_gb=before["total_gb"],
                               seconds=round(time.time() - started))

        with engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE "{table_name}" CLUSTER ON "{before["primary_key"]}"'))
            conn.execute(text(f'ANALYZE "{table_name}"'))
        with engine.connect() as conn:
            after = _table_facts(conn, table_name)
        return _set_status(job_id, state="done", output=output, after=after, seconds=round(time.time() - started))
    except RepackRefused as e:
        return _set_status(job_id, state="refused", reason=str(e))
    except Exception as e:
        celery_logger.error(f"Repack {job_id} failed: {e}", exc_info=True)
        return _set_status(job_id, state="failed", error=str(e), seconds=round(time.time() - started))
    finally:
        if REDIS_CLIENT.get(lock_key) in (job_id, job_id.encode()):
            REDIS_CLIENT.delete(lock_key)


_FROZEN_NAME = re.compile(r"^(?P<datastack>.+)__mat(?P<version>\d+)$")


def list_databases(with_sizes: bool = True) -> List[Dict[str, Any]]:
    """Databases on this instance, live and frozen, with frozen versions' validity and expiry.

    Frozen (materialized) databases are named <datastack>__mat<version>; their details come
    from the analysisversion table of the live database that holds them.
    """
    with db_manager.get_engine("postgres").connect() as conn:
        rows = conn.execute(
            text(
                "SELECT datname, " + ("pg_database_size(datname)" if with_sizes else "NULL")
                + " FROM pg_database WHERE NOT datistemplate ORDER BY datname"
            )
        ).fetchall()
    names = [r[0] for r in rows if r[0] not in _EXCLUDED_DATABASES]
    sizes = {r[0]: r[1] for r in rows}

    versions: Dict[str, Dict[str, Any]] = {}
    for name in names:
        if _FROZEN_NAME.match(name):
            continue
        try:
            with db_manager.get_engine(name).connect() as conn:
                if not conn.execute(text("SELECT to_regclass('public.analysisversion')")).scalar():
                    continue
                for v in conn.execute(text(
                    "SELECT datastack, version, valid, expires_on, status, time_stamp FROM analysisversion"
                )):
                    versions[f"{v[0]}__mat{v[1]}"] = {
                        "live_database": name, "datastack": v[0], "version": v[1], "valid": v[2],
                        "expires_on": v[3].isoformat() if v[3] else None, "status": v[4],
                        "time_stamp": v[5].isoformat() if v[5] else None,
                    }
        except Exception as e:  # a database we cannot read is still listed
            celery_logger.warning(f"Could not read analysisversion in {name}: {e}")

    result = []
    for name in names:
        frozen = _FROZEN_NAME.match(name)
        entry = {"name": name, "kind": "frozen" if frozen else "live",
                 "size_gb": None if sizes.get(name) is None else round(sizes[name] / 1e9, 1)}
        if frozen:
            entry.update(versions.get(name) or {"datastack": frozen["datastack"], "version": int(frozen["version"])})
        result.append(entry)
    return result


def list_repack_jobs(limit: int = 50) -> List[Dict[str, Any]]:
    """Recent repack jobs, newest first (status records are kept for 7 days)."""
    jobs = []
    for key in REDIS_CLIENT.scan_iter(f"{STATUS_KEY_PREFIX}*", count=1000):
        raw = REDIS_CLIENT.get(key)
        if raw:
            jobs.append(json.loads(raw))
    jobs.sort(key=lambda j: j.get("updated_at", ""), reverse=True)
    return jobs[:limit]
