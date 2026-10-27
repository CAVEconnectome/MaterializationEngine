"""Reads and writes of the upload state database.

Plain functions taking an engine, usable from the API, the RayJob driver and the cleanup
CronJob alike. Only the driver of a job writes its stages, parts and chunks; the API
writes the job row when the upload starts and the cancel request.
"""

import datetime
import json
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from materializationengine.upload_state import models
from materializationengine.upload_state.models import (
    ACTIVE_STATUSES,
    upload_cloudsql_ops,
    upload_jobs,
    upload_parts,
    upload_spatial,
    upload_spatial_chunks,
    upload_stages,
)

# A driver whose heartbeat is older than this is presumed dead; its lease can be taken.
LEASE_STALE_SECONDS = 90


class ActiveUploadExists(RuntimeError):
    """Another upload into the same staging table is still running."""


def _dumps(value) -> str:
    return json.dumps(value, default=str)


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


# --- jobs ---------------------------------------------------------------------------


def create_job(
    engine,
    job_id: str,
    *,
    user_id: Optional[str],
    datastack_name: str,
    table_name: str,
    staging_database: str,
    record: Dict[str, Any],
    params: Dict[str, Any],
) -> None:
    """Insert a pending job; ActiveUploadExists if the table already has a running one."""
    record = {**record, "status": record.get("status", "pending"), "last_updated": _utcnow().isoformat()}
    try:
        with engine.begin() as conn:
            conn.execute(
                upload_jobs.insert().values(
                    job_id=job_id,
                    user_id=user_id,
                    datastack_name=datastack_name,
                    table_name=table_name,
                    staging_database=staging_database,
                    status=record["status"],
                    record=record,
                    params=params,
                )
            )
    except IntegrityError as e:
        if "ux_upload_jobs_one_active_per_table" in str(e):
            raise ActiveUploadExists(
                f"An upload into {staging_database}.{table_name} is already running"
            ) from e
        raise


def get_job(engine, job_id: str) -> Optional[dict]:
    with engine.connect() as conn:
        row = conn.execute(
            upload_jobs.select().where(upload_jobs.c.job_id == job_id)
        ).first()
    return dict(row) if row else None


def get_job_record(engine, job_id: str) -> Optional[dict]:
    """The job record as the upload pages read it (the redis-shaped dict)."""
    job = get_job(engine, job_id)
    return dict(job["record"]) if job else None


def update_job(
    engine,
    job_id: str,
    patch: Dict[str, Any],
    *,
    epoch: Optional[int] = None,
    stage: Optional[str] = None,
    finished: bool = False,
) -> bool:
    """Merge patch into the job record; False if nothing was written.

    Like update_job_status for redis records: once a cancel has been requested, only an
    update that itself sets status "cancelled" is applied, so a driver still running
    cannot overwrite the cancellation. With epoch, the update applies only if that
    driver still holds the lease.
    """
    patch = {**patch, "last_updated": _utcnow().isoformat()}
    status = patch.get("status")
    sql = """
        UPDATE upload_jobs
           SET record = record || CAST(:patch AS jsonb),
               status = COALESCE(:status, status),
               current_stage = COALESCE(:stage, current_stage),
               finished_at = CASE WHEN :finished THEN now() ELSE finished_at END,
               updated_at = now()
         WHERE job_id = :job_id
           AND (cancel_requested_at IS NULL OR :status = 'cancelled')
    """
    params = {
        "patch": _dumps(patch),
        "status": status,
        "stage": stage,
        "finished": finished,
        "job_id": job_id,
    }
    if epoch is not None:
        sql += " AND driver_epoch = :epoch"
        params["epoch"] = epoch
    with engine.begin() as conn:
        return conn.execute(text(sql), params).rowcount > 0


def set_job_fields(engine, job_id: str, **fields) -> None:
    """Set columns of the job row (rayjob_name, retryable, rayjob_last_status, ...)."""
    if not fields:
        return
    with engine.begin() as conn:
        conn.execute(
            upload_jobs.update()
            .where(upload_jobs.c.job_id == job_id)
            .values(**fields, updated_at=_utcnow())
        )


def list_jobs(engine, window_hours: float = 24) -> List[dict]:
    """Records of running jobs, and of jobs that ended within the last window_hours."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT job_id, record FROM upload_jobs
                 WHERE status IN :active OR updated_at > now() - make_interval(secs => :secs)
                 ORDER BY updated_at DESC
                """
            ),
            {"active": tuple(ACTIVE_STATUSES), "secs": window_hours * 3600},
        ).fetchall()
    return [{**dict(r["record"]), "job_id": r["job_id"]} for r in rows]


# Stages a cancel can no longer stop: the transfer replaces the production table.
UNCANCELLABLE_STAGES = ("transfer", "cleanup")


def request_cancel(
    engine, job_id: str, by: Optional[str] = None, record_patch: Optional[Dict[str, Any]] = None
) -> Optional[dict]:
    """Record a cancel request (once); returns the job row, or None if there is none.

    record_patch is merged into the record with it (update_job refuses updates once a
    cancel is requested). Refused (cancel_requested_at stays NULL in the returned row)
    for a finished job, or once the driver has entered the transfer; see enter_stage.
    """
    patch = {**(record_patch or {}), "last_updated": _utcnow().isoformat()}
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE upload_jobs
                   SET cancel_requested_at = COALESCE(cancel_requested_at, now()),
                       cancel_requested_by = COALESCE(cancel_requested_by, :by),
                       record = CASE WHEN cancel_requested_at IS NULL
                                     THEN record || CAST(:patch AS jsonb) ELSE record END,
                       updated_at = now()
                 WHERE job_id = :job_id
                   AND status IN :active
                   AND (current_stage IS NULL OR current_stage NOT IN :late)
                """
            ),
            {"job_id": job_id, "by": by, "late": UNCANCELLABLE_STAGES,
             "patch": _dumps(patch), "active": tuple(ACTIVE_STATUSES)},
        )
    return get_job(engine, job_id)


def enter_stage(engine, job_id: str, stage: str, epoch: int, refuse_if_cancelled: bool = False) -> bool:
    """Set the job's current stage, if this driver holds the lease (and, with
    refuse_if_cancelled, if no cancel has been requested). False if not set."""
    sql = (
        "UPDATE upload_jobs SET current_stage = :stage, updated_at = now() "
        "WHERE job_id = :job_id AND driver_epoch = :epoch"
    )
    if refuse_if_cancelled:
        sql += " AND cancel_requested_at IS NULL"
    with engine.begin() as conn:
        return conn.execute(
            text(sql), {"stage": stage, "job_id": job_id, "epoch": epoch}
        ).rowcount > 0


def is_cancel_requested(engine, job_id: str) -> bool:
    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("SELECT cancel_requested_at IS NOT NULL FROM upload_jobs WHERE job_id = :j"),
                {"j": job_id},
            ).scalar()
        )


def mark_terminal_if_active(engine, job_id: str, status: str, error: Optional[str] = None) -> bool:
    """Set a final status unless the job already has one (used when its RayJob is gone)."""
    patch = {"status": status, **({"error": error} if error else {})}
    with engine.begin() as conn:
        return (
            conn.execute(
                text(
                    """
                    UPDATE upload_jobs
                       SET record = record || CAST(:patch AS jsonb), status = :status,
                           finished_at = now(), updated_at = now()
                     WHERE job_id = :job_id AND status IN :active
                    """
                ),
                {"patch": _dumps({**patch, "last_updated": _utcnow().isoformat()}),
                 "status": status, "job_id": job_id, "active": tuple(ACTIVE_STATUSES)},
            ).rowcount
            > 0
        )


# --- lease --------------------------------------------------------------------------


def acquire_lease(engine, job_id: str, host: str, stale_seconds: int = LEASE_STALE_SECONDS) -> Optional[int]:
    """Take the job's lease if no live driver holds it; returns the new epoch or None."""
    with engine.begin() as conn:
        return conn.execute(
            text(
                """
                UPDATE upload_jobs
                   SET driver_epoch = driver_epoch + 1, driver_host = :host,
                       driver_heartbeat_at = now(), updated_at = now()
                 WHERE job_id = :job_id
                   AND (driver_heartbeat_at IS NULL
                        OR driver_heartbeat_at < now() - make_interval(secs => :stale))
                RETURNING driver_epoch
                """
            ),
            {"job_id": job_id, "host": host, "stale": stale_seconds},
        ).scalar()


def heartbeat(engine, job_id: str, epoch: int) -> Tuple[bool, bool]:
    """Renew the lease; returns (still the owner, cancel requested)."""
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                UPDATE upload_jobs SET driver_heartbeat_at = now()
                 WHERE job_id = :job_id AND driver_epoch = :epoch
                RETURNING cancel_requested_at IS NOT NULL
                """
            ),
            {"job_id": job_id, "epoch": epoch},
        ).first()
    return (row is not None, bool(row[0]) if row else False)


def release_lease(engine, job_id: str, epoch: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE upload_jobs SET driver_heartbeat_at = NULL "
                "WHERE job_id = :job_id AND driver_epoch = :epoch"
            ),
            {"job_id": job_id, "epoch": epoch},
        )


# --- stages -------------------------------------------------------------------------


def get_stages(engine, job_id: str) -> Dict[str, dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            upload_stages.select().where(upload_stages.c.job_id == job_id)
        ).fetchall()
    return {r["stage"]: dict(r) for r in rows}


def start_stage(engine, job_id: str, stage: str, epoch: int) -> None:
    stmt = insert(upload_stages).values(
        job_id=job_id, stage=stage, status="running", attempts=1, epoch=epoch,
        started_at=_utcnow(),
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["job_id", "stage"],
        set_={
            "status": "running",
            "attempts": upload_stages.c.attempts + 1,
            "epoch": epoch,
            "started_at": _utcnow(),
            "error": None,
        },
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def finish_stage(engine, job_id: str, stage: str, epoch: int, result: Optional[dict] = None) -> bool:
    """Mark a stage done; False if this driver no longer holds the lease."""
    with engine.begin() as conn:
        owner = conn.execute(
            text("SELECT driver_epoch = :epoch FROM upload_jobs WHERE job_id = :job_id FOR UPDATE"),
            {"job_id": job_id, "epoch": epoch},
        ).scalar()
        if not owner:
            return False
        conn.execute(
            upload_stages.update()
            .where((upload_stages.c.job_id == job_id) & (upload_stages.c.stage == stage))
            .values(status="done", result=result or {}, finished_at=_utcnow(), error=None)
        )
    return True


def fail_stage(engine, job_id: str, stage: str, error: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            upload_stages.update()
            .where((upload_stages.c.job_id == job_id) & (upload_stages.c.stage == stage))
            .values(status="failed", error=error[:4000], finished_at=_utcnow())
        )


# --- parts --------------------------------------------------------------------------


def plan_parts(engine, job_id: str, parts: Iterable[dict]) -> None:
    """Store the part plan once; a plan already stored is kept as it is."""
    rows = [
        {
            "job_id": job_id,
            "part_index": p["part_index"],
            "kind": p["kind"],
            "spec": p["spec"],
            "output_blob": p["output_blob"],
            "id_counter_start": p.get("id_counter_start"),
            "status": "pending",
        }
        for p in parts
    ]
    if not rows:
        return
    with engine.begin() as conn:
        conn.execute(insert(upload_parts).values(rows).on_conflict_do_nothing())


def get_parts(engine, job_id: str) -> List[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            upload_parts.select()
            .where(upload_parts.c.job_id == job_id)
            .order_by(upload_parts.c.part_index)
        ).fetchall()
    return [dict(r) for r in rows]


def update_parts(engine, job_id: str, updates: Iterable[dict]) -> None:
    """Apply per-part updates: each dict has part_index plus the columns to set."""
    with engine.begin() as conn:
        for u in updates:
            values = {k: v for k, v in u.items() if k != "part_index"}
            values["updated_at"] = _utcnow()
            conn.execute(
                upload_parts.update()
                .where((upload_parts.c.job_id == job_id) & (upload_parts.c.part_index == u["part_index"]))
                .values(**values)
            )


# --- the Cloud SQL import -----------------------------------------------------------


def get_import(engine, job_id: str) -> Optional[dict]:
    with engine.connect() as conn:
        row = conn.execute(
            upload_cloudsql_ops.select().where(
                (upload_cloudsql_ops.c.job_id == job_id) & (upload_cloudsql_ops.c.kind == "import")
            )
        ).first()
    return dict(row) if row else None


def record_import_intent(engine, job_id: str, uri: str, database: str, table: str) -> dict:
    """Note the import before it is requested; returns the (possibly existing) row."""
    with engine.begin() as conn:
        conn.execute(
            insert(upload_cloudsql_ops)
            .values(job_id=job_id, kind="import", uri=uri, database=database,
                    target_table=table, status="requested")
            .on_conflict_do_nothing()
        )
    return get_import(engine, job_id)


def set_import_operation(engine, job_id: str, operation_name: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            upload_cloudsql_ops.update()
            .where((upload_cloudsql_ops.c.job_id == job_id) & (upload_cloudsql_ops.c.kind == "import"))
            .values(operation_name=operation_name, status="running")
        )


def finish_import(engine, job_id: str, status: str, error: Optional[str] = None) -> None:
    with engine.begin() as conn:
        conn.execute(
            upload_cloudsql_ops.update()
            .where((upload_cloudsql_ops.c.job_id == job_id) & (upload_cloudsql_ops.c.kind == "import"))
            .values(status=status, error=error, finished_at=_utcnow())
        )


def reset_import(engine, job_id: str) -> None:
    """Forget a requested import that never started, before requesting it again."""
    with engine.begin() as conn:
        conn.execute(
            upload_cloudsql_ops.delete().where(
                (upload_cloudsql_ops.c.job_id == job_id) & (upload_cloudsql_ops.c.kind == "import")
            )
        )


# --- spatial lookup -----------------------------------------------------------------


def init_spatial(engine, job_id: str, **fields) -> dict:
    """Create the job's spatial lookup row once; returns the stored row."""
    with engine.begin() as conn:
        conn.execute(
            insert(upload_spatial)
            .values(job_id=job_id, start_time=_utcnow(), updated_at=_utcnow(), **fields)
            .on_conflict_do_nothing()
        )
    return get_spatial(engine, job_id)


def get_spatial(engine, job_id: str) -> Optional[dict]:
    with engine.connect() as conn:
        row = conn.execute(
            upload_spatial.select().where(upload_spatial.c.job_id == job_id)
        ).first()
    return dict(row) if row else None


def get_spatial_by_workflow(engine, database_name: str, workflow_name: str) -> Optional[dict]:
    """The most recent spatial lookup of a table in a database."""
    with engine.connect() as conn:
        row = conn.execute(
            upload_spatial.select()
            .where(
                (upload_spatial.c.database_name == database_name)
                & (upload_spatial.c.workflow_name == workflow_name)
            )
            .order_by(upload_spatial.c.created_at.desc())
            .limit(1)
        ).first()
    return dict(row) if row else None


def update_spatial(engine, job_id: str, **fields) -> None:
    with engine.begin() as conn:
        conn.execute(
            upload_spatial.update()
            .where(upload_spatial.c.job_id == job_id)
            .values(**fields, updated_at=_utcnow())
        )


def record_chunk_results(engine, job_id: str, rows: List[dict]) -> None:
    """Upsert chunk rows (chunk_index, status, attempt_count, rows_processed, points,
    error_message, error_type) in one statement. A COMPLETED chunk stays COMPLETED."""
    if not rows:
        return
    # one row per chunk: an upsert may not touch the same row twice, and a chunk
    # retried before a flush appears more than once (the last is the latest)
    rows = list({r["chunk_index"]: r for r in rows}.values())
    values = [
        {
            "job_id": job_id,
            "chunk_index": r["chunk_index"],
            "status": r["status"],
            "attempt_count": r.get("attempt_count", 0),
            "rows_processed": r.get("rows_processed"),
            "points": r.get("points"),
            "error_message": (r.get("error_message") or None) and r["error_message"][:4000],
            "error_type": r.get("error_type"),
            "updated_at": _utcnow(),
        }
        for r in rows
    ]
    stmt = insert(upload_spatial_chunks).values(values)
    excluded = stmt.excluded
    stmt = stmt.on_conflict_do_update(
        index_elements=["job_id", "chunk_index"],
        set_={
            "status": excluded.status,
            "attempt_count": excluded.attempt_count,
            "rows_processed": excluded.rows_processed,
            "points": excluded.points,
            "error_message": excluded.error_message,
            "error_type": excluded.error_type,
            "updated_at": excluded.updated_at,
        },
        where=upload_spatial_chunks.c.status != "COMPLETED",
    )
    with engine.begin() as conn:
        conn.execute(stmt)


def chunk_state(engine, job_id: str) -> Tuple[Set[int], Set[int], Dict[int, int]]:
    """(completed chunk indexes, permanently failed ones, attempts by chunk)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT chunk_index, status, attempt_count FROM upload_spatial_chunks "
                "WHERE job_id = :j"
            ),
            {"j": job_id},
        ).fetchall()
    completed = {r[0] for r in rows if r[1] == "COMPLETED"}
    permanent = {r[0] for r in rows if r[1] == "FAILED_PERMANENT"}
    attempts = {r[0]: r[2] for r in rows}
    return completed, permanent, attempts


def chunk_totals(engine, job_id: str) -> Dict[str, int]:
    """Counts recomputed from the chunk rows (the authority after a driver restart)."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT count(*) FILTER (WHERE status = 'COMPLETED'),
                       count(*) FILTER (WHERE status = 'FAILED_PERMANENT'),
                       coalesce(sum(points) FILTER (WHERE status = 'COMPLETED'), 0)
                  FROM upload_spatial_chunks WHERE job_id = :j
                """
            ),
            {"j": job_id},
        ).first()
    return {"completed_chunks": row[0], "failed_chunks": row[1], "rows_processed": int(row[2])}


def chunk_rows(engine, job_id: str) -> List[dict]:
    with engine.connect() as conn:
        rows = conn.execute(
            upload_spatial_chunks.select()
            .where(upload_spatial_chunks.c.job_id == job_id)
            .order_by(upload_spatial_chunks.c.chunk_index)
        ).fetchall()
    return [dict(r) for r in rows]


def list_active_spatial(engine, database_name: Optional[str] = None) -> List[dict]:
    """Spatial lookups not yet completed, failed or cancelled."""
    query = upload_spatial.select().where(
        ~upload_spatial.c.status.in_(("completed", "COMPLETED", "failed", "cancelled"))
    )
    if database_name:
        query = query.where(upload_spatial.c.database_name == database_name)
    with engine.connect() as conn:
        return [dict(r) for r in conn.execute(query).fetchall()]


def as_workflow_data(row: dict):
    """A checkpoint_manager.WorkflowData for a spatial row, so the existing status and
    progress-estimate code applies unchanged."""
    from materializationengine.blueprints.upload.checkpoint_manager import WorkflowData

    def iso(value):
        return value.isoformat() if isinstance(value, datetime.datetime) else value

    return WorkflowData(
        table_name=row["workflow_name"],
        task_id=row["job_id"],
        datastack_name=row.get("datastack_name"),
        status=row["status"],
        total_chunks=row.get("total_chunks") or 0,
        completed_chunks=row.get("completed_chunks") or 0,
        rows_processed=row.get("rows_processed") or 0,
        start_time=iso(row.get("start_time")) or iso(row.get("created_at")),
        updated_at=iso(row.get("updated_at")) or iso(row.get("created_at")),
        created_at=iso(row.get("created_at")),
        estimated_completion=row.get("estimated_completion"),
        processing_rate=row.get("processing_rate"),
        total_row_estimate=row.get("total_row_estimate"),
        total_point_estimate=row.get("total_point_estimate"),
        rate_samples=row.get("rate_samples"),
        min_enclosing_bbox=row.get("min_enclosing_bbox"),
        chunking_strategy=row.get("chunking_strategy"),
        used_chunk_size=row.get("used_chunk_size"),
        last_error=row.get("last_error"),
        chunking_parameters=row.get("chunking_parameters"),
    )


# --- retention ----------------------------------------------------------------------


def purge_older_than(engine, days: float, dry_run: bool = False) -> List[str]:
    """Delete jobs (and, by cascade, all their rows) not updated for days; returns ids."""
    with engine.begin() as conn:
        ids = [
            r[0]
            for r in conn.execute(
                text(
                    "SELECT job_id FROM upload_jobs "
                    "WHERE updated_at < now() - make_interval(days => :days) ORDER BY job_id"
                ),
                {"days": int(days)},
            )
        ]
        if ids and not dry_run:
            conn.execute(upload_jobs.delete().where(upload_jobs.c.job_id.in_(ids)))
    return ids


def jobs_with_dead_driver(engine, stale_minutes: float = 10) -> List[dict]:
    """Running jobs whose driver has not heartbeated for stale_minutes (or never has)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM upload_jobs
                 WHERE status IN :active
                   AND coalesce(driver_heartbeat_at, created_at) < now() - make_interval(mins => :m)
                """
            ),
            {"active": tuple(ACTIVE_STATUSES), "m": int(stale_minutes)},
        ).fetchall()
    return [dict(r) for r in rows]


def jobs_needing_staging_cleanup(engine, grace_hours: float = 48) -> List[dict]:
    """Failed or cancelled jobs, finished more than grace_hours ago, whose staging tables
    were not dropped (the driver died, or dropping them failed)."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT * FROM upload_jobs
                 WHERE status IN ('error', 'cancelled')
                   AND coalesce(finished_at, updated_at) < now() - make_interval(hours => :h)
                   AND coalesce(record->>'staging_cleanup', '') <> 'done'
                 ORDER BY job_id
                """
            ),
            {"h": int(grace_hours)},
        ).fetchall()
    return [dict(r) for r in rows]


__all__ = [n for n in dir() if not n.startswith("_")] + ["models"]
