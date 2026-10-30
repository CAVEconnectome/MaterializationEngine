"""The upload API's side of Ray-run uploads: starting one, and reading their state.

A Ray-run upload's job record, spatial lookup and chunks live in the upload state
database instead of redis. The upload pages keep calling the same endpoints and get
the same JSON: the endpoints look a job or workflow up here first, then in redis.
"""

import datetime
import logging
import shlex
from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from flask import current_app

from materializationengine.rayjobs import submitter
from materializationengine.upload_state import db, models, store
from materializationengine.utils import get_config_param

logger = logging.getLogger(__name__)

# How often a status request may ask Kubernetes about the job's RayJob.
RAYJOB_CHECK_SECONDS = 10
# A RayJob not found this soon after submission may just not be visible yet.
RAYJOB_MISSING_GRACE_SECONDS = 180
QUEUED_PHASE = "Queued, starting a Ray cluster"
ENTRYPOINT = "python -m materializationengine.rayjobs.entrypoints.bulk_upload"


def _int_setting(name: str) -> Optional[int]:
    value = get_config_param(name)
    return int(value) if value not in (None, "") else None


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def store_engine():
    """The upload state database, or None where Ray uploads are not set up (or it is
    unreachable, which must not break the Celery uploads' pages)."""
    if not submitter.ray_enabled():
        return None
    try:
        return db.get_engine()
    except Exception as e:
        logger.warning(f"upload state database unavailable: {e}")
        return None


# --- starting -----------------------------------------------------------------------


def start_ray_upload(
    job_id: str,
    file_path: str,
    file_metadata: Dict[str, Any],
    datastack_info: Dict[str, Any],
    user_id: str,
) -> Tuple[Dict[str, Any], int]:
    """Create the job in the upload state database and submit its RayJob.

    Returns the start endpoint's (JSON, status code). Everything the driver needs, and
    every value that must stay the same across its attempts, goes in params now.
    """
    from materializationengine.blueprints.upload import production_build as pb
    from materializationengine.blueprints.upload.supplied_ids import (
        maps_segmentation_columns,
        supplied_segmentation,
    )
    from materializationengine.database import db_manager

    config = current_app.config
    engine = db.get_engine()
    now = _utcnow()
    table_name = file_metadata["metadata"]["table_name"]
    datastack_name = datastack_info.get("datastack", "unknown")
    # The tables are built in the production database under temporary names and
    # published by renaming them (production_build.py); there is no staging copy.
    aligned_volume = datastack_info["aligned_volume"]["name"]
    pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]
    build_tables = pb.build_names(job_id, table_name, pcg_table_name)
    table_meta = pb.table_metadata_from_file(file_metadata)
    too_long = pb.names_too_long(build_tables, table_meta, pcg_table_name)
    if too_long:
        return {
            "status": "error",
            "message": f"Table name {table_name!r} is too long: Postgres would truncate {too_long}",
        }, 400
    taken = pb.names_in_use(db_manager.get_engine(aligned_volume), build_tables)
    if taken:
        return {
            "status": "error",
            "message": f"{taken} already exist in {aligned_volume}; choose another table name",
        }, 409
    params = {
        "file_path": file_path,
        "file_metadata": file_metadata,
        "datastack_info": datastack_info,
        "sql_instance_name": config.get("SQL_INSTANCE_NAME"),
        "chunk_scale_factor": config.get("CHUNK_SCALE_FACTOR", 2),
        "supervoxel_batch_size": config.get("SUPERVOXEL_BATCH_SIZE"),
        # the rows' created time and the processed CSV's name
        "created_ts": now.isoformat(),
        # root ids are looked up as of this moment, on every attempt
        "materialization_time_stamp": now.replace(tzinfo=None).isoformat(),
        "build_in_production": True,
        "build_tables": build_tables,
        "table_meta": table_meta,
    }
    supplied = (
        supplied_segmentation(file_metadata)
        if maps_segmentation_columns(file_metadata.get("column_mapping"))
        else None
    )
    if supplied:
        # the source's own supervoxel and/or root ids (supplied_ids.py)
        params["supplied_segmentation"] = supplied
    try:
        store.create_job(
            engine,
            job_id,
            user_id=user_id,
            datastack_name=datastack_name,
            table_name=table_name,
            staging_database=aligned_volume,
            record={
                "status": "pending",
                "phase": QUEUED_PHASE,
                "progress": 0,
                "user_id": user_id,
                "datastack_name": datastack_name,
                "staging_table_name": build_tables["annotation"],
            },
            params=params,
        )
    except store.ActiveUploadExists as e:
        return {"status": "error", "message": f"{e}; wait for it to finish or cancel it"}, 409

    try:
        name = submitter.submit_rayjob(
            f"{ENTRYPOINT} --job-id {shlex.quote(job_id)}",
            f"upload-{table_name}",
            num_workers=_int_setting("UPLOAD_RAY_MAX_WORKERS"),
            metadata={
                submitter.UPLOAD_WORKLOAD_LABEL: submitter.UPLOAD_WORKLOAD_VALUE,
                submitter.UPLOAD_JOB_LABEL: job_id,
            },
            active_deadline_seconds=_int_setting("UPLOAD_RAY_ACTIVE_DEADLINE_SECONDS"),
        )
    except submitter.RayJobSubmissionError as e:
        logger.error(f"Could not submit the RayJob for upload {job_id}: {e}")
        store.mark_terminal_if_active(engine, job_id, "error", f"Could not start the Ray job: {e}")
        return {"status": "error", "message": f"Could not start the upload: {e}"}, 500

    store.set_job_fields(
        engine, job_id, rayjob_name=name, rayjob_namespace=submitter._namespace(), rayjob_submissions=1
    )
    return {"status": "start", "task_id": name, "job_id": job_id}, 200


# --- job records --------------------------------------------------------------------


def _driver_alive(job: dict, now: datetime.datetime) -> bool:
    beat = job.get("driver_heartbeat_at")
    return beat is not None and (now - beat).total_seconds() < store.LEASE_STALE_SECONDS


def _rayjob_status(engine, job: dict, now: datetime.datetime) -> Optional[dict]:
    """The RayJob's status, read at most every RAYJOB_CHECK_SECONDS; {"missing": True}
    if it no longer exists, None if it could not be read."""
    checked = job.get("rayjob_checked_at")
    if checked is not None and (now - checked).total_seconds() < RAYJOB_CHECK_SECONDS:
        return job.get("rayjob_last_status")
    try:
        status = submitter.get_rayjob_status(job["rayjob_name"])
    except submitter.RayJobSubmissionError as e:
        if getattr(e.__cause__, "status", None) != 404:
            logger.warning(f"could not read RayJob {job['rayjob_name']}: {e}")
            return job.get("rayjob_last_status")
        status = {"missing": True}
    store.set_job_fields(engine, job["job_id"], rayjob_last_status=status, rayjob_checked_at=now)
    return status


def merged_record(engine, job: dict) -> dict:
    """The job record, with what its RayJob says that the driver could not.

    A job whose RayJob ended (or vanished) while it was still running can never finish,
    so it is marked error here, rather than showing "pending" or a stale phase for
    ever. A cluster still starting, or a failed attempt being retried, is shown in the
    phase.
    """
    record = dict(job["record"])
    if job["status"] not in models.ACTIVE_STATUSES or not job.get("rayjob_name"):
        return record
    now = _utcnow()
    status = _rayjob_status(engine, job, now)
    if not status:
        return record
    alive = _driver_alive(job, now)
    ended = None
    if status.get("missing"):
        if (now - job["created_at"]).total_seconds() > RAYJOB_MISSING_GRACE_SECONDS and not alive:
            ended = f"The upload's Ray job {job['rayjob_name']} no longer exists"
    elif status.get("finished") and not alive:
        detail = status.get("message") or status.get("jobDeploymentStatus") or status.get("jobStatus")
        ended = f"The upload's Ray job ended ({detail}) without finishing the upload"
    if ended:
        final = "cancelled" if job.get("cancel_requested_at") else "error"
        if store.mark_terminal_if_active(engine, job["job_id"], final, None if final == "cancelled" else ended):
            logger.warning(f"upload {job['job_id']}: {ended}")
        return store.get_job_record(engine, job["job_id"]) or record
    if status.get("retrying") or (status.get("failed_attempts") and not alive):
        record["phase"] = f"{record.get('phase') or 'Processing'} (retrying after a failed attempt)"
    elif job["status"] == "pending" and not alive:
        record["phase"] = QUEUED_PHASE
    return record


def get_record(job_id: str) -> Optional[dict]:
    """A Ray-run upload's record as the status endpoint returns it, or None."""
    engine = store_engine()
    if engine is None:
        return None
    job = store.get_job(engine, job_id)
    return merged_record(engine, job) if job else None


def list_records(window_hours: float = 24) -> List[dict]:
    """Records (with job_id) of running Ray uploads and of those that ended recently."""
    engine = store_engine()
    if engine is None:
        return []
    try:
        return store.list_jobs(engine, window_hours)
    except Exception as e:
        logger.warning(f"could not list Ray uploads: {e}")
        return []


def merge_job_lists(redis_jobs: List[dict], ray_jobs: List[dict]) -> List[dict]:
    """Both lists, one entry per job_id (a Ray upload's own record wins), newest first."""
    by_id = {j.get("job_id"): j for j in redis_jobs}
    by_id.update({j["job_id"]: j for j in ray_jobs})
    jobs = list(by_id.values())
    jobs.sort(key=lambda j: str(j.get("last_updated") or ""), reverse=True)
    return jobs


def request_cancel(job_id: str, user_id: Optional[str]) -> Optional[Tuple[Dict[str, Any], int]]:
    """Cancel a Ray-run upload; None if job_id is not one.

    Only a request: the driver stops at its next check, discards staging and sets the
    status. Refused once the transfer to production has started.
    """
    engine = store_engine()
    if engine is None or store.get_job(engine, job_id) is None:
        return None
    job = store.request_cancel(
        engine, job_id, by=user_id, record_patch={"phase": "Cancelling", "error": None}
    )
    if job["cancel_requested_at"] is None:
        if job["status"] in models.ACTIVE_STATUSES:
            message = "The transfer to production has started; it can no longer be cancelled"
        else:
            message = f"The upload is already {job['status']}"
        return {"status": "error", "message": message, "details": job["record"]}, 409
    return {"status": "success", "message": "Cancelling", "details": job["record"]}, 200


# --- spatial lookup workflows -------------------------------------------------------


class StoreCheckpointView:
    """The reading side of RedisCheckpointManager, over the upload state database."""

    def __init__(self, engine, database: str):
        self.engine = engine
        self.database = database

    def _row(self, workflow_name: str) -> Optional[dict]:
        return store.get_spatial_by_workflow(self.engine, self.database, workflow_name)

    def get_workflow_data(self, workflow_name: str):
        row = self._row(workflow_name)
        return store.as_workflow_data(row) if row else None

    def get_all_chunk_statuses(self, workflow_name: str) -> Dict[str, str]:
        row = self._row(workflow_name)
        if not row:
            return {}
        return {str(c["chunk_index"]): c["status"] for c in store.chunk_rows(self.engine, row["job_id"])}

    def get_all_failed_chunk_details(self, workflow_name: str) -> Dict[str, dict]:
        row = self._row(workflow_name)
        if not row:
            return {}
        return {
            str(c["chunk_index"]): {
                "error_message": c["error_message"],
                "attempt_count": c["attempt_count"],
                "timestamp": c["updated_at"].isoformat() if c["updated_at"] else None,
                "status": c["status"],
                # redis's details have no type; the workflow page shows it when present
                "error_type": c["error_type"],
            }
            for c in store.chunk_rows(self.engine, row["job_id"])
            if c["status"] in ("FAILED_RETRYABLE", "FAILED_PERMANENT")
        }

    def get_active_workflows(self) -> List[Dict[str, Any]]:
        workflows = []
        for row in store.list_active_spatial(self.engine, self.database):
            wf = asdict(store.as_workflow_data(row))
            wf["latest_completed_chunk_info"] = None
            workflows.append(wf)
        return workflows


def checkpoint_reader(database: str, workflow_name: Optional[str] = None):
    """The checkpoint to read a workflow from: redis's, or the upload state database's
    when that holds the more recently updated run of workflow_name."""
    from materializationengine.blueprints.upload.checkpoint_manager import RedisCheckpointManager

    redis_view = RedisCheckpointManager(database)
    engine = store_engine()
    if engine is None or workflow_name is None:
        return redis_view
    try:
        stored = StoreCheckpointView(engine, database)
        stored_data = stored.get_workflow_data(workflow_name)
    except Exception as e:
        logger.warning(f"could not read workflow {workflow_name} from the upload state database: {e}")
        return redis_view
    if stored_data is None:
        return redis_view
    redis_data = redis_view.get_workflow_data(workflow_name)
    if redis_data is not None and str(redis_data.updated_at or "") > str(stored_data.updated_at or ""):
        return redis_view
    return stored


def active_workflows(database: str) -> List[Dict[str, Any]]:
    """Active workflows of a database from redis and the upload state database."""
    from materializationengine.blueprints.upload.checkpoint_manager import RedisCheckpointManager

    workflows = RedisCheckpointManager(database).get_active_workflows()
    engine = store_engine()
    if engine is None:
        return workflows
    try:
        stored = StoreCheckpointView(engine, database).get_active_workflows()
    except Exception as e:
        logger.warning(f"could not list workflows in the upload state database: {e}")
        return workflows
    names = {wf["table_name"] for wf in stored}
    return [wf for wf in workflows if wf.get("table_name") not in names] + stored
