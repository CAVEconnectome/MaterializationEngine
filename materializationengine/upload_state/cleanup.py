"""Daily upkeep of the upload state database (the chart's upload-state-cleanup CronJob).

    python -m materializationengine.upload_state.cleanup --retention-days 30 --reconcile

1. Deletes jobs not updated for --retention-days, with all their rows (cascade).
2. With --reconcile, ends jobs whose driver is gone: a running job with no heartbeat
   for --stale-minutes whose RayJob has finished, or no longer exists, can never
   finish, so it is marked error (and shows so on the upload pages).
3. Drops the staging tables of failed or cancelled jobs that still have them, once
   they have been finished for --staging-grace-hours.
4. Deletes bulk upload RayJob CRs that finished more than --rayjob-retention-days ago.

Creates the database first if it is missing, like everything else that uses it.
"""

import argparse
import datetime
import logging
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("upload_state.cleanup")


def reconcile(engine, stale_minutes: float, dry_run: bool) -> list:
    from materializationengine.rayjobs import submitter
    from materializationengine.upload_state import store

    ended = []
    for job in store.jobs_with_dead_driver(engine, stale_minutes):
        name = job.get("rayjob_name")
        reason = None
        if not name:
            reason = "The upload's Ray job was never started"
        else:
            try:
                if not submitter.rayjob_exists(name):
                    reason = f"The upload's Ray job {name} no longer exists"
                else:
                    status = submitter.get_rayjob_status(name)
                    if status["finished"]:
                        reason = (
                            f"The upload's Ray job {name} ended "
                            f"({status['jobDeploymentStatus'] or status['jobStatus']}) "
                            "without finishing the upload"
                        )
            except submitter.RayJobSubmissionError as e:
                logger.warning(f"could not check RayJob {name} of {job['job_id']}: {e}")
                continue
        if reason is None:
            continue  # still running or retrying: its driver will be back
        ended.append(job["job_id"])
        logger.info(f"{job['job_id']}: {reason}")
        if not dry_run:
            if job.get("cancel_requested_at"):
                # cancelled while no driver was running to notice
                store.mark_terminal_if_active(engine, job["job_id"], "cancelled")
            else:
                store.mark_terminal_if_active(engine, job["job_id"], "error", reason)
            store.set_job_fields(engine, job["job_id"], retryable=False)
    return ended


def drop_failed_staging(engine, grace_hours: float, dry_run: bool) -> list:
    from materializationengine.blueprints.upload.tasks import _discard_staging
    from materializationengine.upload_state import store
    from materializationengine.upload_state.status_sink import StoreSink, use_sink

    dropped = []
    for job in store.jobs_needing_staging_cleanup(engine, grace_hours):
        dropped.append(job["job_id"])
        logger.info(f"{job['job_id']}: dropping staging tables of {job['table_name']}")
        if dry_run:
            continue
        # a cancelled job's record must still take the staging_cleanup note
        with use_sink(StoreSink(engine, job["job_id"])):
            _discard_staging(job["table_name"], job["job_id"], job["status"])
    return dropped


def delete_old_rayjobs(retention_days: float, dry_run: bool) -> list:
    from materializationengine.rayjobs import submitter

    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=retention_days)
    deleted = []
    selector = f"{submitter.UPLOAD_WORKLOAD_LABEL}={submitter.UPLOAD_WORKLOAD_VALUE}"
    for job in submitter.list_rayjobs(selector):
        end = job.get("endTime")
        if not job["finished"] or not end:
            continue
        if datetime.datetime.fromisoformat(end.replace("Z", "+00:00")) > cutoff:
            continue
        deleted.append(job["name"])
        if not dry_run:
            submitter.delete_rayjob(job["name"])
    return deleted


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Upkeep of the upload state database")
    parser.add_argument("--retention-days", type=float, default=30)
    parser.add_argument("--reconcile", action="store_true")
    parser.add_argument("--stale-minutes", type=float, default=10)
    parser.add_argument("--staging-grace-hours", type=float, default=48)
    parser.add_argument("--rayjob-retention-days", type=float, default=7)
    parser.add_argument("--skip-rayjobs", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    from materializationengine.rayjobs.upload_tasks import create_worker_app
    from materializationengine.upload_state import db, store

    app = create_worker_app()
    failures = 0
    with app.app_context():
        engine = db.get_engine()
        steps = [
            ("purge", lambda: store.purge_older_than(engine, args.retention_days, args.dry_run)),
        ]
        if args.reconcile:
            steps.append(("reconcile", lambda: reconcile(engine, args.stale_minutes, args.dry_run)))
        steps.append(("staging", lambda: drop_failed_staging(engine, args.staging_grace_hours, args.dry_run)))
        if not args.skip_rayjobs:
            steps.append(("rayjobs", lambda: delete_old_rayjobs(args.rayjob_retention_days, args.dry_run)))
        for name, step in steps:
            # one failing step must not keep the others from running
            try:
                done = step()
                logger.info(f"{name}: {len(done)} {'(dry run) ' if args.dry_run else ''}{done[:20]}")
            except Exception:
                failures += 1
                logger.exception(f"{name} failed")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
