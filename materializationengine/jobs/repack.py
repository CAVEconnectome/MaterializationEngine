"""One pg_repack, run as a Kubernetes Job: `python -m materializationengine.jobs.repack`.

The api's start_repack recorded the job as queued and passed run_repack's keyword
arguments in MAINTENANCE_JOB_ARGS. This records progress and the result in the same redis
status record the Celery task writes, so the api reads both the same way.

Exit status: 0 when the repack finished, was a dry run, or was refused (nothing changed,
and the reason is in the status record); 1 when it failed. backoffLimit is 0, so a failed
Job is never retried.
"""

import json
import logging
import sys

from materializationengine.k8s_jobs import job_kwargs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("materializationengine.jobs.repack")


def main() -> int:
    kwargs = job_kwargs()
    if not kwargs.get("job_id"):
        logger.error("MAINTENANCE_JOB_ARGS has no job_id: %r", kwargs)
        return 2

    # db_manager and get_config_param read SQLALCHEMY_DATABASE_URI through the app
    # context, so the repack needs one, as the Celery worker has.
    from materializationengine.app import create_app
    from materializationengine.workflows.table_maintenance import run_repack

    app = create_app()
    with app.app_context():
        status = run_repack(**kwargs)
    logger.info("repack %s: %s", kwargs["job_id"], json.dumps(status, default=str)[:4000])
    return 0 if status.get("state") in ("done", "dry run", "refused") else 1


if __name__ == "__main__":
    sys.exit(main())
