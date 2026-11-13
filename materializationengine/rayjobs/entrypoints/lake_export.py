"""Export tables of a frozen version to Delta Lake, in one RayJob (the parallel export).

The driver (the RayJob's head pod) plans each table -- or, if KubeRay restarted it, loads
the plan it saved -- and runs every table's id ranges and optimize tasks as Ray tasks on
the workers, at most --max-streams ranges at once (DELTALAKE_EXPORT_MAX_STREAMS). Each
range is published to its lakes from the driver as it finishes. See
materializationengine/workflows/lake_export and DELTA_LAKE_EXPORT_PLAN.md.

With one table, the table's progress record uses --job-id itself, as the single-table
endpoint reports it; with several, each table's is <job-id>.<table>. --version-job
exports every table of the version (or --tables), keeps the version's status record
(get_version_status), and with --views runs the views stage once every table is complete
(materializationengine/workflows/lake_export/version.py).

Usage::

    python -m materializationengine.rayjobs.entrypoints.lake_export \\
        --datastack minnie65_phase3_v1 --version 1926 --job-id abc123 --tables synapses_pni_2
    python -m materializationengine.rayjobs.entrypoints.lake_export \\
        --datastack minnie65_phase3_v1 --version 1926 --job-id abc123 --version-job --views
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ray.lake_export")


class RayExecutor:
    """Units as Ray tasks of one CPU each, retried when their worker is lost or they raise
    (a range retries into a fresh staging directory; an optimize rewrites its partitions)."""

    def __init__(self, retries: int):
        import ray

        self.ray = ray
        self.retries = retries
        self._remote = {}

    def submit(self, unit):
        remote = self._remote.get(unit.fn)
        if remote is None:
            remote = self._remote[unit.fn] = self.ray.remote(unit.fn).options(
                num_cpus=1, max_retries=self.retries, retry_exceptions=True)
        return remote.remote(*unit.args)

    def wait(self, handles, timeout):
        done, _ = self.ray.wait(list(handles), num_returns=1, timeout=timeout)
        return done

    def result(self, handle):
        return self.ray.get(handle)

    def cancel(self, handles):
        for h in handles:
            try:
                self.ray.cancel(h)
            except Exception:
                pass


def cluster_now(ray) -> dict:
    """The cluster's CPUs and live worker nodes (the head runs no tasks)."""
    workers = [n for n in ray.nodes() if n.get("Alive") and n.get("Resources", {}).get("CPU", 0) > 0]
    return {"cpus": ray.cluster_resources().get("CPU", 0), "workers": len(workers)}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Export tables of a frozen version to Delta Lake via Ray")
    parser.add_argument("--datastack", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--tables", nargs="+", default=None,
                        help="tables to export (a version job's default: every table of the version)")
    parser.add_argument("--version-job", action="store_true",
                        help="export as a version job: the version's status record, then the views stage")
    parser.add_argument("--views", action="store_true",
                        help="run the views stage once every table is complete (needs --version-job)")
    parser.add_argument("--output-specs", default=None, help="JSON list of output specs (one table only)")
    parser.add_argument("--config", default=None, help="DELTALAKE_EXPORT_CONFIG JSON (default: the app config)")
    parser.add_argument("--name-suffix", default=None, help="appended to every output spec name")
    parser.add_argument("--max-streams", type=int, default=None,
                        help="ranges at once across the job (default DELTALAKE_EXPORT_MAX_STREAMS)")
    args = parser.parse_args(argv)
    if not args.version_job and not args.tables:
        parser.error("--tables is required unless --version-job")
    if args.views and not args.version_job:
        parser.error("--views needs --version-job")
    if args.output_specs and (args.version_job or len(args.tables) != 1):
        parser.error("--output-specs needs exactly one table and no --version-job")

    from materializationengine.app import create_app
    from materializationengine.info_client import get_datastack_info
    from materializationengine.utils import get_config_param
    from materializationengine.workflows import deltalake_version_export as dve
    from materializationengine.workflows.lake_export.job import TableRequest, run_tables
    from materializationengine.workflows.lake_export.settings import ExportSettings
    from materializationengine.workflows.lake_export.source import FrozenSource
    from materializationengine.workflows.lake_export.version import VIEWS_STAGE_CONFIG, VersionStatus, \
        resolve_stage, run_version

    app = create_app()
    with app.app_context():
        settings = ExportSettings.from_config(app.config)
        status = VersionStatus(args.datastack, args.version, args.job_id) if args.version_job else None
        try:
            config = dve.load_export_config(args.config or get_config_param("DELTALAKE_EXPORT_CONFIG", None))
            source = FrozenSource()
            tables = args.tables or [t["table"] for t in source.list_tables(args.datastack, args.version)]
            requests, skipped = [], []
            for table in tables:
                skip, output_specs, overrides = dve.resolve_table_config(config, table)
                if skip:
                    skipped.append(table)
                    continue
                if args.output_specs:
                    output_specs = json.loads(args.output_specs)
                if args.name_suffix:
                    overrides["name_suffix"] = args.name_suffix
                requests.append(TableRequest(table, output_specs, overrides))
            views_stage = resolve_stage(get_config_param(VIEWS_STAGE_CONFIG, None)) if args.views else None
            datastack_info = get_datastack_info(args.datastack)
        except Exception as e:
            logger.exception("could not plan the job")
            if status is not None:
                status.update(state="failed", error=f"{type(e).__name__}: {e}")
            return 1
        logger.info("exporting %d table(s) of %s v%d; skipped by config: %s", len(requests), args.datastack,
                    args.version, skipped or "none")

        import ray

        ray.init()
        logger.info("ray cluster resources: %s", ray.cluster_resources())
        executor = RayExecutor(settings.range_retries)
        if args.version_job:
            status.update(rayjob=os.environ.get("RAY_JOB_NAME") or None)
            if args.views:
                # Keep the job's workers through the tables' tail into the views stage:
                # the autoscaler drops a worker idle for a minute, and as optimizes finish
                # most are, just before the stage needs them all.
                from ray.autoscaler.sdk import request_resources

                workers = settings.workers_for(args.max_streams, views=True)
                request_resources(bundles=[{"CPU": settings.worker_cpus}] * workers)
                logger.info("holding %d worker(s) of %d CPU for the views stage", workers, settings.worker_cpus)
            result = run_version(datastack_info, args.version, args.job_id, requests, settings, executor,
                                 views=args.views, views_stage=views_stage, skipped=skipped,
                                 max_streams=args.max_streams, source=source, status=status,
                                 name_suffix=args.name_suffix, cluster=lambda: cluster_now(ray))
            summaries, ok = result["tables"], result["state"] == "done"
            logger.info("views stage: %s", result["views"])
        else:
            summaries = run_tables(datastack_info, args.version, requests, job_id_for=lambda t: args.job_id
                                   if len(requests) == 1 else dve.table_job_id(args.job_id, t),
                                   settings=settings, executor=executor, max_streams=args.max_streams,
                                   source=source)
            ok = all(s["state"] == "complete" for s in summaries.values())
    for table, summary in summaries.items():
        logger.info("%s: %s", table, summary)
    failed = sorted(t for t, s in summaries.items() if s["state"] != "complete")
    logger.info("%d table(s) exported, %d failed%s", len(summaries) - len(failed), len(failed),
                f": {failed}" if failed else "")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
