"""Export every table of one frozen version to Delta Lake in one RayJob.

The driver (the RayJob's head pod) lists the version's tables, resolves each one's
settings (DELTALAKE_EXPORT_CONFIG), and runs each table's export -- the unchanged
run_deltalake_export -- as a Ray task on a worker, at most --max-streams at a time,
biggest first. Each table task fans its per-spec optimize out to further workers, as the
per-table RayJob does. A table that fails is recorded and the others carry on; the job
exits 1 if any failed. Progress: get_version_status (the api's
GET .../write_deltalake_version/...), with each running table's own progress folded in.

Table tasks need the database, so workers need their cloud-sql-proxy (the chart gives
them one when ray.upload or ray.worker.cloudsqlProxy is set) and enough memory for a
stream: about 3x DELTALAKE_FLUSH_THRESHOLD_BYTES at peak (~1.7 GB at ltv's 512 MiB,
~6.3 GB at minnie's 2 GiB). RAY_TABLE_EXPORT_RESOURCES (JSON Ray task options, default
{"num_cpus": 1}) sets what each table task asks for, e.g. {"num_cpus": 1, "memory": ...}
or a custom resource that only export-sized workers advertise.

Usage::

    python -m materializationengine.rayjobs.entrypoints.deltalake_export_version \\
        --datastack minnie65_phase3_v1 --version 2253 --job-id abc123
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ray.deltalake_export_version")


def _table_task_options() -> dict:
    raw = os.environ.get("RAY_TABLE_EXPORT_RESOURCES", "").strip()
    if not raw:
        return {"num_cpus": 1}
    try:
        options = json.loads(raw)
    except ValueError:
        raise SystemExit(f"RAY_TABLE_EXPORT_RESOURCES must be a JSON object of Ray task options, got {raw!r}")
    if not isinstance(options, dict):
        raise SystemExit("RAY_TABLE_EXPORT_RESOURCES must be a JSON object")
    return options


def main() -> int:
    from materializationengine.rayjobs.entrypoints.deltalake_export import _optimize_memory_default

    parser = argparse.ArgumentParser(description="Export all tables of a frozen version to Delta Lake via Ray")
    parser.add_argument("--datastack", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--tables", nargs="*", help="only these tables (default: all of the version's tables)")
    parser.add_argument("--include-views", action="store_true", help="also export the version's views")
    parser.add_argument("--max-streams", type=int, default=None,
                        help="tables streaming at once (default DELTALAKE_MAX_CONCURRENT_TABLE_STREAMS, else 2)")
    parser.add_argument("--config", default=None, help="DELTALAKE_EXPORT_CONFIG JSON (default: the app config)")
    parser.add_argument("--name-suffix", default=None,
                        help="appended to every output spec name, to export a version again beside a first export")
    parser.add_argument("--optimize-memory-bytes", type=int, default=_optimize_memory_default())
    parser.add_argument("--optimize-max-concurrent-tasks", type=int, default=4)
    args = parser.parse_args()

    from materializationengine.app import create_app
    from materializationengine.info_client import get_datastack_info
    from materializationengine.utils import get_config_param
    from materializationengine.workflows import deltalake_version_export as dve

    app = create_app()
    with app.app_context():
        def status(**f):
            return dve.set_version_status(args.datastack, args.version, args.job_id, **f)

        try:
            config = dve.load_export_config(args.config or get_config_param("DELTALAKE_EXPORT_CONFIG", None))
            max_streams = args.max_streams or int(get_config_param(
                "DELTALAKE_MAX_CONCURRENT_TABLE_STREAMS", dve.DEFAULT_MAX_CONCURRENT_TABLE_STREAMS))
            tables = dve.list_version_tables(args.datastack, args.version, include_views=args.include_views)
            if args.tables:
                missing = set(args.tables) - {t["table"] for t in tables}
                if missing:
                    raise ValueError(f"not in v{args.version}: {sorted(missing)}")
                tables = [t for t in tables if t["table"] in set(args.tables)]
            plan, skipped = [], []
            for t in tables:
                skip, output_specs, overrides = dve.resolve_table_config(config, t["table"])
                if skip:
                    skipped.append(t["table"])
                    continue
                if args.name_suffix:
                    overrides["name_suffix"] = args.name_suffix
                plan.append({**t, "output_specs": output_specs, "overrides": overrides})
            datastack_info = get_datastack_info(args.datastack)
        except Exception as e:
            logger.exception("could not plan the export")
            status(state="failed", error=f"{type(e).__name__}: {e}")
            return 1

        status(state="running", max_streams=max_streams, skipped=skipped,
               rayjob=os.environ.get("RAY_JOB_NAME") or None)
        logger.info("exporting %d table(s) of %s v%d, %d at a time; skipped by config: %s",
                    len(plan), args.datastack, args.version, max_streams, skipped or "none")

        import ray

        ray.init()
        logger.info("ray cluster resources: %s", ray.cluster_resources())
        table_options = _table_task_options()

        @ray.remote(max_retries=0)
        def export_table(datastack_info, version, table, output_specs, overrides, job_id,
                         optimize_memory_bytes, optimize_max_concurrent_tasks):
            # max_retries=0: a lake half-written by a lost worker must be deleted before
            # the table is exported again (run_deltalake_export refuses existing lakes),
            # so a retry would only fail on it. The table is reported failed instead.
            from materializationengine.rayjobs.entrypoints.deltalake_export import _build_ray_optimize_runner
            from materializationengine.rayjobs.upload_tasks import worker_app
            from materializationengine.workflows.deltalake_export import run_deltalake_export

            with worker_app().app_context():
                try:
                    run_deltalake_export(
                        datastack_info, version, table, output_specs=output_specs, job_id=job_id,
                        optimize_runner=_build_ray_optimize_runner(optimize_memory_bytes, optimize_max_concurrent_tasks),
                        spec_overrides=overrides or None,
                    )
                except Exception as e:  # many client exceptions do not pickle back to the driver
                    raise RuntimeError(f"{type(e).__name__}: {e}") from None
            return table

        def start(t):
            return export_table.options(**table_options).remote(
                datastack_info, args.version, t["table"], t["output_specs"], t["overrides"],
                dve.table_job_id(args.job_id, t["table"]), args.optimize_memory_bytes,
                args.optimize_max_concurrent_tasks)

        def wait(handles):
            done, _ = ray.wait(handles, num_returns=1)
            try:
                ray.get(done[0])
                return done[0], None
            except Exception as e:
                cause = getattr(e, "cause", None) or e
                return done[0], f"{type(cause).__name__}: {cause}"

        states = dve.run_tables(plan, start, wait, max_streams, on_change=lambda s: status(tables=s))
        failed = sorted(name for name, s in states.items() if s["state"] == "failed")
        status(state="failed" if failed else "done", failed_tables=failed)
        logger.info("v%d: %d table(s) exported, %d failed%s", args.version, len(states) - len(failed), len(failed),
                    f": {failed}" if failed else "")
        return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
