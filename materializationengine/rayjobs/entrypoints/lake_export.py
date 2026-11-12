"""Export tables of a frozen version to Delta Lake, in one RayJob (the parallel export).

The driver (the RayJob's head pod) plans each table -- or, if KubeRay restarted it, loads
the plan it saved -- and runs every table's id ranges and optimize tasks as Ray tasks on
the workers, at most --max-streams ranges at once (DELTALAKE_EXPORT_MAX_STREAMS). Each
range is published to its lakes from the driver as it finishes. See
materializationengine/workflows/lake_export and DELTA_LAKE_EXPORT_PLAN.md.

With one table, the table's progress record uses --job-id itself, as the single-table
endpoint reports it; with several, each table's is <job-id>.<table>.

Usage::

    python -m materializationengine.rayjobs.entrypoints.lake_export \\
        --datastack minnie65_phase3_v1 --version 1926 --job-id abc123 --tables synapses_pni_2
"""

import argparse
import json
import logging
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


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Export tables of a frozen version to Delta Lake via Ray")
    parser.add_argument("--datastack", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--job-id", required=True)
    parser.add_argument("--tables", nargs="+", required=True)
    parser.add_argument("--output-specs", default=None, help="JSON list of output specs (one table only)")
    parser.add_argument("--config", default=None, help="DELTALAKE_EXPORT_CONFIG JSON (default: the app config)")
    parser.add_argument("--name-suffix", default=None, help="appended to every output spec name")
    parser.add_argument("--max-streams", type=int, default=None,
                        help="ranges at once across the job (default DELTALAKE_EXPORT_MAX_STREAMS)")
    args = parser.parse_args(argv)
    if args.output_specs and len(args.tables) != 1:
        parser.error("--output-specs needs exactly one table")

    from materializationengine.app import create_app
    from materializationengine.info_client import get_datastack_info
    from materializationengine.utils import get_config_param
    from materializationengine.workflows import deltalake_version_export as dve
    from materializationengine.workflows.lake_export.job import TableRequest, run_tables
    from materializationengine.workflows.lake_export.settings import ExportSettings

    app = create_app()
    with app.app_context():
        settings = ExportSettings.from_config(app.config)
        config = dve.load_export_config(args.config or get_config_param("DELTALAKE_EXPORT_CONFIG", None))
        requests = []
        for table in args.tables:
            skip, output_specs, overrides = dve.resolve_table_config(config, table)
            if skip:
                logger.info("%s: skipped by DELTALAKE_EXPORT_CONFIG", table)
                continue
            if args.output_specs:
                output_specs = json.loads(args.output_specs)
            if args.name_suffix:
                overrides["name_suffix"] = args.name_suffix
            requests.append(TableRequest(table, output_specs, overrides))
        datastack_info = get_datastack_info(args.datastack)

        import ray

        ray.init()
        logger.info("ray cluster resources: %s", ray.cluster_resources())
        single = len(args.tables) == 1
        summaries = run_tables(
            datastack_info, args.version, requests,
            job_id_for=lambda t: args.job_id if single else dve.table_job_id(args.job_id, t),
            settings=settings, executor=RayExecutor(settings.range_retries), max_streams=args.max_streams)
    for table, summary in summaries.items():
        logger.info("%s: %s", table, summary)
    failed = sorted(t for t, s in summaries.items() if s["state"] != "complete")
    logger.info("%d table(s) exported, %d failed%s", len(summaries) - len(failed), len(failed),
                f": {failed}" if failed else "")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
