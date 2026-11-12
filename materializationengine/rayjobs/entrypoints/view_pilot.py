"""The view pilot as a RayJob: build a version's views with Postgres, DuckDB and Daft.

For each view and round, each engine builds the view into its own flat lake under
<DELTALAKE_OUTPUT_BUCKET>/<datastack>/v<version>/_pilot/<run_id>/r<round>/<engine>/<view>
(see workflows/view_pilot.py). Postgres and DuckDB run as one Ray task each on a worker
(RAY_VIEW_PILOT_RESOURCES, JSON Ray task options, sizes it, default one CPU so it fits any
worker; DuckDB uses that many threads);
Daft runs from the driver on the whole Ray cluster. Engines run one at a time, so timings
don't share a machine. The results -- seconds, rows, bytes, checksum per build, and whether
the engines agree per view -- are logged and written to <run>/results.json.

Usage::

    python -m materializationengine.rayjobs.entrypoints.view_pilot \\
        --datastack minnie65_phase3_v1 --version 2253 --run-id abc123 \\
        --views synapses_pni_2_in_out_degree connections_with_nuclei --rounds 1
"""

import argparse
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("ray.view_pilot")


def _task_options() -> dict:
    raw = os.environ.get("RAY_VIEW_PILOT_RESOURCES", "").strip()
    options = json.loads(raw) if raw else {"num_cpus": 1}
    if not isinstance(options, dict):
        raise SystemExit("RAY_VIEW_PILOT_RESOURCES must be a JSON object of Ray task options")
    return options


def main() -> int:
    parser = argparse.ArgumentParser(description="Build views with Postgres, DuckDB and Daft, and compare")
    parser.add_argument("--datastack", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--views", nargs="+", required=True)
    parser.add_argument("--engines", nargs="+", default=["postgres", "duckdb", "daft"])
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--duckdb-memory", default=None, help="DuckDB memory_limit, e.g. 24GB")
    parser.add_argument("--attempts", type=int, default=3,
                        help="tries per Daft build: a lost worker fails the build, which is run again")
    args = parser.parse_args()

    from materializationengine.app import create_app
    from materializationengine.database import db_manager
    from materializationengine.info_client import get_datastack_info
    from materializationengine.utils import get_config_param
    from materializationengine.workflows import view_pilot as vp
    from materializationengine.workflows.deltalake_export import _build_frozen_db_connection_string

    app = create_app()
    with app.app_context():
        info = get_datastack_info(args.datastack)
        aligned_volume = info["aligned_volume"]["name"]
        pcg = info["segmentation_source"].split("/")[-1]
        frozen = f"{args.datastack}__mat{args.version}"
        connection_string = _build_frozen_db_connection_string(
            get_config_param("SQLALCHEMY_DATABASE_URI"), args.datastack, args.version)
        defs, cols = vp.view_definitions(db_manager.get_engine(frozen))
        missing = [v for v in args.views if v not in defs]
        if missing:
            logger.error("not views of %s: %s", frozen, missing)
            return 1
        lakes = vp.version_lakes(aligned_volume, args.datastack, args.version)
        run_uri = f"{get_config_param('DELTALAKE_OUTPUT_BUCKET')}/{args.datastack}/v{args.version}/_pilot/{args.run_id}"
        plans = {}
        for view in args.views:
            try:
                plans[view] = vp.plan_view(view, defs, cols, lakes, pcg)
                logger.info("%s reads %s:\n%s", view, sorted(plans[view].lakes), plans[view].sql)
            except Exception as e:
                logger.error("%s: no lake plan: %s: %s", view, type(e).__name__, e)

    import ray

    ray.init()
    logger.info("ray cluster resources: %s", ray.cluster_resources())
    options = _task_options()
    threads = max(1, int(options.get("num_cpus", 1)))

    @ray.remote(max_retries=0)
    def on_worker(engine, view, out_uri, plan, lakes, connection_string, columns, threads, duckdb_memory):
        from materializationengine.rayjobs.upload_tasks import worker_app
        from materializationengine.workflows import view_pilot as vp

        with worker_app().app_context():
            return vp.run_one(engine, view, out_uri, plan=plan, lakes=lakes, connection_string=connection_string,
                              columns=columns, duckdb_threads=threads, duckdb_memory=duckdb_memory)

    if "daft" in args.engines:
        import daft

        daft.set_runner_ray()  # the cluster ray.init() joined

    results = []
    for r in range(1, args.rounds + 1):
        for view in args.views:
            for engine in args.engines:
                out_uri = f"{run_uri}/r{r}/{engine}/{view}"
                if engine != "postgres" and view not in plans:
                    results.append({"engine": engine, "view": view, "round": r, "error": "no lake plan"})
                    continue
                plan = plans.get(view)
                if engine == "daft":
                    result = vp.run_one(engine, view, out_uri, plan=plan, lakes=lakes, attempts=args.attempts)
                else:
                    result = ray.get(on_worker.options(**options).remote(
                        engine, view, out_uri, plan, lakes, connection_string, cols[view], threads,
                        args.duckdb_memory))
                result["round"] = r
                logger.info("result: %s", json.dumps(result))
                results.append(result)

    summary = {"datastack": args.datastack, "version": args.version, "run_id": args.run_id,
               "task_options": options, "results": results, "agreement": vp.compare(results),
               "sql": {v: p.sql for v, p in plans.items()}}
    logger.info("agreement: %s", json.dumps(summary["agreement"]))
    import fsspec

    with fsspec.open(f"{run_uri}/results.json", "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    logger.info("wrote %s/results.json", run_uri)
    return 0 if all(not r.get("error") for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
