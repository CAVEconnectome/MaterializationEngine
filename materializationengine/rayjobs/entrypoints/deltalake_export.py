"""Delta Lake export driven by Ray instead of Celery.

Runs the same export pipeline as ``deltalake:write_deltalake_table`` -- literally
the same function, :func:`~materializationengine.workflows.deltalake_export.run_deltalake_export`
-- with two differences that address the failure modes recorded in the chart's
values.yaml:

**No broker.** The RayJob CR is the unit of work. There is no ack to expire, so
the 2026-08-25 incident where a ``visibility_timeout`` of 21600 redelivered a task
3,677 seconds into its run, and two workers then wrote the same lake, cannot
recur. There is also no ``soft_time_limit`` of 21000: the bound is the RayJob's
``activeDeadlineSeconds``, set per deployment.

**Optimize fans out.** Each output spec is a separate Delta table, so the final
z-order/compact/vacuum pass is embarrassingly parallel across specs. It ran
serially with ``max_concurrent_tasks=1`` only because one Celery pod could not
afford two z-orders at once -- that is the setting the OOMKills were tuned
around. Here each spec gets its own Ray worker with its own memory budget.

Why the fan-out is per *spec* and not finer: parallelism across specs touches
different Delta tables and therefore different transaction logs. Concurrent
writers against a *single* table would be correct -- Delta appends are
blind-append and delta-rs resolves the optimistic-concurrency race -- but they
would serialise on that table's commit ``.json``, a single GCS object limited to
roughly one mutation per second, and contend into 429s. Different tables, no
contention.

Deliberately unchanged: the streaming read and ``_flush_buffer``. That code
carries hard-won memory tuning (documented at 6.8x buffer size down to 3.6x, and
a sustained peak from 5.3 GB to 3.4 GB) and its own incident history. It keeps
running on the driver, which under Ray is a dedicated per-job head pod sized
independently of the Celery fleet rather than a long-lived shared worker.

Usage::

    python -m materializationengine.rayjobs.entrypoints.deltalake_export \\
        --datastack minnie65_phase3_v1 --version 1822 --table synapses_pni_2
"""

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("ray.deltalake_export")

# Memory ceiling for one optimize task, and what Ray schedules against. z-order
# is the memory-hungry phase; requesting it explicitly stops Ray packing several
# onto a worker that cannot hold them, which is the failure the serial
# max_concurrent_tasks=1 was working around in the first place.
DEFAULT_OPTIMIZE_MEMORY_BYTES = 6 * 1024 * 1024 * 1024


def _build_ray_optimize_runner(memory_bytes: int, max_concurrent_tasks: int):
    """Return an ``optimize_runner`` that fans specs out across Ray workers."""
    import ray

    from materializationengine.workflows.deltalake_export import optimize_deltalake

    @ray.remote(max_retries=3)
    def _optimize_one(uri, zorder_columns, bloom_filter_columns, fpp, kwargs) -> str:
        # max_retries covers preemption: workers run on the preemptible
        # mesh-pool, and optimize is idempotent -- it rewrites files and commits
        # a new version, so re-running after a lost node converges rather than
        # corrupting.
        optimize_deltalake(
            uri,
            zorder_columns=zorder_columns,
            bloom_filter_columns=bloom_filter_columns,
            fpp=fpp,
            **kwargs,
        )
        return uri

    def _runner(jobs, optimize_kwargs, optimize_callback=None) -> None:
        kwargs = dict(optimize_kwargs)
        # The per-pod serial compromise no longer applies: each task owns a
        # worker, so let delta-rs use it.
        kwargs["max_concurrent_tasks"] = max_concurrent_tasks

        logger.info(
            "optimizing %d Delta Lake(s) in parallel (%.1f GiB, %d merge tasks each)",
            len(jobs),
            memory_bytes / 1024**3,
            max_concurrent_tasks,
        )

        pending = {}
        for spec, uri in jobs:
            if optimize_callback is not None:
                optimize_callback(
                    spec.name, "z_order" if spec.zorder_columns else "compact"
                )
            ref = _optimize_one.options(memory=memory_bytes).remote(
                uri,
                spec.zorder_columns or None,
                spec.bloom_filter_columns or None,
                spec.bloom_filter_fpp or 0.001,
                kwargs,
            )
            pending[ref] = spec

        # Report each lake as it lands rather than waiting for all of them, so
        # the Redis phase/log the UI polls keeps moving. A failure is raised
        # immediately, with the remaining tasks cancelled -- a half-optimized set
        # of lakes should fail the job loudly, not look like a success.
        outstanding = list(pending)
        failures = []
        while outstanding:
            done, outstanding = ray.wait(outstanding, num_returns=1)
            for ref in done:
                spec = pending[ref]
                try:
                    ray.get(ref)
                except Exception as exc:
                    logger.exception("optimize failed for spec %s", spec.name)
                    failures.append((spec.name, exc))
                    continue
                logger.info("optimized %s", spec.name)
                if optimize_callback is not None:
                    optimize_callback(spec.name, "vacuum")

        if failures:
            for ref in outstanding:
                ray.cancel(ref, force=True)
            names = ", ".join(name for name, _ in failures)
            raise RuntimeError(
                f"optimize failed for {len(failures)} spec(s): {names}"
            ) from failures[0][1]

    return _runner


def main() -> int:
    parser = argparse.ArgumentParser(description="Export a table to Delta Lake via Ray")
    parser.add_argument("--datastack", required=True)
    parser.add_argument("--version", type=int, required=True)
    parser.add_argument("--table", required=True, help="annotation table name")
    parser.add_argument(
        "--job-id",
        default=None,
        help="disambiguates Redis progress keys; should match what the api issued",
    )
    parser.add_argument(
        "--output-specs",
        default=None,
        help="JSON list of output spec dicts; defaults are derived from indexes",
    )
    parser.add_argument(
        "--optimize-memory-bytes",
        type=int,
        default=DEFAULT_OPTIMIZE_MEMORY_BYTES,
        help="memory Ray reserves per optimize task",
    )
    parser.add_argument(
        "--optimize-max-concurrent-tasks",
        type=int,
        default=4,
        help="delta-rs merge tasks within a single optimize (1 was the per-pod compromise)",
    )
    parser.add_argument(
        "--serial-optimize",
        action="store_true",
        help="run optimize on the driver instead of fanning out, for comparison",
    )
    args = parser.parse_args()

    output_specs = json.loads(args.output_specs) if args.output_specs else None
    if output_specs is not None and not isinstance(output_specs, list):
        print("--output-specs must be a JSON list", file=sys.stderr)
        return 2

    # The Flask app config carries SQLALCHEMY_DATABASE_URI, DELTALAKE_OUTPUT_BUCKET
    # and the DELTALAKE_* tuning knobs. get_config_param reads it through the app
    # context, so the export needs one exactly as the celery worker does.
    from materializationengine.app import create_app
    from materializationengine.info_client import get_datastack_info
    from materializationengine.workflows.deltalake_export import run_deltalake_export

    app = create_app()
    with app.app_context():
        datastack_info = get_datastack_info(args.datastack)

        optimize_runner = None
        if not args.serial_optimize:
            import ray

            ray.init()
            logger.info("ray cluster resources: %s", ray.cluster_resources())
            optimize_runner = _build_ray_optimize_runner(
                args.optimize_memory_bytes, args.optimize_max_concurrent_tasks
            )

        run_deltalake_export(
            datastack_info,
            args.version,
            args.table,
            output_specs=output_specs,
            job_id=args.job_id,
            optimize_runner=optimize_runner,
        )

    logger.info("export complete: %s v%s", args.table, args.version)
    return 0


if __name__ == "__main__":
    sys.exit(main())
