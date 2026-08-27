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
import os
import sys

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)
logger = logging.getLogger("ray.deltalake_export")

# Memory reserved per optimize task.
#
# Default None: request no `memory` resource at all, and rely on num_cpus=1 to
# keep one optimize per worker. A hardcoded figure here is a number that has to
# agree with the worker's memory request in a DIFFERENT repository, and when it
# does not the failure is silent and total -- Ray marks the demand infeasible,
# the autoscaler declines to create a worker (it cannot make one big enough),
# and the job hangs until activeDeadlineSeconds fires. That is a 24-hour wait
# for a scheduling constraint that was never satisfiable, with nothing logged.
#
# The chart sets RAY_OPTIMIZE_MEMORY_BYTES from the worker's own memory request
# when it wants an explicit reservation, so the two cannot diverge.
DEFAULT_OPTIMIZE_MEMORY_BYTES = None

# How long to wait for the first optimize task to be scheduled before declaring
# the demand unsatisfiable. Generous enough for a cold mesh-pool scale-up
# (~60-90s for a node, plus image pull), short enough to be a useful error.
SCHEDULING_TIMEOUT_SECONDS = 900


def _optimize_memory_default():
    """Resolve RAY_OPTIMIZE_MEMORY_BYTES, or None to reserve no memory.

    Unset, empty and "0" all mean "reserve none" -- the safe default, where
    num_cpus=1 alone keeps one optimize per worker. A malformed value is a
    configuration error worth naming: left to argparse it surfaces as a bare
    ValueError raised while the parser is being built, before --help exists.
    """
    raw = os.environ.get("RAY_OPTIMIZE_MEMORY_BYTES", "").strip()
    if not raw:
        return DEFAULT_OPTIMIZE_MEMORY_BYTES
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(
            f"RAY_OPTIMIZE_MEMORY_BYTES must be an integer number of bytes, "
            f"got {raw!r}. Unset it (or set 0) to reserve no memory and let "
            f"num_cpus=1 keep one optimize per worker."
        ) from None
    if value < 0:
        raise SystemExit(
            f"RAY_OPTIMIZE_MEMORY_BYTES must not be negative, got {value}."
        )
    return value or DEFAULT_OPTIMIZE_MEMORY_BYTES


def _warn_if_packing_risk(ray, memory_bytes) -> None:
    """Warn when workers could pack several optimizes onto one node.

    With no memory reservation, `num_cpus=1` is the ONLY thing keeping one
    z-order per worker, and that holds only while Ray believes a worker has one
    CPU. The chart pins that via rayStartParams.num-cpus, but this image can be
    run against a cluster that does not -- a hand-written RayJob, a plain Ray
    cluster, a laptop -- where Ray auto-detects and a 4-vCPU node advertises 4.
    Four concurrent z-orders on one small worker is how the original OOMKills
    happened, so make the condition visible rather than silently likely.
    """
    if memory_bytes:
        return  # an explicit reservation already bounds co-location
    try:
        workers = [
            n
            for n in ray.nodes()
            if n.get("Alive") and not n.get("Resources", {}).get("node:__internal_head__")
        ]
        crowded = [n for n in workers if n.get("Resources", {}).get("CPU", 0) > 1]
    except Exception:  # diagnostics must never break the export
        return
    if crowded:
        cpus = sorted({int(n["Resources"]["CPU"]) for n in crowded})
        logger.warning(
            "%d worker node(s) advertise %s CPUs, so Ray may run that many "
            "z-orders concurrently on one node with no memory reservation to "
            "stop it. Set RAY_OPTIMIZE_MEMORY_BYTES (<= the worker memory "
            "request), or pin rayStartParams.num-cpus to 1 as the chart does.",
            len(crowded),
            "/".join(str(c) for c in cpus),
        )


def _fail_if_unschedulable(ray, refs, options) -> None:
    """Raise if no task has started within SCHEDULING_TIMEOUT_SECONDS.

    An over-large resource request does not error in Ray -- it is simply never
    satisfiable, so the autoscaler declines to add a node (it cannot build one
    that fits) and the tasks pend silently until activeDeadlineSeconds kills the
    job hours later, with nothing in the logs to say why. That happened: a 6 GiB
    memory request against workers declaring 3 Gi.

    A wait that is only ever going to time out should say so, and say what to
    change.
    """
    import time

    deadline = time.time() + SCHEDULING_TIMEOUT_SECONDS
    while time.time() < deadline:
        ready, _ = ray.wait(refs, num_returns=1, timeout=15)
        if ready:
            return
        # Any worker joining means the demand was satisfiable after all.
        if any(k for k in ray.cluster_resources() if k.startswith("node:")) and len(
            ray.nodes()
        ) > 1:
            return
        logger.info("waiting for a worker to accept the optimize tasks...")

    demand = ", ".join(f"{k}={v}" for k, v in sorted(options.items()))
    raise RuntimeError(
        f"no optimize task was scheduled within {SCHEDULING_TIMEOUT_SECONDS}s. "
        f"Each task requests {demand}, and Ray reports cluster resources "
        f"{ray.cluster_resources()}. If the request exceeds what one worker "
        f"declares, the autoscaler cannot create a node that fits and will not "
        f"try -- the job would otherwise hang until activeDeadlineSeconds. "
        f"Lower RAY_OPTIMIZE_MEMORY_BYTES, or raise the worker's memory request "
        f"(ray.worker.resources.requests.memory in the chart)."
    )


def _build_ray_optimize_runner(memory_bytes, max_concurrent_tasks: int):
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
            "optimizing %d Delta Lake(s) in parallel (memory reservation: %s, "
            "%d merge tasks each)",
            len(jobs),
            f"{memory_bytes / 1024**3:.1f} GiB" if memory_bytes else "none (num_cpus only)",
            max_concurrent_tasks,
        )

        # num_cpus=1 is what keeps one optimize per worker. `memory` is only
        # added when explicitly configured -- see DEFAULT_OPTIMIZE_MEMORY_BYTES
        # for why an unsatisfiable default is worse than none.
        options = {"num_cpus": 1}
        if memory_bytes:
            options["memory"] = memory_bytes

        pending = {}
        for spec, uri in jobs:
            if optimize_callback is not None:
                optimize_callback(
                    spec.name, "z_order" if spec.zorder_columns else "compact"
                )
            ref = _optimize_one.options(**options).remote(
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
        _fail_if_unschedulable(ray, outstanding, options)
        # After the first task schedules, at least one worker exists to inspect.
        _warn_if_packing_risk(ray, memory_bytes)
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
        default=_optimize_memory_default(),
        help=(
            "memory Ray reserves per optimize task; 0/unset requests none and "
            "relies on num_cpus=1. The chart sets RAY_OPTIMIZE_MEMORY_BYTES from "
            "the worker's own memory request so the two cannot diverge"
        ),
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
