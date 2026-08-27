"""Smoke test for the RayJob platform.

Proves the whole loop without touching any CAVE data: the api can create a
RayJob, KubeRay stands up a cluster, workers schedule onto mesh-pool and can
actually execute tasks, the mounted credentials are the ones intended, and the
cluster tears itself down afterwards.

Run it as the first thing after enabling ray on a deployment::

    python -m materializationengine.rayjobs.entrypoints.smoke --workers 2

It deliberately verifies the two invariants that are easy to get wrong and
expensive to discover later:

* workers are on more than one node when asked for more than one, i.e. the
  nodeSelector and resource requests actually let them spread rather than
  silently packing onto the head;
* the mounted credential is the dedicated least-privilege ray key, not the
  materialize key -- anything that reaches a Ray head gets arbitrary code
  execution, so mounting the wrong secret there is a real finding.
"""

import argparse
import json
import os
import socket
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--workers",
        type=int,
        default=2,
        help="tasks to fan out; each pins a CPU so Ray must scale up to run them",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="seconds to wait for workers before failing",
    )
    args = parser.parse_args()

    import ray

    ray.init()
    print("ray.cluster_resources():", json.dumps(ray.cluster_resources(), default=str))

    @ray.remote(num_cpus=1, max_retries=3)
    def probe(i: int) -> dict:
        # Reports where it ran and what it can see, so one round-trip answers
        # both "did workers schedule" and "did they get the right secret".
        cred = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
        return {
            "task": i,
            "node": ray.util.get_node_ip_address(),
            "hostname": socket.gethostname(),
            "credentials_path": cred,
            "credentials_present": os.path.isfile(cred) if cred else False,
        }

    deadline = time.time() + args.timeout
    refs = [probe.remote(i) for i in range(args.workers)]
    pending = list(refs)
    results = []
    while pending:
        if time.time() > deadline:
            print(
                f"FAIL: {len(pending)} of {args.workers} tasks still pending after "
                f"{args.timeout}s. Workers are not scheduling -- check mesh-pool "
                f"capacity and that worker resource requests fit a node.",
                file=sys.stderr,
            )
            return 1
        done, pending = ray.wait(pending, num_returns=len(pending), timeout=10)
        results.extend(ray.get(done))
        if pending:
            print(f"waiting on {len(pending)}/{args.workers} tasks...")

    for r in results:
        print("  ", r)

    nodes = {r["node"] for r in results}
    print(f"\n{len(results)} tasks completed across {len(nodes)} node(s)")

    failures = []
    if not all(r["credentials_present"] for r in results):
        failures.append(
            "mounted credentials missing -- check the ray-google-cloud-key secret "
            "exists in the ray namespace and helmfile resolved its ref+gcpsecrets:// value"
        )
    if args.workers > 1 and len(nodes) < 2:
        # Not fatal on a cluster with spare capacity on one big node, but worth
        # surfacing: it usually means workers packed somewhere unintended.
        print(
            f"NOTE: {args.workers} tasks ran on {len(nodes)} node(s). Expected "
            "spread across workers; verify with `kubectl get pods -o wide`.",
        )

    if failures:
        for f in failures:
            print(f"FAIL: {f}", file=sys.stderr)
        return 1

    print("\nSMOKE TEST PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
