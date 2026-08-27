"""Ephemeral RayJob submission for long-running materialization work.

This package is deliberately named ``rayjobs`` rather than ``ray``: a
``materializationengine.ray`` package would shadow the real ``ray`` distribution
for any module inside it, so the entrypoints could not import the thing they
exist to run.

The pattern here replaces Celery for jobs Celery struggles with. Instead of a
task on a Redis-backed queue, the api creates a Kubernetes ``RayJob`` custom
resource and that CR becomes the durable unit of work:

* no broker, so no ``visibility_timeout`` and no redelivery of a task that is
  still running,
* ``backoffLimit`` handles retries and ``activeDeadlineSeconds`` bounds runaways,
* ``shutdownAfterJobFinishes`` reclaims the whole Ray cluster on completion, so
  nothing is running -- or billing -- between jobs.

The pod spec itself is not built here. It is rendered by Helm into a ConfigMap
(``templates/ray_job_template_configmap.yaml``) and mounted into the api pods, so
node pools, image tags, secret names and the cloud-sql-proxy sidecar stay in the
chart where the rest of that configuration already lives.
"""

from materializationengine.rayjobs.submitter import (
    RayJobSubmissionError,
    delete_rayjob,
    get_rayjob_status,
    ray_enabled,
    submit_rayjob,
)

__all__ = [
    "RayJobSubmissionError",
    "delete_rayjob",
    "get_rayjob_status",
    "ray_enabled",
    "submit_rayjob",
]
