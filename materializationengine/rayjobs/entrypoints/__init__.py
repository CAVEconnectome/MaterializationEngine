"""Entrypoints executed inside a RayJob.

Each module here is run by ``python -m`` on the Ray head, named in a RayJob's
``spec.entrypoint``. They are ordinary scripts, not Celery tasks: the RayJob CR
is the unit of work, so there is nothing to ack and no broker to reconnect to.

Contract for anything added here:

* ``ray.init()`` with no address -- the driver runs on the head and attaches to
  the cluster KubeRay already started.
* Give every ``@ray.remote`` function a ``max_retries``. Workers run on the
  preemptible mesh-pool, so losing one mid-task is expected and must be a retry
  rather than a job failure.
* Exit non-zero on failure. That is what KubeRay reports as ``jobStatus:
  FAILED`` and what ``backoffLimit`` retries against.
"""
