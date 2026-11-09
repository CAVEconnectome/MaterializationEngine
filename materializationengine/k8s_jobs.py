"""Run one-off maintenance work (pg_repack) as Kubernetes Jobs, and observe them.

Why not a Celery task: a repack of a large table runs for hours, and the Celery workers
that could run it are autoscaled. KEDA scales the workflow queue's deployment on queue
length and in-flight tasks, a long task acked at its start is neither, and on 2026-11-09
it scaled the producer to zero 37 minutes into a repack of minnie's synapse table. The
pod's grace period ran out 100 minutes later and took pg_repack with it, leaving its
half-built copy behind. A Job pod belongs to nothing that scales, is marked
safe-to-evict false for the cluster autoscaler, and is never retried (backoffLimit 0),
because a pg_repack that died must be cleaned up before it runs again.

Like rayjobs/submitter.py, this module is thin. The pod spec -- image, node pool, config
and secret mounts, the cloud-sql-proxy sidecar, resources, deadlines -- comes from the
Helm-rendered Job template mounted at MAINTENANCE_JOB_TEMPLATE_PATH. Per submission,
only the module to run, its arguments, the name prefix and labels are patched in.
"""

import json
import logging
import os
import threading

import yaml

from materializationengine.rayjobs.submitter import dns_label, label_value

logger = logging.getLogger(__name__)

DEFAULT_TEMPLATE_PATH = "/etc/materializationengine/jobs/maintenance-job-template.yaml"
# Read by the template's container command: `python -m $MAINTENANCE_JOB_MODULE`.
MODULE_ENV = "MAINTENANCE_JOB_MODULE"
# JSON object of keyword arguments for the module's run function.
ARGS_ENV = "MAINTENANCE_JOB_ARGS"

_client_lock = threading.Lock()
_batch_api = None
_core_api = None


class JobSubmissionError(RuntimeError):
    """Raised when a Job cannot be created or read."""


def maintenance_jobs_enabled() -> bool:
    """Whether this deployment runs maintenance work as Kubernetes Jobs.

    Set by the chart (materialize.maintenanceJobs.enabled) along with the template mount
    and the RBAC that lets the api create Jobs. Absent elsewhere (tests, deployments on an
    older chart), where callers fall back to their Celery task.
    """
    return os.environ.get("MAINTENANCE_JOBS_ENABLED", "").lower() in ("1", "true", "yes")


def _namespace() -> str:
    return os.environ.get("MAINTENANCE_JOB_NAMESPACE", "default")


def _apis():
    global _batch_api, _core_api
    if _batch_api is not None:
        return _batch_api, _core_api
    with _client_lock:
        if _batch_api is None:
            try:
                from kubernetes import client, config

                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
                _batch_api, _core_api = client.BatchV1Api(), client.CoreV1Api()
            except Exception as exc:
                raise JobSubmissionError(f"could not initialise kubernetes client: {exc}") from exc
    return _batch_api, _core_api


def _load_template() -> dict:
    path = os.environ.get("MAINTENANCE_JOB_TEMPLATE_PATH", DEFAULT_TEMPLATE_PATH)
    try:
        with open(path) as fh:
            template = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise JobSubmissionError(
            f"Job template not found at {path}. It is mounted from the "
            "materializationengine-maintenance-job-template ConfigMap, which the chart "
            "renders when materialize.maintenanceJobs.enabled is true."
        ) from exc
    except yaml.YAMLError as exc:
        raise JobSubmissionError(f"Job template at {path} is not valid YAML: {exc}") from exc
    if not isinstance(template, dict) or template.get("kind") != "Job":
        raise JobSubmissionError(f"Job template at {path} is not a Job manifest")
    return template


def submit_job(module: str, kwargs: dict, name_prefix: str, labels: dict | None = None,
               active_deadline_seconds: int | None = None) -> str:
    """Create a Job running `python -m module` with kwargs, and return its generated name.

    Returns once the Job is accepted; poll it with get_job_status.
    """
    job = _load_template()
    meta = job.setdefault("metadata", {})
    meta.pop("name", None)
    # generateName: Jobs outlive their pods (ttlSecondsAfterFinished), so a fixed name
    # would collide with its own predecessor.
    meta["generateName"] = f"{dns_label(name_prefix, fallback='maintenance')}-"
    all_labels = {k: label_value(v) for k, v in (labels or {}).items()}
    meta.setdefault("labels", {}).update(all_labels)

    spec = job.setdefault("spec", {})
    if active_deadline_seconds:
        spec["activeDeadlineSeconds"] = int(active_deadline_seconds)
    pod = spec.setdefault("template", {})
    pod.setdefault("metadata", {}).setdefault("labels", {}).update(all_labels)
    containers = pod.get("spec", {}).get("containers") or []
    if not containers:
        raise JobSubmissionError("Job template declares no containers")
    main = containers[0]  # sidecars (cloud-sql-proxy) are native: initContainers
    env = {e["name"]: e for e in main.get("env", [])}
    env[MODULE_ENV] = {"name": MODULE_ENV, "value": module}
    env[ARGS_ENV] = {"name": ARGS_ENV, "value": json.dumps(kwargs, default=str)}
    main["env"] = list(env.values())

    namespace = meta.get("namespace") or _namespace()
    batch, _ = _apis()
    try:
        created = batch.create_namespaced_job(namespace=namespace, body=job)
    except Exception as exc:
        raise JobSubmissionError(f"failed to create Job in {namespace}: {exc}") from exc
    name = created.metadata.name if hasattr(created, "metadata") else created["metadata"]["name"]
    logger.info("submitted Job %s in %s: python -m %s", name, namespace, module)
    return name


def get_job_status(name: str) -> dict:
    """Normalised status of a Job: exists, finished, succeeded, failed, and why.

    A Job deleted (or garbage collected after ttlSecondsAfterFinished) reports
    exists=False. The reason for a failure comes from the Job's Failed condition
    (DeadlineExceeded, BackoffLimitExceeded) and, when there is one, from the pod's
    terminated container state (OOMKilled, Error with an exit code) or its eviction.
    """
    namespace = _namespace()
    batch, core = _apis()
    try:
        job = batch.read_namespaced_job(name=name, namespace=namespace)
    except Exception as exc:
        if getattr(exc, "status", None) == 404:
            return {"name": name, "exists": False, "finished": True, "succeeded": False, "failed": True,
                    "reason": "the Job no longer exists"}
        raise JobSubmissionError(f"failed to read Job {name} in {namespace}: {exc}") from exc

    status = job.status
    conditions = {c.type: c for c in (status.conditions or []) if c.status == "True"}
    succeeded = "Complete" in conditions or bool(status.succeeded)
    failed = "Failed" in conditions or bool(status.failed)
    reason = None
    if "Failed" in conditions:
        c = conditions["Failed"]
        reason = f"{c.reason}: {c.message}" if c.message else c.reason
    if failed or not succeeded:
        detail = _pod_detail(core, namespace, name)
        if detail:
            reason = f"{reason}; {detail}" if reason else detail
    return {"name": name, "exists": True, "active": bool(status.active), "finished": succeeded or failed,
            "succeeded": succeeded, "failed": failed and not succeeded, "reason": reason}


def _pod_detail(core, namespace: str, job_name: str) -> str | None:
    try:
        pods = core.list_namespaced_pod(namespace=namespace, label_selector=f"job-name={job_name}").items
    except Exception:
        return None
    for pod in pods:
        if pod.status.reason:  # e.g. Evicted
            return f"pod {pod.metadata.name} {pod.status.reason}: {pod.status.message or ''}".strip()
        for cs in pod.status.container_statuses or []:
            term = cs.state.terminated if cs.state else None
            if term and term.exit_code:
                return f"container {cs.name} {term.reason or 'terminated'} (exit {term.exit_code})"
            waiting = cs.state.waiting if cs.state else None
            if waiting and waiting.reason not in (None, "ContainerCreating", "PodInitializing"):
                return f"container {cs.name} waiting: {waiting.reason}"
    return None


def job_kwargs() -> dict:
    """The keyword arguments the submitter passed, inside the Job's pod."""
    return json.loads(os.environ.get(ARGS_ENV) or "{}")
