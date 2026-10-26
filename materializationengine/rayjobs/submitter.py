"""Create and observe RayJob custom resources.

Kept deliberately thin. Everything environmental -- image, node pools, secret
mounts, the cloud-sql-proxy sidecar, resource requests -- comes from the Helm
rendered template mounted at ``RAY_JOB_TEMPLATE_PATH``. This module patches in
only what varies per submission: the entrypoint, the worker ceiling, and env.
"""

import hashlib
import logging
import os
import re
import threading

import yaml

logger = logging.getLogger(__name__)

GROUP = "ray.io"
VERSION = "v1"
PLURAL = "rayjobs"

# Terminal values of .status.jobStatus. KubeRay also reports a separate
# .status.jobDeploymentStatus for the cluster lifecycle; a job can be FAILED
# without ever reaching RUNNING (e.g. the image never pulled), so callers must
# treat both as authoritative rather than waiting for jobStatus alone.
TERMINAL_JOB_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})

# Labels on every bulk upload RayJob, for the cleanup CronJob to find them by.
UPLOAD_WORKLOAD_LABEL = "materialize-workload"
UPLOAD_WORKLOAD_VALUE = "bulk-upload"
UPLOAD_JOB_LABEL = "upload-job"

_client_lock = threading.Lock()
_custom_objects_api = None


class RayJobSubmissionError(RuntimeError):
    """Raised when a RayJob cannot be created or read."""


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return raw.lower() in ("1", "true", "yes")


def ray_enabled() -> bool:
    """Whether the Ray *platform* is available to this deployment.

    Set by the chart only when ``ray.enabled``; absent everywhere else, so the
    Celery path stays the default and this whole subsystem is inert until a
    deployment explicitly turns it on.

    This answers "can I submit a RayJob at all", not "should this particular
    workload use Ray" -- see :func:`ray_deltalake_export_enabled`.
    """
    return _env_flag("RAY_ENABLED")


def ray_deltalake_export_enabled() -> bool:
    """Whether Delta Lake exports should be routed through Ray.

    Deliberately a separate switch from :func:`ray_enabled`. Having the platform
    installed and routing production exports through it are different decisions:
    an operator will want the operator, namespace and RBAC in place while
    exports still run on Celery, and will want to roll a misbehaving Ray path
    back to Celery by flipping one chart value -- not by tearing the platform
    down and losing any in-flight job's records with it.

    Chart: ``ray.deltalakeExport``, which is itself gated on ``ray.enabled``, so
    this can never be true without a platform to run on. The `and` below keeps
    that invariant even if the env vars are set by hand.
    """
    return ray_enabled() and _env_flag("RAY_DELTALAKE_EXPORT", default=True)


def ray_upload_enabled() -> bool:
    """Whether bulk uploads run as RayJobs (chart: ``ray.upload``, off by default).

    Like the export switch, gated on the platform being there at all.
    """
    return ray_enabled() and _env_flag("RAY_UPLOAD", default=False)


def dns_label(value: str, max_length: int = 40, fallback: str = "rayjob") -> str:
    """value as a DNS label prefix: lower case letters, digits and dashes."""
    label = re.sub(r"[^a-z0-9-]+", "-", value.lower())
    label = re.sub(r"-{2,}", "-", label)[:max_length].strip("-")
    return label or fallback


def label_value(value) -> str:
    """value as a Kubernetes label value (<= 63 chars of [A-Za-z0-9_.-], alphanumeric at
    both ends); a value that does not fit becomes a sha1 of itself, still unique."""
    text = str(value)
    if re.fullmatch(r"([A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?)?", text):
        return text
    return hashlib.sha1(text.encode()).hexdigest()[:40]


def _namespace() -> str:
    # Not "default": Ray's job submission API has no authentication, so RayJobs
    # are kept in their own namespace to scope RBAC.
    return os.environ.get("RAY_JOB_NAMESPACE", "ray-jobs")


def _api():
    """Lazily build a CustomObjectsApi against the pod's in-cluster identity.

    Imported and constructed lazily so that importing this module -- which the
    api blueprint does unconditionally -- neither requires the kubernetes client
    to be configured nor performs I/O at import time. Outside a cluster (tests,
    local runs) nothing here is touched unless a caller actually submits.
    """
    global _custom_objects_api
    if _custom_objects_api is not None:
        return _custom_objects_api

    with _client_lock:
        if _custom_objects_api is not None:
            return _custom_objects_api
        try:
            from kubernetes import client, config

            try:
                config.load_incluster_config()
            except config.ConfigException:
                # Developer machine with a kubeconfig; in a pod the first call
                # succeeds and this branch never runs.
                config.load_kube_config()
            _custom_objects_api = client.CustomObjectsApi()
        except Exception as exc:
            raise RayJobSubmissionError(
                f"could not initialise kubernetes client: {exc}"
            ) from exc
    return _custom_objects_api


def _load_template() -> dict:
    path = os.environ.get(
        "RAY_JOB_TEMPLATE_PATH", "/etc/materializationengine/ray/rayjob-template.yaml"
    )
    try:
        with open(path) as fh:
            template = yaml.safe_load(fh)
    except FileNotFoundError as exc:
        raise RayJobSubmissionError(
            f"RayJob template not found at {path}. It is mounted from the "
            "materializationengine-rayjob-template ConfigMap, which the chart "
            "renders only when ray.enabled is true."
        ) from exc
    except yaml.YAMLError as exc:
        raise RayJobSubmissionError(
            f"RayJob template at {path} is not valid YAML: {exc}"
        ) from exc

    if not isinstance(template, dict) or template.get("kind") != "RayJob":
        raise RayJobSubmissionError(
            f"RayJob template at {path} is not a RayJob manifest"
        )
    return template


def _apply_env(pod_spec: dict, container_key: str, env: dict) -> None:
    """Merge ``env`` into every container of a pod spec, overriding by name."""
    for container in pod_spec.get(container_key, []):
        existing = {e["name"]: e for e in container.get("env", [])}
        for name, value in env.items():
            existing[name] = {"name": name, "value": str(value)}
        container["env"] = list(existing.values())


def submit_rayjob(
    entrypoint: str,
    name_prefix: str,
    num_workers: int | None = None,
    env: dict | None = None,
    metadata: dict | None = None,
    active_deadline_seconds: int | None = None,
) -> str:
    """Create a RayJob and return its generated name.

    Returns as soon as the CR is accepted -- it does not wait for the cluster to
    come up or the job to finish. The returned name is the job id; poll it with
    :func:`get_rayjob_status`.

    Args:
        entrypoint: shell command run on the Ray head, e.g.
            ``python -m materializationengine.rayjobs.entrypoints.deltalake_export ...``
        name_prefix: becomes ``metadata.generateName``; must be DNS-label safe
            and is truncated so the generated suffix still fits in 63 chars.
        num_workers: ceiling on worker pods. Clamped to the chart's configured
            maximum so a caller cannot ask for an unbounded fan-out.
        env: extra environment variables for head and worker containers.
        metadata: extra labels recorded on the CR for later lookup; values that are not
            valid label values are recorded as a hash of themselves.
        active_deadline_seconds: overrides the template's activeDeadlineSeconds (a
            337M-row upload needs longer than the 24 h default).
    """
    job = _load_template()

    # generateName, not name: concurrent submissions for the same table must not
    # collide, and the RayJob CR outlives the pods (ttlSecondsAfterFinished) so a
    # fixed name would clash with its own predecessor.
    prefix = dns_label(name_prefix)
    job.setdefault("metadata", {})
    job["metadata"]["generateName"] = f"{prefix}-"
    job["metadata"].pop("name", None)
    if metadata:
        labels = job["metadata"].setdefault("labels", {})
        labels.update({k: label_value(v) for k, v in metadata.items()})

    spec = job.setdefault("spec", {})
    spec["entrypoint"] = entrypoint
    if active_deadline_seconds:
        spec["activeDeadlineSeconds"] = int(active_deadline_seconds)

    worker_groups = spec.get("rayClusterSpec", {}).get("workerGroupSpecs") or []
    if not worker_groups:
        raise RayJobSubmissionError("RayJob template declares no workerGroupSpecs")

    if num_workers is not None:
        ceiling = int(
            os.environ.get(
                "RAY_JOB_MAX_WORKERS", worker_groups[0].get("maxReplicas", 20)
            )
        )
        requested = max(1, int(num_workers))
        if requested > ceiling:
            logger.warning(
                "requested %s ray workers, clamping to configured ceiling %s",
                requested,
                ceiling,
            )
            requested = ceiling
        worker_groups[0]["maxReplicas"] = requested
        # minReplicas stays 0: the in-tree autoscaler grows the group on demand,
        # so a job whose fan-out turns out to be small never pays for the rest.
        worker_groups[0]["replicas"] = 0

    if env:
        head_spec = spec["rayClusterSpec"]["headGroupSpec"]["template"]["spec"]
        _apply_env(head_spec, "containers", env)
        for group in worker_groups:
            _apply_env(group["template"]["spec"], "containers", env)

    namespace = job.get("metadata", {}).get("namespace") or _namespace()
    try:
        created = _api().create_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=namespace,
            plural=PLURAL,
            body=job,
        )
    except RayJobSubmissionError:
        raise
    except Exception as exc:
        raise RayJobSubmissionError(
            f"failed to create RayJob in {namespace}: {exc}"
        ) from exc

    job_name = created["metadata"]["name"]
    logger.info(
        "submitted RayJob %s in %s (maxReplicas=%s): %s",
        job_name,
        namespace,
        worker_groups[0].get("maxReplicas"),
        entrypoint,
    )
    return job_name


def get_rayjob_status(name: str) -> dict:
    """Return a normalised status for a RayJob.

    ``jobStatus`` alone is not sufficient: a job that never started -- image pull
    failure, unschedulable head -- can go straight to a terminal
    ``jobDeploymentStatus`` while ``jobStatus`` stays empty, so both are
    reported and ``finished`` accounts for both.
    """
    namespace = _namespace()
    try:
        obj = _api().get_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=namespace,
            plural=PLURAL,
            name=name,
        )
    except Exception as exc:
        raise RayJobSubmissionError(
            f"failed to read RayJob {name} in {namespace}: {exc}"
        ) from exc

    return normalise_status(name, namespace, obj)


def normalise_status(name: str, namespace: str, obj: dict) -> dict:
    """The status fields of a RayJob CR, with ``finished`` and ``retrying``.

    With backoffLimit, a failed attempt reports jobStatus FAILED while KubeRay sets
    jobDeploymentStatus Retrying and starts the next attempt; that job is not finished.
    """
    status = obj.get("status") or {}
    job_status = status.get("jobStatus") or ""
    deployment_status = status.get("jobDeploymentStatus") or ""
    retrying = deployment_status == "Retrying"
    return {
        "name": name,
        "namespace": namespace,
        "jobStatus": job_status,
        "jobDeploymentStatus": deployment_status,
        "rayClusterName": status.get("rayClusterName", ""),
        "startTime": status.get("startTime"),
        "endTime": status.get("endTime"),
        "message": status.get("message", ""),
        "finished": deployment_status in ("Complete", "Failed")
        or (job_status in TERMINAL_JOB_STATUSES and not retrying),
        "retrying": retrying,
        "failed": deployment_status == "Failed" or (job_status == "FAILED" and not retrying),
        "succeeded": job_status == "SUCCEEDED",
        "succeeded_attempts": status.get("succeeded"),
        "failed_attempts": status.get("failed"),
    }


def delete_rayjob(name: str) -> None:
    """Delete a RayJob and, through it, any cluster KubeRay created for it."""
    namespace = _namespace()
    try:
        _api().delete_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=namespace,
            plural=PLURAL,
            name=name,
        )
    except Exception as exc:
        if getattr(exc, "status", None) == 404:
            logger.info("RayJob %s in %s was already gone", name, namespace)
            return
        raise RayJobSubmissionError(
            f"failed to delete RayJob {name} in {namespace}: {exc}"
        ) from exc
    logger.info("deleted RayJob %s in %s", name, namespace)


def list_rayjobs(label_selector: str) -> list:
    """Normalised statuses of the RayJobs in the namespace that match label_selector."""
    namespace = _namespace()
    try:
        listing = _api().list_namespaced_custom_object(
            group=GROUP,
            version=VERSION,
            namespace=namespace,
            plural=PLURAL,
            label_selector=label_selector,
        )
    except Exception as exc:
        raise RayJobSubmissionError(
            f"failed to list RayJobs in {namespace}: {exc}"
        ) from exc
    return [
        {**normalise_status(obj["metadata"]["name"], namespace, obj), "labels": obj["metadata"].get("labels", {})}
        for obj in listing.get("items", [])
    ]


def rayjob_exists(name: str) -> bool:
    """False only if the RayJob is certainly gone (404); other errors are raised."""
    try:
        get_rayjob_status(name)
        return True
    except RayJobSubmissionError as exc:
        if getattr(exc.__cause__, "status", None) == 404:
            return False
        raise

