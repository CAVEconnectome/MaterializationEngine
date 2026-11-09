"""Maintenance Jobs: the Job built from the template, and the status read back from it."""

import json
import re
from types import SimpleNamespace as NS
from unittest import mock

import pytest
import yaml

from materializationengine import k8s_jobs

TEMPLATE = {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "metadata": {"name": "template", "namespace": "elsewhere", "labels": {"app": "materialize-maintenance"}},
    "spec": {
        "backoffLimit": 0,
        "activeDeadlineSeconds": 172800,
        "template": {
            "metadata": {"labels": {"app": "materialize-maintenance"}},
            "spec": {
                "restartPolicy": "Never",
                "containers": [{"name": "job", "env": [{"name": "REDIS_HOST", "value": "redis"},
                                                       {"name": "MAINTENANCE_JOB_ARGS", "value": "stale"}]}],
                "initContainers": [{"name": "cloudsql-proxy", "restartPolicy": "Always"}],
            },
        },
    },
}


class ApiError(Exception):
    def __init__(self, status):
        super().__init__(f"status {status}")
        self.status = status


@pytest.fixture
def apis(tmp_path, monkeypatch):
    path = tmp_path / "job.yaml"
    path.write_text(yaml.safe_dump(TEMPLATE))
    monkeypatch.setenv("MAINTENANCE_JOB_TEMPLATE_PATH", str(path))
    monkeypatch.setenv("MAINTENANCE_JOB_NAMESPACE", "default")
    batch, core = mock.MagicMock(), mock.MagicMock()
    core.list_namespaced_pod.return_value = NS(items=[])
    with mock.patch.object(k8s_jobs, "_apis", return_value=(batch, core)):
        yield batch, core


def pod(phase="Failed", deleting=False, terminated=None, reason=None):
    return NS(metadata=NS(name="p", deletion_timestamp="2026-11-09T06:00:00Z" if deleting else None),
              status=NS(phase=phase, reason=reason, message=None,
                        container_statuses=[NS(name="job", state=NS(terminated=terminated, waiting=None))]))


def job(conditions=(), active=0, succeeded=0, failed=0):
    return NS(status=NS(active=active, succeeded=succeeded, failed=failed,
                        conditions=[NS(type=t, status="True", reason=r, message=m) for t, r, m in conditions]))


class TestSubmit:
    def test_module_args_name_and_labels(self, apis):
        batch, _ = apis
        name = k8s_jobs.job_name("repack-Synapses_PNI_2")
        assert re.fullmatch(r"repack-synapses-pni-2-[0-9a-f]{6}", name) and name != k8s_jobs.job_name("repack-Synapses_PNI_2")
        assert k8s_jobs.submit_job("materializationengine.jobs.repack", {"job_id": "db.t.1", "jobs": 2}, name=name,
                                   labels={"materialize-workload": "repack", "repack-database": "minnie65_phase3_v1__mat1926"}) == name
        body = batch.create_namespaced_job.call_args.kwargs["body"]
        assert body["metadata"]["name"] == name and "generateName" not in body["metadata"]
        assert body["metadata"]["labels"]["materialize-workload"] == "repack"
        assert body["spec"]["template"]["metadata"]["labels"]["repack-database"] == "minnie65_phase3_v1__mat1926"
        env = {e["name"]: e["value"] for e in body["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["MAINTENANCE_JOB_MODULE"] == "materializationengine.jobs.repack"
        assert json.loads(env["MAINTENANCE_JOB_ARGS"]) == {"job_id": "db.t.1", "jobs": 2}  # replaced, not duplicated
        assert env["REDIS_HOST"] == "redis"
        assert body["spec"]["backoffLimit"] == 0 and body["spec"]["activeDeadlineSeconds"] == 172800
        # created where status reads look, not where the template says
        assert batch.create_namespaced_job.call_args.kwargs["namespace"] == "default" == body["metadata"]["namespace"]

    def test_deadline_override(self, apis):
        batch, _ = apis
        k8s_jobs.submit_job("m", {}, name="repack-x-1", active_deadline_seconds=3600)
        assert batch.create_namespaced_job.call_args.kwargs["body"]["spec"]["activeDeadlineSeconds"] == 3600

    def test_missing_or_wrong_template(self, apis, tmp_path, monkeypatch):
        monkeypatch.setenv("MAINTENANCE_JOB_TEMPLATE_PATH", str(tmp_path / "nope.yaml"))
        with pytest.raises(k8s_jobs.JobSubmissionError, match="not found"):
            k8s_jobs.submit_job("m", {}, name="x-1")
        (tmp_path / "ray.yaml").write_text(yaml.safe_dump({"kind": "RayJob"}))
        monkeypatch.setenv("MAINTENANCE_JOB_TEMPLATE_PATH", str(tmp_path / "ray.yaml"))
        with pytest.raises(k8s_jobs.JobSubmissionError, match="not a Job"):
            k8s_jobs.submit_job("m", {}, name="x-1")

    def test_create_failure_is_a_submission_error(self, apis):
        batch, _ = apis
        batch.create_namespaced_job.side_effect = ApiError(403)
        with pytest.raises(k8s_jobs.JobSubmissionError, match="failed to create Job"):
            k8s_jobs.submit_job("m", {}, name="x-1")


class TestStatus:
    def test_running(self, apis):
        batch, _ = apis
        batch.read_namespaced_job.return_value = job(active=1)
        s = k8s_jobs.get_job_status("j")
        assert s["exists"] and s["active"] and not s["finished"]

    def test_complete(self, apis):
        batch, _ = apis
        batch.read_namespaced_job.return_value = job([("Complete", None, None)], succeeded=1)
        s = k8s_jobs.get_job_status("j")
        assert s["finished"] and s["succeeded"] and not s["failed"]

    def test_failure_reason_includes_the_pod(self, apis):
        batch, core = apis
        batch.read_namespaced_job.return_value = job([("Failed", "BackoffLimitExceeded", "Job has reached the specified backoff limit")], failed=1)
        core.list_namespaced_pod.return_value = NS(items=[pod(terminated=NS(exit_code=137, reason="OOMKilled"))])
        s = k8s_jobs.get_job_status("j")
        assert s["failed"] and "BackoffLimitExceeded" in s["reason"] and "OOMKilled (exit 137)" in s["reason"]
        assert core.list_namespaced_pod.call_args.kwargs["label_selector"] == "job-name=j"

    def test_deadline_exceeded(self, apis):
        batch, _ = apis
        batch.read_namespaced_job.return_value = job([("Failed", "DeadlineExceeded", "Job was active longer than specified deadline")])
        assert "DeadlineExceeded" in k8s_jobs.get_job_status("j")["reason"]

    def test_a_deleted_job_is_a_failure(self, apis):
        batch, _ = apis
        batch.read_namespaced_job.side_effect = ApiError(404)
        s = k8s_jobs.get_job_status("j")
        assert not s["exists"] and s["finished"] and s["failed"]

    def test_a_deleted_job_whose_pod_is_still_terminating_is_not_finished(self, apis):
        batch, core = apis
        batch.read_namespaced_job.side_effect = ApiError(404)
        core.list_namespaced_pod.return_value = NS(items=[pod(phase="Running", deleting=True)])
        s = k8s_jobs.get_job_status("j")
        assert not s["finished"] and not s["failed"] and "still terminating" in s["reason"]

    def test_a_failed_job_with_a_running_pod_is_not_finished(self, apis):
        batch, core = apis
        batch.read_namespaced_job.return_value = job([("Failed", "DeadlineExceeded", None)], failed=1)
        core.list_namespaced_pod.return_value = NS(items=[pod(phase="Running")])
        s = k8s_jobs.get_job_status("j")
        assert not s["finished"] and not s["failed"] and "still running" in s["reason"]

    def test_the_job_is_read_before_its_pods(self, apis):
        batch, core = apis
        order = []
        batch.read_namespaced_job.side_effect = lambda **k: order.append("job") or job(active=1)
        core.list_namespaced_pod.side_effect = lambda **k: order.append("pods") or NS(items=[])
        k8s_jobs.get_job_status("j")
        assert order == ["job", "pods"]

    def test_pods_that_cannot_be_listed_count_as_live(self, apis):
        batch, core = apis
        batch.read_namespaced_job.side_effect = ApiError(404)
        core.list_namespaced_pod.side_effect = ApiError(500)
        assert not k8s_jobs.get_job_status("j")["finished"]

    def test_other_read_errors_are_raised(self, apis):
        batch, _ = apis
        batch.read_namespaced_job.side_effect = ApiError(500)
        with pytest.raises(k8s_jobs.JobSubmissionError):
            k8s_jobs.get_job_status("j")


class TestFlag:
    def test_enabled_flag(self, monkeypatch):
        monkeypatch.delenv("MAINTENANCE_JOBS_ENABLED", raising=False)
        assert not k8s_jobs.maintenance_jobs_enabled()
        monkeypatch.setenv("MAINTENANCE_JOBS_ENABLED", "true")
        assert k8s_jobs.maintenance_jobs_enabled()
