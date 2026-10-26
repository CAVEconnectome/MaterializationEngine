"""RayJob submission: names and labels Kubernetes accepts, retrying jobs, deadlines."""

from unittest import mock

import pytest
import yaml

from materializationengine.rayjobs import submitter

TEMPLATE = {
    "apiVersion": "ray.io/v1",
    "kind": "RayJob",
    "metadata": {"name": "template", "labels": {"app": "materialize"}},
    "spec": {
        "activeDeadlineSeconds": 86400,
        "rayClusterSpec": {
            "headGroupSpec": {"template": {"spec": {"containers": [{"name": "ray-head"}]}}},
            "workerGroupSpecs": [{"maxReplicas": 20, "template": {"spec": {"containers": [{"name": "w"}]}}}],
        },
    },
}


@pytest.fixture
def api(tmp_path, monkeypatch):
    path = tmp_path / "rayjob.yaml"
    path.write_text(yaml.safe_dump(TEMPLATE))
    monkeypatch.setenv("RAY_JOB_TEMPLATE_PATH", str(path))
    fake = mock.MagicMock()
    fake.create_namespaced_custom_object.side_effect = lambda **kw: {
        "metadata": {"name": kw["body"]["metadata"]["generateName"] + "abcde"}
    }
    with mock.patch.object(submitter, "_api", return_value=fake):
        yield fake


class TestNames:
    @pytest.mark.parametrize("value, expected", [
        ("upload-synapses_330m_test4", "upload-synapses-330m-test4"),
        ("Upload--MINNIE65__Phase3", "upload-minnie65-phase3"),
        ("---", "rayjob"),
        ("x" * 60, "x" * 40),
    ])
    def test_dns_label(self, value, expected):
        assert submitter.dns_label(value) == expected

    def test_label_values(self):
        assert submitter.label_value("ltv7") == "ltv7"
        job_id = "minnie65_phase3_v1_synapses_330m_test4_20261025_203742_with_a_long_suffix"
        hashed = submitter.label_value(job_id)
        assert len(hashed) == 40 and hashed == submitter.label_value(job_id)
        assert submitter.label_value("a b") != "a b"


class TestSubmit:
    def test_sanitised_name_labels_and_deadline(self, api):
        name = submitter.submit_rayjob(
            "python -m x --job-id 'j'", "upload-Synapses_330m", num_workers=4,
            metadata={"upload-job": "minnie65_phase3_v1_synapses_330m_test4_with_a_long_name_20261025_203742"},
            active_deadline_seconds=259200,
        )
        body = api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert name == "upload-synapses-330m-abcde"
        assert body["metadata"]["generateName"] == "upload-synapses-330m-"
        assert len(body["metadata"]["labels"]["upload-job"]) == 40
        assert body["spec"]["activeDeadlineSeconds"] == 259200
        assert body["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["maxReplicas"] == 4

    def test_template_deadline_by_default(self, api):
        submitter.submit_rayjob("python -m x", "upload")
        body = api.create_namespaced_custom_object.call_args.kwargs["body"]
        assert body["spec"]["activeDeadlineSeconds"] == 86400


class TestStatus:
    def status(self, job_status, deployment_status):
        return submitter.normalise_status("j", "ray-jobs", {
            "status": {"jobStatus": job_status, "jobDeploymentStatus": deployment_status}
        })

    def test_a_failed_attempt_being_retried_is_not_finished(self):
        s = self.status("FAILED", "Retrying")
        assert (s["finished"], s["retrying"], s["failed"]) == (False, True, False)

    def test_terminal_states(self):
        assert self.status("SUCCEEDED", "Complete")["finished"]
        assert self.status("SUCCEEDED", "Complete")["succeeded"]
        failed = self.status("FAILED", "Failed")
        assert failed["finished"] and failed["failed"]
        # never started: no jobStatus at all
        assert self.status("", "Failed")["finished"]
        assert not self.status("RUNNING", "Running")["finished"]


class TestDelete:
    def test_a_missing_rayjob_is_already_deleted(self):
        fake = mock.MagicMock()
        gone = Exception("Not Found")
        gone.status = 404
        fake.delete_namespaced_custom_object.side_effect = gone
        with mock.patch.object(submitter, "_api", return_value=fake):
            submitter.delete_rayjob("j")  # no error

    def test_other_errors_are_raised(self):
        fake = mock.MagicMock()
        fake.delete_namespaced_custom_object.side_effect = Exception("forbidden")
        with mock.patch.object(submitter, "_api", return_value=fake):
            with pytest.raises(submitter.RayJobSubmissionError):
                submitter.delete_rayjob("j")


class TestFlags:
    def test_upload_needs_the_platform(self, monkeypatch):
        monkeypatch.setenv("RAY_UPLOAD", "true")
        monkeypatch.delenv("RAY_ENABLED", raising=False)
        assert not submitter.ray_upload_enabled()
        monkeypatch.setenv("RAY_ENABLED", "true")
        assert submitter.ray_upload_enabled()
        monkeypatch.delenv("RAY_UPLOAD")
        assert not submitter.ray_upload_enabled()
