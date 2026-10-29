"""Upload endpoints check the user's permission on the datastack named in the request
or job record: edit to create a table, view to see another user's jobs."""

import inspect
import json
from unittest import mock

import pytest
from flask import Flask, g

from materializationengine.blueprints.upload import api

DATASTACK = "minnie65_phase3_v1"
DATASET = "minnie65"


def user(id=3771, **extra):
    return {"id": id, "permissions_v2": {}, "datasets_admin": [], **extra}


VIEWER = user(permissions_v2={DATASET: ["view"]})
EDITOR = user(permissions_v2={DATASET: ["edit", "view"]})
OTHER_DATASET_EDITOR = user(permissions_v2={"minnie65_sandbox": ["edit", "view"]})


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(
        MATERIALIZATION_UPLOAD_BUCKET_PATH="bucket",
        SQLALCHEMY_DATABASE_URI="postgresql://x/y",
        STAGING_DATABASE_NAME="staging",
    )
    return app


@pytest.fixture
def auth_on():
    with mock.patch.object(api, "is_auth_disabled", return_value=False), \
            mock.patch.object(api, "_dataset_for", return_value=DATASET):
        yield


def call(app, view, auth_user, json_body=None):
    """Run an endpoint function without its auth decorators."""
    with app.test_request_context(json=json_body):
        g.auth_user = auth_user
        response = inspect.unwrap(view)()
        if isinstance(response, tuple):
            response, code = response
        else:
            code = response.status_code
        return json.loads(response.get_data()), code


class TestHasDatastackPermission:
    @pytest.mark.parametrize(
        "auth_user, level, expected",
        [
            (VIEWER, "view", True),
            (VIEWER, "edit", False),
            (EDITOR, "edit", True),
            (EDITOR, "admin", False),
            # permission on another dataset does not count
            (OTHER_DATASET_EDITOR, "edit", False),
            (OTHER_DATASET_EDITOR, "view", False),
            (user(datasets_admin=[DATASET]), "admin", True),
            (user(datasets_admin=[DATASET]), "edit", True),
            (user(admin=True), "edit", True),
            # the old check's format: a permissions list of "<datastack>_<level>"
            (user(permissions=[f"{DATASTACK}_edit"]), "edit", False),
            (None, "view", False),
        ],
    )
    def test_levels(self, app, auth_on, auth_user, level, expected):
        with app.app_context():
            assert api._has_datastack_permission(auth_user, level, DATASTACK) is expected

    def test_dataset_lookup_failure_denies(self, app):
        with app.app_context(), \
                mock.patch.object(api, "_dataset_for", side_effect=RuntimeError("auth down")):
            assert api._has_datastack_permission(EDITOR, "edit", DATASTACK) is False


FILE_METADATA = {
    "filename": "file.csv",
    "metadata": {"datastack_name": DATASTACK, "table_name": "synapses_test"},
}


class TestStart:
    def start(self, app, auth_user):
        calls = mock.MagicMock()
        calls.process_and_upload.s.return_value.apply_async.return_value.id = "task-id"
        with mock.patch.object(api, "UploadRequestSchema") as schema, \
                mock.patch.object(api, "get_datastack_info", calls.get_datastack_info), \
                mock.patch.object(api, "ray_upload_enabled", return_value=False), \
                mock.patch.object(api, "update_job_status", calls.update_job_status), \
                mock.patch.object(api, "process_and_upload", calls.process_and_upload):
            schema.return_value.load.return_value = json.loads(json.dumps(FILE_METADATA))
            calls.get_datastack_info.return_value = {"datastack": DATASTACK}
            body, code = call(app, api.start_csv_processing, auth_user, FILE_METADATA)
        return body, code, calls

    def test_without_edit_is_refused_before_anything_starts(self, app, auth_on):
        body, code, calls = self.start(app, OTHER_DATASET_EDITOR)
        assert code == 403
        assert "edit" in body["message"]
        calls.update_job_status.assert_not_called()
        calls.process_and_upload.s.assert_not_called()

    def test_view_is_not_enough(self, app, auth_on):
        _, code, calls = self.start(app, VIEWER)
        assert code == 403
        calls.process_and_upload.s.assert_not_called()

    def test_with_edit_starts(self, app, auth_on):
        body, code, calls = self.start(app, EDITOR)
        assert code == 200 and body["task_id"] == "task-id"
        calls.process_and_upload.s.assert_called_once()


class TestSaveMetadata:
    METADATA = {
        "schema_type": "synapse",
        "datastack_name": DATASTACK,
        "table_name": "synapses_test",
        "description": "d",
        "voxel_resolution_nm_x": 4,
        "voxel_resolution_nm_y": 4,
        "voxel_resolution_nm_z": 40,
        "write_permission": "PRIVATE",
        "read_permission": "PUBLIC",
    }

    def test_without_edit_is_refused(self, app, auth_on):
        with mock.patch.object(api, "create_storage_service") as storage:
            body, code = call(app, api.save_metadata, VIEWER, self.METADATA)
        assert code == 403 and "edit" in body["message"]
        storage.return_value.save_metadata.assert_not_called()


class TestUserJobs:
    JOBS = [
        {"job_id": "mine", "user_id": "3771", "datastack_name": DATASTACK},
        {"job_id": "theirs", "user_id": "121", "datastack_name": DATASTACK},
        {"job_id": "elsewhere", "user_id": "121", "datastack_name": "fanc_production"},
    ]

    def job_ids(self, app, auth_user):
        datasets = {DATASTACK: DATASET, "fanc_production": "fanc"}
        with mock.patch.object(api, "is_auth_disabled", return_value=False), \
                mock.patch.object(api, "_dataset_for", side_effect=datasets.get), \
                mock.patch.object(api, "REDIS_CLIENT") as redis, \
                mock.patch.object(api.ray_status, "list_records", return_value=self.JOBS):
            redis.scan_iter.return_value = []
            body, code = call(app, api.get_user_jobs, auth_user)
        assert code == 200
        return sorted(j["job_id"] for j in body["jobs"])

    def test_viewers_see_the_datastacks_jobs(self, app):
        assert self.job_ids(app, user(id=5, permissions_v2={DATASET: ["view"]})) == [
            "mine",
            "theirs",
        ]

    def test_others_see_only_their_own(self, app):
        assert self.job_ids(app, OTHER_DATASET_EDITOR) == ["mine"]
