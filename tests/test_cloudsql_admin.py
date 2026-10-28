"""Cloud SQL CSV import/export through the Admin REST API instead of the gcloud CLI."""

from unittest import mock

import pytest

from materializationengine import cloudsql_admin
from materializationengine.cloudsql_admin import CloudSQLAdminError

ROOT = cloudsql_admin.SQLADMIN_ROOT


def _response(status=200, payload=None, text=""):
    response = mock.MagicMock()
    response.ok = status < 400
    response.status_code = status
    response.json.return_value = payload or {}
    response.text = text
    return response


@pytest.fixture
def session():
    session = mock.MagicMock()
    with mock.patch.object(
        cloudsql_admin, "_session_and_default_project", return_value=(session, "key-project")
    ), mock.patch.object(cloudsql_admin, "get_config_param", return_value=None), \
            mock.patch.object(cloudsql_admin.time, "sleep"):
        yield session


class TestCloudSQLAdmin:
    """import_csv/export_csv build the same requests gcloud made and wait like it did."""

    def test_import_csv_posts_import_and_waits_for_done(self, session):
        session.post.return_value = _response(payload={"name": "op1", "status": "PENDING"})
        session.get.side_effect = [
            _response(payload={"name": "op1", "status": "RUNNING"}),
            _response(payload={"name": "op1", "status": "DONE"}),
        ]

        result = cloudsql_admin.import_csv(
            "ltv-downsize-test", "gs://bucket/processed.csv", "staging", "synapse_1m_test"
        )

        assert result["status"] == "DONE"
        url = session.post.call_args.args[0]
        assert url == f"{ROOT}/projects/key-project/instances/ltv-downsize-test/import"
        assert session.post.call_args.kwargs["json"] == {
            "importContext": {
                "kind": "sql#importContext",
                "fileType": "CSV",
                "uri": "gs://bucket/processed.csv",
                "database": "staging",
                "importUser": "postgres",
                "csvImportOptions": {"table": "synapse_1m_test"},
            }
        }
        assert session.get.call_args.args[0] == f"{ROOT}/projects/key-project/operations/op1"

    def test_operation_error_raises(self, session):
        session.post.return_value = _response(payload={"name": "op1", "status": "PENDING"})
        session.get.return_value = _response(
            payload={
                "name": "op1",
                "status": "DONE",
                "error": {"errors": [{"code": "ERROR_RDBMS", "message": "relation does not exist"}]},
            }
        )
        with pytest.raises(CloudSQLAdminError, match="relation does not exist"):
            cloudsql_admin.import_csv("inst", "gs://b/f.csv", "staging", "t")

    def test_http_error_raises_with_status(self, session):
        session.post.return_value = _response(status=403, text="permission denied")
        with pytest.raises(CloudSQLAdminError, match="HTTP 403: permission denied"):
            cloudsql_admin.import_csv("inst", "gs://b/f.csv", "staging", "t")

    def test_import_times_out(self, session):
        session.post.return_value = _response(payload={"name": "op1", "status": "PENDING"})
        session.get.return_value = _response(payload={"name": "op1", "status": "RUNNING"})
        with mock.patch.object(cloudsql_admin.time, "monotonic", side_effect=[0, 0, 5000]):
            with pytest.raises(TimeoutError):
                cloudsql_admin.import_csv("inst", "gs://b/f.csv", "staging", "t", timeout=10)

    def test_export_csv_without_wait_does_not_poll(self, session):
        session.post.return_value = _response(payload={"name": "op2", "status": "PENDING"})

        cloudsql_admin.export_csv(
            "inst", "gs://dump/ds/v1/t.csv.gz", "ds__mat1", "SELECT * from t", wait=False
        )

        assert session.post.call_args.args[0] == f"{ROOT}/projects/key-project/instances/inst/export"
        assert session.post.call_args.kwargs["json"]["exportContext"] == {
            "kind": "sql#exportContext",
            "fileType": "CSV",
            "uri": "gs://dump/ds/v1/t.csv.gz",
            "databases": ["ds__mat1"],
            "csvExportOptions": {"selectQuery": "SELECT * from t"},
        }
        session.get.assert_not_called()

    @pytest.mark.parametrize(
        "configured_project, instance_name, expected",
        [
            ("sql-project", "inst", ("sql-project", "inst")),
            (None, "conn-project:us-east1:inst", ("conn-project", "inst")),
            ("sql-project", "conn-project:us-east1:inst", ("sql-project", "inst")),
            (None, "inst", ("key-project", "inst")),
        ],
        ids=["config", "connection_name", "config_over_connection_name", "credentials"],
    )
    def test_resolve_instance(self, configured_project, instance_name, expected):
        with mock.patch.object(cloudsql_admin, "get_config_param", return_value=configured_project):
            assert cloudsql_admin.resolve_instance(instance_name, "key-project") == expected

    def test_bearer_session_sends_only_authorization_header(self):
        credentials = mock.MagicMock(valid=False, token="tok")
        bearer = cloudsql_admin._BearerTokenSession(credentials)
        with mock.patch.object(bearer, "_http") as http:
            bearer.post(f"{ROOT}/x", json={}, timeout=60)
            bearer.get(f"{ROOT}/y", timeout=60)

        credentials.refresh.assert_called()
        # no x-allowed-locations: the Cloud SQL Admin front end rejects it with a 400
        assert http.post.call_args.kwargs["headers"] == {"Authorization": "Bearer tok"}
        assert http.get.call_args.kwargs["headers"] == {"Authorization": "Bearer tok"}

    def test_credentials_request_cloud_platform_scope(self):
        # instances.import rejects tokens that only have sqlservice.admin
        with mock.patch.object(
            cloudsql_admin.google.auth, "default", return_value=(mock.MagicMock(), "p")
        ) as default:
            cloudsql_admin._session_and_default_project()
        assert default.call_args.kwargs["scopes"] == [
            "https://www.googleapis.com/auth/cloud-platform"
        ]

    def test_resolve_instance_without_any_project_raises(self):
        with mock.patch.object(cloudsql_admin, "get_config_param", return_value=None):
            with pytest.raises(CloudSQLAdminError, match="SQL_INSTANCE_PROJECT"):
                cloudsql_admin.resolve_instance("inst", None)


class TestImportResume:
    def test_import_waits_while_the_instance_is_busy(self, session):
        session.post.side_effect = [
            _response(status=409, text="another operation is in progress"),
            _response(payload={"name": "op1", "status": "PENDING"}),
        ]
        session.get.return_value = _response(payload={"name": "op1", "status": "DONE"})
        assert cloudsql_admin.import_csv("inst", "gs://b/f.csv", "staging", "t")["status"] == "DONE"
        assert session.post.call_count == 2

    def test_find_import_operation_matches_the_uri(self, session):
        session.get.side_effect = [
            _response(payload={"items": [
                {"name": "e1", "operationType": "EXPORT", "status": "RUNNING"},
                {"name": "i0", "operationType": "IMPORT", "status": "DONE",
                 "importContext": {"uri": "gs://b/f.csv"}, "error": {"errors": [{"code": "x"}]}},
            ], "nextPageToken": "p2"}),
            _response(payload={"items": [
                {"name": "i1", "operationType": "IMPORT", "status": "RUNNING",
                 "importContext": {"uri": "gs://b/other.csv"}},
                {"name": "i2", "operationType": "IMPORT", "status": "RUNNING",
                 "importContext": {"uri": "gs://b/f.csv"}},
            ]}),
        ]
        assert cloudsql_admin.find_import_operation("inst", "gs://b/f.csv")["name"] == "i2"
        assert session.get.call_args.kwargs["params"] == {
            "instance": "inst", "maxResults": 100, "pageToken": "p2",
        }

    def test_find_import_operation_finds_nothing(self, session):
        session.get.return_value = _response(payload={})
        assert cloudsql_admin.find_import_operation("inst", "gs://b/f.csv") is None


class TestTransferOperations:
    """What the transfer to production needs: imports into named columns, exports it
    can record and resume, and finding either again by URI and database."""

    def test_import_csv_names_the_columns(self, session):
        session.post.return_value = _response(payload={"name": "op1", "status": "DONE"})
        cloudsql_admin.import_csv("inst", "gs://b/f.csv", "prod", "t", columns=["id", "v"])
        assert session.post.call_args.kwargs["json"]["importContext"]["csvImportOptions"] == {
            "table": "t", "columns": ["id", "v"],
        }

    def test_export_csv_reports_its_operation_and_waits_out_a_busy_instance(self, session):
        session.post.side_effect = [
            _response(status=409, text="busy"),
            _response(payload={"name": "e1", "status": "PENDING"}),
        ]
        session.get.return_value = _response(payload={"name": "e1", "status": "DONE"})
        started = []
        cloudsql_admin.export_csv(
            "inst", "gs://b/seg.csv", "staging", "SELECT 1", on_started=started.append, busy_timeout=60
        )
        assert started == ["e1"] and session.post.call_count == 2

    def test_find_operation_matches_type_uri_and_database(self, session):
        session.get.return_value = _response(payload={"items": [
            {"name": "i-staging", "operationType": "IMPORT", "status": "DONE",
             "importContext": {"uri": "gs://b/f.csv", "database": "staging"}},
            {"name": "e1", "operationType": "EXPORT", "status": "RUNNING",
             "exportContext": {"uri": "gs://b/seg.csv", "databases": ["staging"]}},
            {"name": "i-prod", "operationType": "IMPORT", "status": "RUNNING",
             "importContext": {"uri": "gs://b/f.csv", "database": "prod"}},
        ]})
        assert cloudsql_admin.find_import_operation("inst", "gs://b/f.csv", database="prod")["name"] == "i-prod"
        assert cloudsql_admin.find_import_operation("inst", "gs://b/f.csv")["name"] == "i-staging"
        assert cloudsql_admin.find_operation("inst", "EXPORT", "gs://b/seg.csv", database="staging")["name"] == "e1"
        assert cloudsql_admin.find_operation("inst", "EXPORT", "gs://b/seg.csv", database="prod") is None
