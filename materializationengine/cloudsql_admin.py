"""Cloud SQL Admin API calls (CSV import and export) made directly over REST.

These used to shell out to `gcloud sql import csv` / `gcloud sql export csv`.
Requests made with a service account key carry an `x-allowed-locations` (trust
boundary) header, added both by the google-auth bundled in gcloud 587 and by
google-auth's own service account credentials, and the Cloud SQL Admin API front end
rejects it with a bare HTML 400. So these requests send only the bearer token,
instead of going through gcloud or google-auth's AuthorizedSession.
"""

import time
from typing import Optional, Tuple

import google.auth
import google.auth.transport.requests
import requests

from materializationengine.utils import get_config_param

SQLADMIN_ROOT = "https://sqladmin.googleapis.com/v1"
SCOPES = ["https://www.googleapis.com/auth/sqlservice.admin"]


class CloudSQLAdminError(RuntimeError):
    """A Cloud SQL Admin request or operation failed."""


class _BearerTokenSession:
    """requests.Session that authenticates with only an Authorization header.

    google-auth's AuthorizedSession lets the credentials add their own headers,
    including the x-allowed-locations header the Cloud SQL Admin API rejects.
    """

    def __init__(self, credentials):
        self._credentials = credentials
        self._http = requests.Session()

    def _headers(self) -> dict:
        if not self._credentials.valid:
            self._credentials.refresh(google.auth.transport.requests.Request())
        return {"Authorization": f"Bearer {self._credentials.token}"}

    def get(self, url, **kwargs):
        return self._http.get(url, headers=self._headers(), **kwargs)

    def post(self, url, **kwargs):
        return self._http.post(url, headers=self._headers(), **kwargs)


def _session_and_default_project() -> Tuple[_BearerTokenSession, Optional[str]]:
    # Uses GOOGLE_APPLICATION_CREDENTIALS (the service account key) when it is set.
    credentials, default_project = google.auth.default(scopes=SCOPES)
    return _BearerTokenSession(credentials), default_project


def resolve_instance(instance_name: str, default_project: Optional[str]) -> Tuple[str, str]:
    """(project, instance) for SQL_INSTANCE_NAME.

    Accepts a bare instance name or a "project:region:instance" connection name. The
    project comes from SQL_INSTANCE_PROJECT, then the connection name, then the
    credentials' project.
    """
    project = get_config_param("SQL_INSTANCE_PROJECT")
    parts = instance_name.split(":")
    if len(parts) == 3:
        project = project or parts[0]
        instance_name = parts[2]
    project = project or default_project
    if not project:
        raise CloudSQLAdminError(
            f"Could not determine the Google project for Cloud SQL instance '{instance_name}'; "
            "set SQL_INSTANCE_PROJECT"
        )
    return project, instance_name


def _post_operation(session, url: str, body: dict) -> dict:
    response = session.post(url, json=body, timeout=60)
    if not response.ok:
        raise CloudSQLAdminError(
            f"POST {url} failed with HTTP {response.status_code}: {response.text[:2000]}"
        )
    return response.json()


def wait_for_operation(
    session, project: str, operation: dict, timeout: float, poll_interval: float = 5
) -> dict:
    """Poll a Cloud SQL operation until it is DONE; raise if it failed or timed out."""
    name = operation["name"]
    url = f"{SQLADMIN_ROOT}/projects/{project}/operations/{name}"
    deadline = time.monotonic() + timeout
    while operation.get("status") != "DONE":
        if time.monotonic() > deadline:
            raise TimeoutError(f"Cloud SQL operation {name} did not finish within {timeout}s")
        time.sleep(poll_interval)
        response = session.get(url, timeout=60)
        if not response.ok:
            raise CloudSQLAdminError(
                f"GET {url} failed with HTTP {response.status_code}: {response.text[:2000]}"
            )
        operation = response.json()

    errors = (operation.get("error") or {}).get("errors") or []
    if errors:
        messages = "; ".join(f"{e.get('code')}: {e.get('message')}" for e in errors)
        raise CloudSQLAdminError(f"Cloud SQL operation {name} failed: {messages}")
    return operation


def import_csv(
    instance_name: str,
    uri: str,
    database: str,
    table: str,
    user: str = "postgres",
    timeout: float = 1200,
) -> dict:
    """Import a CSV from GCS into a table and wait for it to finish
    (equivalent of `gcloud sql import csv`)."""
    session, default_project = _session_and_default_project()
    project, instance = resolve_instance(instance_name, default_project)
    body = {
        "importContext": {
            "kind": "sql#importContext",
            "fileType": "CSV",
            "uri": uri,
            "database": database,
            "importUser": user,
            "csvImportOptions": {"table": table},
        }
    }
    operation = _post_operation(
        session, f"{SQLADMIN_ROOT}/projects/{project}/instances/{instance}/import", body
    )
    return wait_for_operation(session, project, operation, timeout=timeout)


def export_csv(
    instance_name: str,
    uri: str,
    database: str,
    select_query: str,
    wait: bool = True,
    timeout: float = 1200,
) -> dict:
    """Export a query's results as CSV to GCS (equivalent of `gcloud sql export csv`).

    A uri ending in .gz is gzip-compressed by Cloud SQL. With wait=False the operation
    is only started, like `--async`.
    """
    session, default_project = _session_and_default_project()
    project, instance = resolve_instance(instance_name, default_project)
    body = {
        "exportContext": {
            "kind": "sql#exportContext",
            "fileType": "CSV",
            "uri": uri,
            "databases": [database],
            "csvExportOptions": {"selectQuery": select_query},
        }
    }
    operation = _post_operation(
        session, f"{SQLADMIN_ROOT}/projects/{project}/instances/{instance}/export", body
    )
    if not wait:
        return operation
    return wait_for_operation(session, project, operation, timeout=timeout)
