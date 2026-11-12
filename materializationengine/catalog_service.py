"""Register exported Delta lakes with the CAVE catalog service, and remove them on expiry.

The catalog service (CAVEConnectome/catalog) is where datasets beside the frozen
databases are found: a lake registered there can be listed, previewed and read (with a
vended credential) by anyone with view permission on the datastack. Each lake -- one
sorted copy of one table of one version -- is one catalog table asset named
`<table>.<spec_name>` (the catalog's layout-suffix form), with mat_version set and
`source` "materialization". properties carry what lake_query reads from the internal
catalog today (spec, partition_by, row count, bytes, files), so the query path can move
onto the service next.

For now the internal catalog (deltalake_catalog) stays the query path's source of truth
and the record of `writing` / `failed` / `deleting` lakes, which the service has no
status for: a lake is registered only once it is `complete`, and deregistered before its
files are deleted, so the service never lists a lake that is not there.

Off unless CATALOG_SERVICE_URL is set. Calls go out with the service's own token
(AUTH_TOKEN, else the token file caveclient finds by default, which is how Ray pods,
mounting cave-secret.json without DAF_CREDENTIALS, get one); it needs `edit` on each
datastack's dataset.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

SOURCE = "materialization"


def catalog_url() -> str:
    from materializationengine.utils import get_config_param

    return (get_config_param("CATALOG_SERVICE_URL", "") or "").rstrip("/")


def enabled() -> bool:
    return bool(catalog_url())


def _token() -> Optional[str]:
    from materializationengine.utils import get_config_param

    return get_config_param("AUTH_TOKEN", None) or None


def _client(datastack: str):
    from caveclient.catalogservice import CatalogClient

    return CatalogClient(server_address=catalog_url(), datastack_name=datastack, token=_token())


def asset_name(table_name: str, spec_name: str) -> str:
    return f"{table_name}.{spec_name}"


def _properties(datastack: str, version: int, row: Dict) -> Dict:
    return {
        "source": SOURCE,  # with source_table and mat_version, the catalog checks the table exists
        "source_table": row["table_name"],
        "mat_version": int(version),
        "spec_name": row["spec_name"],
        "partition_by": row.get("partition_by"),
        "spec": row.get("spec"),
        "row_count": row.get("row_count"),
        "bytes": row.get("bytes"),
        "files": row.get("files"),
        "columns": row.get("columns"),
        "completed_at": row.get("completed_at"),
        "exporter_version": row.get("exporter_version"),
        "job_id": row.get("job_id"),
        "index_uri": f"{row['uri'].rstrip('/')}/_cave_index/v1.arrows",
    }


def register_lake(datastack: str, version: int, row: Dict, client=None) -> str:
    """Register one complete lake, given its internal catalog row; returns the asset id.

    An asset already registered at the lake's uri by an earlier export of the same lake
    (since deleted, as the export refuses to overwrite one) is replaced. One
    registered by anyone else is left alone, and the registration fails on the catalog's
    duplicate-uri check.
    """
    client = client or _client(datastack)
    uri = row["uri"]
    existing = client.check_uri(uri)
    if existing.get("existing_id"):
        old = client.get_asset(existing["existing_id"])
        if (old.get("properties") or {}).get("source") == SOURCE:
            client.delete_asset(old["id"])
    asset = client.register_table(
        name=asset_name(row["table_name"], row["spec_name"]),
        uri=uri,
        format="delta",
        is_managed=True,
        mat_version=int(version),
        source=SOURCE,
        properties=_properties(datastack, version, row),
    )
    return asset["id"]


def register_lakes(datastack: str, version: int, rows: List[Dict]) -> Dict:
    """Register each complete lake, logging rather than raising on failure: an export or
    a backfill should not fail because the catalog service is down."""
    registered, failed = [], []
    if not rows:
        return {"registered": registered, "failed": failed}
    try:
        client = _client(datastack)
    except Exception:
        logger.exception("catalog service: no client for %s", datastack)
        return {"registered": registered, "failed": [r["uri"] for r in rows]}
    for row in rows:
        try:
            register_lake(datastack, version, row, client=client)
            registered.append(row["uri"])
        except Exception:
            logger.exception("catalog service: could not register %s", row["uri"])
            failed.append(row["uri"])
    return {"registered": registered, "failed": failed}


def register_version(aligned_volume: str, datastack: str, version: int,
                     table_name: Optional[str] = None) -> Dict:
    """Register every complete lake the internal catalog has for a version (or one of its
    tables): what an export does once its lakes check out, and the backfill for lakes
    exported before the catalog service existed."""
    from materializationengine import deltalake_catalog as cat

    rows = cat.list_exports(aligned_volume, datastack, version, table_name=table_name)
    return register_lakes(datastack, version, rows)


def deregister_version(datastack: str, version: int, dry_run: bool = False) -> List[str]:
    """Delete the catalog assets of a version's lakes; returns their uris.

    Only assets this exporter registered (source "materialization") whose uri is a lake
    under <datastack>/v<version>/ are touched. Raises if the service can't be listed or an
    asset can't be deleted, so the caller can leave the files for the next run.
    """
    client = _client(datastack)
    prefix = f"/{datastack}/v{int(version)}/"
    assets = [a for a in client.list_tables(mat_version=int(version), source=SOURCE)
              if prefix in a["uri"] and (a.get("properties") or {}).get("source") == SOURCE]
    if not dry_run:
        for a in assets:
            client.delete_asset(a["id"])
    return [a["uri"] for a in assets]
