"""Delete a version's Delta Lake exports once the version itself has expired.

Which lakes exist comes from the catalog (deltalake_catalog), never from listing the
bucket, so only lakes this exporter wrote and recorded are ever deleted.

When: a version's lakes go once its AnalysisVersion is past expires_on *and* no longer
valid. remove_expired_databases sets valid False when it drops the version's database,
so lakes are removed along with the database they were exported from, and never while
the version is still served (a version kept past expiry by delete_threshold stays valid,
and so keeps its lakes). A version being built is not yet valid but has not expired
either, so it is left alone.

How: the version's rows are marked `deleting` first, so the catalog stops listing them
as complete before any file disappears; then each lake's files are removed and its row
marked `deleted`. A lake whose removal fails stays `deleting`, and since `deleting`
rows still count as present, the next run tries it again.

Why not bucket lifecycle rules: deletes are free either way, expiry can move after a
lake is written (virtual datastacks extend it), and prefix rules are capped per bucket
and lag up to a day. Note that a bucket's soft delete policy still bills deleted objects
for its retention window (the default is 7 days); a bucket used only for exports can
turn soft delete off.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional

import sqlalchemy as sa

from materializationengine import deltalake_catalog as cat

logger = logging.getLogger(__name__)


def delete_expired_enabled() -> bool:
    """DELTALAKE_DELETE_EXPIRED (default on): a bool from the app config, or the raw
    environment string when the task runs without an app context."""
    from materializationengine.utils import get_config_param

    value = get_config_param("DELTALAKE_DELETE_EXPIRED", True)
    if isinstance(value, str):
        return value.strip().lower() not in ("false", "0", "no", "off", "")
    return bool(value)


def expired_lake_versions(aligned_volume: str, datastack: str, now: Optional[datetime] = None) -> List[int]:
    """Versions with lakes still on storage whose AnalysisVersion has expired and is no
    longer valid."""
    from dynamicannotationdb.models import AnalysisVersion

    candidates = cat.versions_with_lakes(aligned_volume, datastack)
    if not candidates:
        return []
    now = now or datetime.utcnow()  # expires_on is a naive UTC timestamp
    av = AnalysisVersion.__table__
    q = sa.select([av.c.version]).where(sa.and_(
        av.c.datastack == datastack, av.c.version.in_(candidates),
        av.c.valid.isnot(True), av.c.expires_on.isnot(None), av.c.expires_on <= now))
    with cat._engine(aligned_volume).connect() as conn:
        return sorted(int(r[0]) for r in conn.execute(q))


def _check_lake_uri(uri: str, datastack: str, version: int) -> None:
    # Every exported lake lives under <bucket>/<datastack>/v<version>/<table>/<spec>;
    # refuse anything else rather than delete a prefix the catalog got wrong.
    if f"/{datastack}/v{int(version)}/" not in uri or uri.rstrip("/").endswith(f"/v{int(version)}"):
        raise ValueError(f"refusing to delete {uri!r}: not a lake under {datastack}/v{version}/")


def remove_lake_files(uri: str) -> int:
    """Delete every object of a lake; returns how many. A lake already gone counts as 0."""
    import fsspec

    fs, path = fsspec.core.url_to_fs(uri)
    try:
        files = fs.find(path)
    except FileNotFoundError:
        return 0
    if files:
        fs.rm(files)
    try:
        fs.rm(path, recursive=True)  # empty directories, on filesystems that have them
    except FileNotFoundError:
        pass
    if hasattr(fs, "invalidate_cache"):
        fs.invalidate_cache(path)
    return len(files)


def remove_version_lakes(aligned_volume: str, datastack: str, version: int, dry_run: bool = False) -> Dict:
    """Mark a version's lakes deleting, delete their files, and mark them deleted."""
    if dry_run:
        rows = cat.list_exports(aligned_volume, datastack, version,
                                statuses=[s for s in cat.STATUSES if s != "deleted"])
        return {"version": version, "dry_run": True, "lakes": [r["uri"] for r in rows]}
    removed, failed, objects = [], [], 0
    for row in cat.mark_deleting(aligned_volume, datastack, version):
        try:
            _check_lake_uri(row["uri"], datastack, version)
            objects += remove_lake_files(row["uri"])
            cat.mark_deleted(aligned_volume, datastack, version, row["table_name"], row["spec_name"])
            removed.append(row["uri"])
        except Exception as e:
            logger.exception("could not delete lake %s", row["uri"])
            cat.safely(cat._set, aligned_volume, datastack, version, row["table_name"], row["spec_name"],
                       error=f"delete failed: {type(e).__name__}: {e}"[:4000])
            failed.append(row["uri"])
    logger.info("%s v%d: deleted %d lake(s) (%d objects), %d failed", datastack, version, len(removed),
                objects, len(failed))
    return {"version": version, "deleted": removed, "failed": failed, "objects": objects}


def remove_expired_lakes(aligned_volume: str, datastack: str, dry_run: bool = False) -> List[Dict]:
    """Delete the lakes of every expired, no-longer-valid version of a datastack."""
    return [remove_version_lakes(aligned_volume, datastack, v, dry_run=dry_run)
            for v in expired_lake_versions(aligned_volume, datastack)]
