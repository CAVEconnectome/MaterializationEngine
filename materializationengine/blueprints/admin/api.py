"""Admin page, organized by datastack, showing each user only what they may do.

Superadmins see every configured datastack and the deployment-wide sections (queues,
workers, upload cleanup). Dataset admins see the datastacks whose auth dataset they
administer, with the actions those endpoints allow a dataset admin. The page only decides
what to show: every action goes through an existing endpoint that checks its own
permission (/materialize/api/v2/..., /materialize/upload/api/admin/...).
"""

import json
import re
import time

from cachetools import TTLCache, cached
from flask import Blueprint, abort, g, jsonify, render_template, request
from middle_auth_client import auth_required, auth_requires_admin
from middle_auth_client.decorators import dataset_from_table_id_from_request

from materializationengine import __version__
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.redis_client import get_redis_client

admin_bp = Blueprint("mat_admin", __name__, url_prefix="/materialize/admin")

CELERY_QUEUES = ("process", "spatial", "workflow", "orchestration", "deltalake", "celery")
_SAFE_DATASTACK = re.compile(r"^[A-Za-z0-9_]+$")


def is_superadmin() -> bool:
    """Whether the logged-in user is a CAVE superadmin (what auth_requires_admin checks)."""
    user = g.get("auth_user") or {}
    return bool(user.get("admin"))


@cached(cache=TTLCache(maxsize=256, ttl=3600))
def _dataset_for(datastack: str) -> str:
    """The auth dataset a datastack belongs to (minnie65_phase3_v1 -> minnie65), as the
    auth decorators look it up."""
    return dataset_from_table_id_from_request(datastack)


def capabilities() -> dict:
    """What the logged-in user may do here, per configured datastack.

    admin: dataset admin of the datastack's auth dataset (auth_requires_dataset_admin).
    edit: edit permission on it (auth_requires_permission("edit")).
    Superadmins (auth_requires_admin) may do everything.
    """
    from materializationengine.utils import get_config_param

    user = g.get("auth_user") or {}
    superadmin = bool(user.get("admin"))
    datasets_admin = set(user.get("datasets_admin") or [])
    permissions = user.get("permissions_v2") or {}
    datastacks = []
    for name in get_config_param("DATASTACKS") or []:
        try:
            dataset = _dataset_for(name)
        except Exception:
            dataset = None
        admin = superadmin or (dataset in datasets_admin)
        edit = superadmin or admin or "edit" in (permissions.get(dataset) or [])
        if superadmin or admin or edit:
            datastacks.append({"name": name, "dataset": dataset, "admin": admin, "edit": edit})
    return {"superadmin": superadmin, "datastacks": datastacks}


def can_see_admin_page() -> bool:
    """Superadmins and admins of at least one configured datastack."""
    if not g.get("auth_user"):
        return False
    if is_superadmin():
        return True
    return any(d["admin"] for d in capabilities()["datastacks"])


@admin_bp.route("/")
@reset_auth
@auth_required
def admin_page():
    if not can_see_admin_page():
        abort(403, "The admin page is for superadmins and datastack admins.")
    return render_template("admin/index.html", version=__version__, current_user=g.get("auth_user", {}))


@admin_bp.route("/api/capabilities")
@reset_auth
@auth_required
def user_capabilities():
    return jsonify(capabilities())


@admin_bp.route("/api/databases")
@reset_auth
@auth_requires_admin
def databases():
    from materializationengine.workflows.table_maintenance import list_databases

    with_sizes = request.args.get("sizes", "true").lower() != "false"
    dbs = list_databases(with_sizes=with_sizes)
    datastack = request.args.get("datastack")
    if datastack:
        from materializationengine.info_client import get_datastack_info

        aligned_volume = get_datastack_info(datastack)["aligned_volume"]["name"]
        dbs = [d for d in dbs if d["name"] == aligned_volume or d.get("datastack") == datastack]
    return jsonify({"databases": dbs})


def _require_datastack_admin(datastack: str):
    """403 unless the user is a superadmin or admin of this configured datastack."""
    entry = next((d for d in capabilities()["datastacks"] if d["name"] == datastack), None)
    if not (entry and entry["admin"]):
        abort(403, f"Requires admin of datastack {datastack}.")


@admin_bp.route("/api/datastack/<string:datastack>/versions")
@reset_auth
@auth_required
def frozen_versions(datastack: str):
    """Frozen versions of the datastack whose databases exist (for choosing one to export)."""
    from materializationengine.workflows.table_maintenance import list_frozen_versions

    _require_datastack_admin(datastack)
    return jsonify({"versions": list_frozen_versions(datastack)})


@admin_bp.route("/api/datastack/<string:datastack>/version/<int:version>/tables")
@reset_auth
@auth_required
def version_tables(datastack: str, version: int):
    """Tables and views of one frozen version's database."""
    from sqlalchemy.exc import OperationalError

    from materializationengine.workflows.table_maintenance import RepackRefused, list_relations

    _require_datastack_admin(datastack)
    database = f"{datastack}__mat{version}"
    try:
        return jsonify({"database": database, "tables": list_relations(database)})
    except (RepackRefused, OperationalError):
        abort(404, f"Version {version} of {datastack} has no database.")


@admin_bp.route("/api/datastack/<string:datastack>/version/<int:version>/annotation_tables")
@reset_auth
@auth_required
def version_annotation_tables(datastack: str, version: int):
    """The annotation tables recorded for a frozen version (what a virtual version may include)."""
    from dynamicannotationdb.models import AnalysisTable, AnalysisVersion

    from materializationengine.database import db_manager
    from materializationengine.info_client import get_relevant_datastack_info

    _require_datastack_admin(datastack)
    aligned_volume, _ = get_relevant_datastack_info(datastack)
    with db_manager.session_scope(aligned_volume) as session:
        rows = (
            session.query(AnalysisTable.table_name, AnalysisTable.schema)
            .join(AnalysisVersion, AnalysisTable.analysisversion_id == AnalysisVersion.id)
            .filter(AnalysisVersion.datastack == datastack, AnalysisVersion.version == version)
            .filter(AnalysisTable.valid == True)  # noqa: E712
            .order_by(AnalysisTable.table_name)
            .all()
        )
    return jsonify({"tables": [{"name": r[0], "schema": r[1]} for r in rows]})


def virtual_target_status(source: str, name: str) -> dict:
    """Whether datastack `name` can serve virtual versions of `source`'s frozen versions.

    A virtual version is an analysisversion row under `name` that points at a frozen version
    of `source`. Queries to it work only once `name` is in the infoservice on the same
    aligned volume (that is where its analysisversion rows are looked up) and is mapped to
    an auth dataset (which decides who may read it).
    """
    from dynamicannotationdb.models import AnalysisVersion

    from materializationengine.database import db_manager
    from materializationengine.info_client import get_datastack_info, get_relevant_datastack_info

    from flask import current_app

    aligned_volume, _ = get_relevant_datastack_info(source)
    status = {"name": name, "aligned_volume": aligned_volume, "this_server": current_app.config.get("LOCAL_SERVER_URL"),
              "global_server": current_app.config.get("GLOBAL_SERVER_URL")}
    try:
        info = get_datastack_info(name)
        status["infoservice"] = {
            "exists": True,
            "aligned_volume": info["aligned_volume"]["name"],
            "aligned_volume_matches": info["aligned_volume"]["name"] == aligned_volume,
            "segmentation_source": info.get("segmentation_source"),
            "local_server": info.get("local_server"),
        }
    except Exception:
        status["infoservice"] = {"exists": False}
    try:
        status["auth_dataset"] = _dataset_for(name)
    except Exception:
        status["auth_dataset"] = None
    with db_manager.session_scope(aligned_volume) as session:
        rows = (
            session.query(AnalysisVersion.version, AnalysisVersion.parent_version)
            .filter(AnalysisVersion.datastack == name)
            .order_by(AnalysisVersion.version.desc())
            .all()
        )
    status["versions"] = [{"version": r[0], "virtual": r[1] is not None} for r in rows]
    status["ready"] = bool(
        status["infoservice"].get("aligned_volume_matches") and status["auth_dataset"]
    )
    return status


@admin_bp.route("/api/datastack/<string:datastack>/virtual_targets")
@reset_auth
@auth_required
def virtual_targets(datastack: str):
    """Other datastacks on this datastack's aligned volume, which can hold its virtual versions."""
    from caveclient.auth import AuthClient
    from caveclient.infoservice import InfoServiceClient
    from flask import current_app

    from materializationengine.info_client import get_relevant_datastack_info

    _require_datastack_admin(datastack)
    aligned_volume, _ = get_relevant_datastack_info(datastack)
    server = current_app.config["GLOBAL_SERVER_URL"]
    info = InfoServiceClient(
        server_address=server,
        auth_client=AuthClient(server_address=server, token=current_app.config["AUTH_TOKEN"]),
        api_version=current_app.config.get("INFO_API_VERSION", 2),
    )
    names = [n for n in info.get_datastacks_by_aligned_volume(aligned_volume) if n != datastack]
    return jsonify({"aligned_volume": aligned_volume, "targets": [virtual_target_status(datastack, n) for n in sorted(names)]})


@admin_bp.route("/api/datastack/<string:datastack>/virtual_target/<string:name>")
@reset_auth
@auth_required
def virtual_target(datastack: str, name: str):
    """Readiness of one (possibly new) datastack name to hold virtual versions."""
    _require_datastack_admin(datastack)
    if not _SAFE_DATASTACK.match(name):
        abort(400, "A datastack name may use only letters, digits and underscores.")
    return jsonify(virtual_target_status(datastack, name))


@admin_bp.route("/api/repack/jobs")
@reset_auth
@auth_requires_admin
def repack_jobs():
    from materializationengine.workflows.table_maintenance import list_repack_jobs

    return jsonify({"jobs": list_repack_jobs(limit=int(request.args.get("limit", 50)))})


def queue_status() -> dict:
    """Queued and claimed (in-flight) celery messages, by queue and worker pool."""
    redis = get_redis_client(0)
    queues = {name: redis.llen(name) for name in CELERY_QUEUES}
    claimed = {}
    now = time.time()
    for key in redis.scan_iter("*unacked", count=1000, _type="hash"):
        pool = key.decode()
        tasks = {}
        for _, raw in redis.hscan_iter(key):
            try:
                task = json.loads(raw)[0].get("headers", {}).get("task", "?")
            except Exception:
                task = "?"
            tasks[task] = tasks.get(task, 0) + 1
        oldest = redis.zrange(f"{pool}_index", 0, 0, withscores=True)
        claimed[pool] = {
            "count": sum(tasks.values()),
            "tasks": tasks,
            "oldest_claim_seconds": round(now - oldest[0][1]) if oldest else None,
        }
    return {"queues": queues, "claimed": claimed}


@admin_bp.route("/api/queues")
@reset_auth
@auth_requires_admin
def queues():
    return jsonify(queue_status())


@admin_bp.route("/api/datastacks")
@reset_auth
@auth_requires_admin
def datastacks():
    """Datastacks this deployment materializes (the workflows on the Materialization tab take one)."""
    from materializationengine.utils import get_config_param

    return jsonify({"datastacks": list(get_config_param("DATASTACKS") or [])})
