import json
import os

from flask import (
    Blueprint,
    current_app,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from middle_auth_client import auth_required, auth_requires_admin

from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.info_client import get_datastack_info, get_datastacks
from materializationengine.utils import get_config_param

deltalake_bp = Blueprint(
    "deltalake",
    __name__,
    url_prefix="/materialize/deltalake",
)


def _is_auth_disabled():
    return current_app.config.get(
        "AUTH_DISABLED",
        os.environ.get("AUTH_DISABLED", "").lower() in ("true", "1", "yes"),
    )


def _has_datastack_permission(auth_user_info, permission_level, datastack_name):
    if not auth_user_info or not datastack_name or not permission_level:
        return False
    permissions = auth_user_info.get("permissions", [])
    required_permission = f"{datastack_name.lower()}_{permission_level.lower()}"
    return required_permission in permissions


# ---------------------------------------------------------------------------
# Wizard page routes
# ---------------------------------------------------------------------------


@deltalake_bp.route("/")
@reset_auth
@auth_requires_admin
def index():
    """Redirect to step 1 of the wizard."""
    return redirect(url_for("deltalake.wizard_step", step_number=1))


@deltalake_bp.route("/step<int:step_number>")
@reset_auth
@auth_requires_admin
def wizard_step(step_number):
    total_steps = 3

    if step_number < 1 or step_number > total_steps:
        return redirect(url_for("deltalake.wizard_step", step_number=1))

    try:
        all_datastacks = get_datastacks() or []
        if _is_auth_disabled() or not g.get("auth_user"):
            datastacks = sorted(all_datastacks)
        else:
            datastacks = sorted(
                ds
                for ds in all_datastacks
                if _has_datastack_permission(g.auth_user, "admin", ds)
            )
    except Exception as e:
        current_app.logger.error(
            f"Failed to get datastacks for deltalake wizard step {step_number}: {e}",
            exc_info=True,
        )
        datastacks = []

    step_template_path = f"deltalake/step{step_number}.html"

    return render_template(
        "deltalake_wizard.html",
        current_step=step_number,
        total_steps=total_steps,
        step_template=step_template_path,
        datastacks=datastacks,
        current_user=g.get("auth_user", {}),
        target_partition_size_mb=get_config_param(
            "DELTALAKE_TARGET_PARTITION_SIZE_MB", 256
        ),
        output_bucket=get_config_param("DELTALAKE_OUTPUT_BUCKET", ""),
    )


@deltalake_bp.route("/running-exports")
@reset_auth
@auth_required
def running_exports_page():
    """Render the running exports monitoring page."""
    return render_template(
        "deltalake/running_exports.html",
        current_user=g.get("auth_user", {}),
    )


# ---------------------------------------------------------------------------
# API routes
# ---------------------------------------------------------------------------


@deltalake_bp.route("/api/defaults", methods=["GET"])
@reset_auth
@auth_required
def get_defaults():
    """Return environment defaults for the wizard UI."""
    return jsonify(
        {
            "target_partition_size_mb": int(
                get_config_param("DELTALAKE_TARGET_PARTITION_SIZE_MB", 256)
            ),
            "output_bucket": get_config_param("DELTALAKE_OUTPUT_BUCKET", ""),
        }
    )


@deltalake_bp.route("/api/discover-specs", methods=["POST"])
@reset_auth
@auth_requires_admin
def discover_specs():
    """Run spec discovery for a table without enqueuing an export.

    Expects JSON body: { datastack, version, table_name, target_partition_size_mb }
    Returns: { row_count, bytes_per_row, specs: [...] }
    """
    from dynamicannotationdb.key_utils import build_segmentation_table_name
    from sqlalchemy import create_engine

    from materializationengine.database import db_manager
    from materializationengine.models import MaterializedMetadata
    from materializationengine.workflows.deltalake_export import (
        DeltaLakeOutputSpec,
        TableSource,
        _build_frozen_db_connection_string,
        _get_redis_client,
        discover_default_output_specs,
        estimate_bytes_per_row,
        resolve_n_partitions,
    )

    if not request.is_json or not request.json:
        return jsonify({"error": "Request body must be JSON"}), 400

    data = request.json
    datastack = data.get("datastack")
    version = data.get("version")
    table_name = data.get("table_name")
    target_partition_size_mb = data.get("target_partition_size_mb", 256)

    if not all([datastack, version, table_name]):
        return jsonify(
            {"error": "datastack, version, and table_name are required"}
        ), 400

    # Check Redis cache first.
    cache_key = f"deltalake_specs:{datastack}:v{version}:{table_name}"
    redis_client = _get_redis_client()
    cached = redis_client.get(cache_key)
    if cached:
        return jsonify(json.loads(cached))

    try:
        datastack_info = get_datastack_info(datastack)
    except Exception as e:
        return jsonify({"error": f"Datastack not found: {e}"}), 404

    sql_uri_config = get_config_param("SQLALCHEMY_DATABASE_URI")
    connection_string = _build_frozen_db_connection_string(
        sql_uri_config, datastack, version
    )

    analysis_database = f"{datastack}__mat{version}"
    pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]

    try:
        engine = db_manager.get_engine(analysis_database)
    except Exception as e:
        return jsonify(
            {"error": f"Cannot connect to frozen DB for version {version}: {e}"}
        ), 404

    # Look up row count.
    with db_manager.session_scope(analysis_database) as session:
        metadata_row = (
            session.query(MaterializedMetadata)
            .filter(MaterializedMetadata.table_name == table_name)
            .first()
        )
        if metadata_row is None:
            return jsonify(
                {"error": f"Table {table_name!r} not found in version {version}"}
            ), 404
        row_count = metadata_row.row_count

    # Detect segmentation table.
    seg_table_name = build_segmentation_table_name(table_name, pcg_table_name)
    has_seg_table = engine.dialect.has_table(engine, seg_table_name)
    segmentation_table_name = seg_table_name if has_seg_table else None

    source = TableSource(
        annotation_table=table_name,
        segmentation_table=segmentation_table_name,
    )

    # Discover specs.
    resolved_specs = discover_default_output_specs(source, engine)
    bytes_per_row = estimate_bytes_per_row(connection_string, source)

    # Resolve partition counts.
    for spec in resolved_specs:
        if spec.n_partitions == "auto":
            effective_target = spec.target_file_size_mb or target_partition_size_mb
            spec.n_partitions = resolve_n_partitions(
                "auto",
                row_count,
                target_file_size_mb=effective_target,
                bytes_per_row=bytes_per_row,
            )

    from dataclasses import asdict

    result = {
        "row_count": row_count,
        "bytes_per_row": bytes_per_row,
        "specs": [asdict(s) for s in resolved_specs],
    }

    # Cache in Redis with 10-minute TTL.
    redis_client.set(cache_key, json.dumps(result), ex=600)

    return jsonify(result)


@deltalake_bp.route("/api/recalculate", methods=["POST"])
@reset_auth
@auth_required
def recalculate():
    """Recompute n_partitions for specs. Pure computation, no DB access.

    Expects JSON body: { row_count, bytes_per_row, specs: [...] }
    Returns: { specs: [...with recomputed n_partitions...] }
    """
    from materializationengine.workflows.deltalake_export import resolve_n_partitions

    if not request.is_json or not request.json:
        return jsonify({"error": "Request body must be JSON"}), 400

    data = request.json
    row_count = data.get("row_count")
    bytes_per_row = data.get("bytes_per_row")
    specs = data.get("specs")

    if not all([row_count, bytes_per_row, specs]):
        return jsonify(
            {"error": "row_count, bytes_per_row, and specs are required"}
        ), 400

    global_target = int(get_config_param("DELTALAKE_TARGET_PARTITION_SIZE_MB", 256))

    result_specs = []
    for spec in specs:
        if spec.get("n_partitions") == "auto" or spec.get("n_partitions") is None:
            effective_target = spec.get("target_file_size_mb") or global_target
            spec["n_partitions"] = resolve_n_partitions(
                "auto",
                row_count,
                target_file_size_mb=effective_target,
                bytes_per_row=bytes_per_row,
            )
        result_specs.append(spec)

    return jsonify({"specs": result_specs})
