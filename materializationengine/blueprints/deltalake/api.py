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
        datastacks = get_datastacks() or []
        datastacks.sort()
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
        bloom_filter_fpp=get_config_param("DELTALAKE_BLOOM_FILTER_FPP", 0.001),
        output_bucket=get_config_param("DELTALAKE_OUTPUT_BUCKET", ""),
    )


@deltalake_bp.route("/running-exports")
@reset_auth
@auth_requires_admin
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
            "bloom_filter_fpp": float(
                get_config_param("DELTALAKE_BLOOM_FILTER_FPP", 0.001)
            ),
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

    from materializationengine.database import db_manager
    from materializationengine.models import MaterializedMetadata
    from materializationengine.workflows.deltalake_export import (
        _DEFAULT_DROP_COLUMNS,
        TableSource,
        _build_frozen_db_connection_string,
        _get_redis_client,
        _resolve_select_columns,
        _validate_identifier,
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

    try:
        _validate_identifier(table_name)
    except ValueError:
        return jsonify({"error": f"Invalid table name: {table_name!r}"}), 400

    # Check Redis cache first.
    cache_key = f"deltalake_specs:{datastack}:v{version}:{table_name}"
    redis_client = _get_redis_client()
    cached = redis_client.get(cache_key)
    if cached:
        cached_data = json.loads(cached)
        # Cached data contains raw specs (n_partitions still "auto").
        # Resolve partition counts per-request using the caller's target.
        for spec in cached_data["specs"]:
            if spec.get("n_partitions") == "auto" or spec.get("n_partitions") is None:
                effective_target = (
                    spec.get("target_file_size_mb") or target_partition_size_mb
                )
                spec["n_partitions"] = resolve_n_partitions(
                    "auto",
                    cached_data["row_count"],
                    target_file_size_mb=effective_target,
                    bytes_per_row=cached_data["bytes_per_row"],
                )
        return jsonify(cached_data)

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

    # Track which specs had "auto" before resolution (for caching).
    was_auto = [spec.n_partitions == "auto" for spec in resolved_specs]

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

    # Build available columns list (base columns + computed columns from specs).
    available_columns = _resolve_select_columns(
        connection_string, source, _DEFAULT_DROP_COLUMNS
    )
    for spec in resolved_specs:
        if spec.source_geometry_column:
            col = spec.source_geometry_column
            for suffix in ["_x", "_y", "_z", "_morton"]:
                computed = f"{col}{suffix}"
                if computed not in available_columns:
                    available_columns.append(computed)

    # Collect geometry columns (position columns that get morton-encoded).
    geometry_columns = sorted(
        {s.source_geometry_column for s in resolved_specs if s.source_geometry_column}
    )

    # Cache raw specs (before n_partitions resolution) so the cache stays
    # valid regardless of the caller's target_partition_size_mb.
    raw_specs = [asdict(s) for s in resolved_specs]
    # Reset resolved n_partitions back to "auto" for specs that were auto.
    for raw, auto in zip(raw_specs, was_auto):
        if auto:
            raw["n_partitions"] = "auto"

    cache_result = {
        "row_count": row_count,
        "bytes_per_row": bytes_per_row,
        "available_columns": available_columns,
        "geometry_columns": geometry_columns,
        "specs": raw_specs,
    }
    redis_client.set(cache_key, json.dumps(cache_result), ex=600)

    # Return the result with resolved partition counts.
    result = {
        "row_count": row_count,
        "bytes_per_row": bytes_per_row,
        "available_columns": available_columns,
        "geometry_columns": geometry_columns,
        "specs": [asdict(s) for s in resolved_specs],
    }

    return jsonify(result)


@deltalake_bp.route("/api/check-exists", methods=["POST"])
@reset_auth
@auth_required
def check_exists():
    """Check whether Delta Lake exports already exist for a table/version.

    Expects JSON body: { datastack, version, table_name, spec_names? }
    Returns: { exists: bool, existing_specs: [{name, uri, row_count}] }

    If ``spec_names`` is provided, checks those exact folder names.
    Otherwise falls back to cached specs or checks "flat".
    """
    if not request.is_json or not request.json:
        return jsonify({"error": "Request body must be JSON"}), 400

    data = request.json
    datastack = data.get("datastack")
    version = data.get("version")
    table_name = data.get("table_name")

    if not all([datastack, version, table_name]):
        return jsonify(
            {"error": "datastack, version, and table_name are required"}
        ), 400

    output_bucket = get_config_param("DELTALAKE_OUTPUT_BUCKET", "")
    if not output_bucket:
        return jsonify({"error": "DELTALAKE_OUTPUT_BUCKET not configured"}), 500

    output_uri_base = f"{output_bucket}/{datastack}/v{version}/{table_name}"

    # If the caller provides explicit spec names, use those.
    # Otherwise fall back to cached specs or just check "flat".
    spec_names = data.get("spec_names")
    if spec_names:
        partition_names = list(set(spec_names))
    else:
        from materializationengine.workflows.deltalake_export import _get_redis_client

        redis_client = _get_redis_client()
        cache_key = f"deltalake_specs:{datastack}:v{version}:{table_name}"
        cached = redis_client.get(cache_key)

        partition_names = ["flat"]
        if cached:
            import json as _json

            cached_data = _json.loads(cached)
            partition_names = list(
                {spec.get("name") or "flat" for spec in cached_data.get("specs", [])}
            )

    existing_specs = []
    for lake_name in partition_names:
        uri = f"{output_uri_base}/{lake_name}"
        try:
            from deltalake import DeltaTable

            dt = DeltaTable(uri)
            row_count = dt.count()
            existing_specs.append(
                {"name": lake_name, "uri": uri, "row_count": row_count}
            )
        except Exception:
            # Table doesn't exist at this URI — not an error.
            pass

    return jsonify(
        {"exists": len(existing_specs) > 0, "existing_specs": existing_specs}
    )
