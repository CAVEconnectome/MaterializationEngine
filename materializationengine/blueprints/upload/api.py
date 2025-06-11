import datetime
import functools
import os
from typing import Any, Dict
import json
from dataclasses import asdict

from dynamicannotationdb.models import AnalysisVersion
from dynamicannotationdb.schema import DynamicSchemaClient
from flask import (
    Blueprint,
    abort,
    current_app,
    g,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from flask_restx import Namespace, Resource, inputs, reqparse
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
from middle_auth_client import (
    auth_requires_admin,
    auth_requires_permission,
    auth_required,
)
from redis import StrictRedis

from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.blueprints.upload.checkpoint_manager import (
    RedisCheckpointManager,
    CHUNK_STATUS_PENDING,
    CHUNK_STATUS_PROCESSING,
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_FAILED_RETRYABLE,
    CHUNK_STATUS_FAILED_PERMANENT,
)
from materializationengine.blueprints.upload.schema_helper import get_schema_types
from materializationengine.blueprints.upload.schemas import UploadRequestSchema
from materializationengine.blueprints.upload.storage import (
    StorageConfig,
    StorageService,
)
from materializationengine.blueprints.upload.tasks import (
    cancel_processing_job,
    get_job_status,
    process_and_upload,
)
from materializationengine.database import db_manager, dynamic_annotation_cache
from materializationengine.info_client import get_datastack_info, get_datastacks
from materializationengine.utils import get_config_param
from materializationengine import __version__


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

upload_bp = Blueprint(
    "upload",
    __name__,
    url_prefix="/materialize/upload",
)

spatial_lookup_bp = Namespace(
    "Spatial Lookup",
    authorizations=authorizations,
    description="Spatial Lookup Operations",
    path="/api/spatial-lookup",
)


spatial_svid_parser = reqparse.RequestParser()
spatial_svid_parser.add_argument(
    "chunk_scale_factor",
    default=1,
    type=int,
    help="Chunk scale factor for spatial lookup. Chunk size is 1024 * scale_factor",
)
spatial_svid_parser.add_argument(
    "supervoxel_batch_size",
    default=50,
    type=int,
    help="Number of supervoxels to lookup at a time per cloud volume call",
)
spatial_svid_parser.add_argument("get_root_ids", default=True, type=inputs.boolean)
spatial_svid_parser.add_argument(
    "upload_to_database", default=True, type=inputs.boolean
)
spatial_svid_parser.add_argument(
    "use_staging_database", default=False, type=inputs.boolean
)
spatial_svid_parser.add_argument(
    "resume_from_checkpoint", default=False, type=inputs.boolean
)


spatial_lookup_status = reqparse.RequestParser()
spatial_lookup_status.add_argument(
    "use_staging_database",
    type=inputs.boolean,
    default=False,
    help="Use staging database for spatial lookup",
)


REDIS_CLIENT = StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)


def is_auth_disabled():
    """
    Check if authentication is disabled.

    Returns:
        bool: True if authentication is disabled, False otherwise
    """
    return current_app.config.get(
        "AUTH_DISABLED",
        os.environ.get("AUTH_DISABLED", "").lower() in ("true", "1", "yes"),
    )


def check_user_permissions():
    """
    Check if the authenticated user has upload permissions.

    Returns:
        bool: True if user has required permissions or auth is disabled
    """
    if is_auth_disabled():
        return True

    if not g.get("auth_user"):
        return False

    permissions = g.auth_user.get("permissions", [])
    return "datasets_admin" in permissions or "groups_admin" in permissions


def require_upload_permissions(f):
    """
    Decorator to require upload permissions.
    Redirects to permission warning page if user lacks required permissions.
    """

    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        if is_auth_disabled() or check_user_permissions():
            return f(*args, **kwargs)
        return redirect(url_for("upload.permission_warning"))

    return decorated_function


@upload_bp.route("/")
@reset_auth
@auth_requires_admin
def index():
    """Redirect to step 1 of the wizard"""
    return redirect(url_for("upload.wizard_step", step_number=1))


@upload_bp.route("/step<int:step_number>")
@reset_auth
@auth_requires_admin
def wizard_step(step_number):

    total_steps = 4

    if step_number < 1 or step_number > total_steps:
        return redirect(url_for("upload.wizard_step", step_number=1))

    try:
        datastacks = get_datastacks()
        if datastacks:
            datastacks.sort()
        else:
            datastacks = []
    except Exception as e:
        current_app.logger.error(
            f"Failed to get datastacks for wizard step {step_number}: {e}",
            exc_info=True,
        )
        datastacks = []

    step_template_path = f"upload/step{step_number}.html"

    return render_template(
        "upload_wizard.html",
        current_step=step_number,
        version=__version__,
        total_steps=total_steps,
        step_template=step_template_path,
        datastacks=datastacks,
        current_user=g.get("auth_user", {}),
    )


@upload_bp.route("/permission-warning")
def permission_warning():
    """
    Display a warning page when user lacks upload permissions
    """
    if is_auth_disabled():
        return redirect(url_for("upload.wizard_step", step_number=1))

    return render_template(
        "permission_warning.html",
        version=__version__,
        current_user=g.get("auth_user", {}),
    )


def _has_datastack_permission(
    auth_user_info: dict, permission_level: str, datastack_name: str
) -> bool:
    """
    Checks if the user has a specific permission level for a given datastack.
    Relies on g.auth_user['permissions'] containing strings like '{datastack_name}_{permission_level}'.
    """
    if not auth_user_info or not datastack_name or not permission_level:
        return False

    permissions = auth_user_info.get("permissions", [])

    required_permission = f"{datastack_name.lower()}_{permission_level.lower()}"

    if required_permission in permissions:
        return True

    return False


@upload_bp.route("/running-uploads")
@reset_auth
@auth_required
def running_uploads_page():
    """Render the running uploads page"""
    viewable_datastacks = []
    if not is_auth_disabled() and g.get("auth_user"):
        try:
            all_datastacks = get_datastacks() or []
            for ds in all_datastacks:
                if _has_datastack_permission(g.auth_user, "view", ds):
                    viewable_datastacks.append(ds)
            if viewable_datastacks:
                viewable_datastacks.sort()
        except Exception as e:
            current_app.logger.error(
                f"Error fetching viewable datastacks for running uploads page: {e}"
            )

    return render_template(
        "upload/running_uploads.html",
        version=__version__,
        current_user=g.get("auth_user", {}),
        viewable_datastacks=viewable_datastacks,
    )


@upload_bp.route(
    "/spatial-lookup/details/<string:datastack_name>/<string:workflow_name>"
)
@reset_auth
@auth_requires_permission("view", table_arg="datastack_name")
def spatial_lookup_workflow_details_page(datastack_name: str, workflow_name: str):
    """Render the spatial lookup workflow details page."""
    database_name = request.args.get("database_name")
    use_staging_database = request.args.get("use_staging_database", type=inputs.boolean)

    return render_template(
        "upload/spatial_workflow_details.html",
        version=__version__,
        current_user=g.get("auth_user", {}),
        datastack_name=datastack_name,
        workflow_name=workflow_name,
        database_name=database_name,
        use_staging_database=use_staging_database,
    )


def create_storage_service():
    config = StorageConfig(
        allowed_origin=current_app.config.get("ALLOWED_ORIGIN"),
    )
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_NAME")
    return StorageService(bucket_name, logger=current_app.logger)


@upload_bp.route("/generate-presigned-url/<string:datastack_name>", methods=["POST"])
@auth_requires_permission("edit", table_arg="datastack_name")
def generate_presigned_url(datastack_name: str):
    data = request.json
    filename = data["filename"]
    content_type = data["contentType"]
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_NAME")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    origin = request.headers.get("Origin") or current_app.config.get(
        "LOCAL_SERVER_URL", "http://localhost:5000"
    )

    try:
        resumable_url = blob.create_resumable_upload_session(
            content_type=content_type,
            origin=origin,  # Allow cross-origin requests for uploads
            timeout=3600,  # Set the session timeout to 1 hour
        )

        return jsonify({"resumableUrl": resumable_url, "origin": origin})
    except google_exceptions.Forbidden as e:
        current_app.logger.error(
            f"GCS Forbidden error generating presigned URL: {str(e)}"
        )
        detailed_message = (
            "Permission denied by Google Cloud Storage. "
            "Please ensure the application's service account has the 'Storage Object Creator' role "
            f"on the bucket '{bucket_name}'. Original error: {str(e)}"
        )
        return jsonify({"status": "error", "message": detailed_message}), 403
    except Exception as e:
        current_app.logger.error(f"Error generating presigned URL: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/get-schema-types", methods=["GET"])
def get_schema_types_endpoint():
    """Endpoint to get available schema types or specific schema details"""
    try:
        schema_name = request.args.get("schema_name", None)
        name_only = request.args.get("name_only", "true").lower() == "true"

        schemas = get_schema_types(schema_name=schema_name, name_only=name_only)

        if schema_name and schemas and not name_only:
            response_data = {"status": "success", "schema": schemas[0]}
        else:
            response_data = {"status": "success", "schemas": schemas}

        return jsonify(response_data), 200

    except Exception as e:
        current_app.logger.error(f"Error getting schema types: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/datastack/<string:datastack_name>/valid-annotation-tables", methods=["GET"])
@auth_required
def get_valid_annotation_tables(datastack_name: str):
    """Get list of valid annotation tables for a given datastack."""
    try:
        datastack_info = get_datastack_info(datastack_name)
        if not datastack_info:
            return jsonify({"status": "error", "message": "Datastack not found"}), 404

        aligned_volume_name = datastack_info["aligned_volume"]["name"]
        
      
        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        
      
        table_names = db.database.get_valid_table_names()
      
        return jsonify({"status": "success", "table_names": table_names})

    except Exception as e:
        current_app.logger.error(f"Error getting valid annotation tables for {datastack_name}: {str(e)}")
        return (
            jsonify({"status": "error", "message": f"Failed to get valid annotation tables: {str(e)}"}),
            500,
        )


@upload_bp.route("/api/get-schema-model", methods=["GET"])
@auth_required
def get_schema_model():
    """Endpoint to get schema model for a specific schema type"""
    try:
        schema_name = request.args.get("schema_name", None)
        if not schema_name:
            return jsonify({"status": "error", "message": "Schema name required"}), 400

        table_metadata = {"reference_table": "your_target_table"}
        schema_model = DynamicSchemaClient.create_annotation_model(
            "example_table",
            schema_name,
            table_metadata=table_metadata,
            reset_cache=True,
            with_crud_columns=False,
        )

        def filter_crud_columns(x):
            return x not in [
                "created",
                "deleted",
                "updated",
                "superceded_id",
                "valid",
            ]

        schema_fields = {}
        for column in schema_model.__table__.columns:
            if filter_crud_columns(column.name):
                schema_fields[column.name] = {
                    "type": str(column.type),
                    "required": not column.nullable,
                    "primary_key": column.primary_key,
                }

        return (
            jsonify(
                {
                    "status": "success",
                    "model": {"name": schema_name, "fields": schema_fields},
                }
            ),
            200,
        )

    except Exception as e:
        current_app.logger.error(f"Error getting schema model: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/save-mapping", methods=["POST"])
@auth_required
def save_mapping():
    """Save the column mapping and ignored columns configuration"""
    try:
        data = request.json
        storage = create_storage_service()

        column_mapping = data.get("columnMapping", {})
        ignored_columns = data.get("ignoredColumns", [])

        success, result = storage.save_metadata(
            filename="filename",
            metadata={
                "column_mapping": column_mapping,
                "ignored_columns": ignored_columns,
            },
        )

        if not isinstance(column_mapping, dict):
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Column mapping must be a dictionary",
                    }
                ),
                400,
            )

        if not isinstance(ignored_columns, list):
            return (
                jsonify(
                    {"status": "error", "message": "Ignored columns must be a list"}
                ),
                400,
            )

        return (
            jsonify(
                {
                    "status": "success",
                    "mapping": {
                        "column_mapping": column_mapping,
                        "ignored_columns": ignored_columns,
                    },
                }
            ),
            200,
        )

    except Exception as e:
        current_app.logger.error(f"Error saving mapping: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def validate_metadata(metadata: Dict[str, Any]) -> tuple[bool, str]:
    """Validate the metadata against required fields and formats"""
    required_fields = {
        "schema_type": str,
        "table_name": str,
        "description": str,
        "voxel_resolution_x": float,
        "voxel_resolution_y": float,
        "voxel_resolution_z": float,
        "write_permission": str,
        "read_permission": str,
    }

    for field, field_type in required_fields.items():
        if field not in metadata:
            return False, f"Missing required field: {field}"
        if not isinstance(metadata[field], field_type):
            return False, f"Invalid type for {field}, expected {field_type}"

    valid_permissions = {"PRIVATE", "GROUP", "PUBLIC"}
    if metadata["write_permission"] not in valid_permissions:
        return False, "Invalid write_permission value"
    if metadata["read_permission"] not in valid_permissions:
        return False, "Invalid read_permission value"

    for field in ["voxel_resolution_x", "voxel_resolution_y", "voxel_resolution_z"]:
        if metadata[field] <= 0:
            return False, f"{field} must be positive"

    return True, ""


@upload_bp.route("/api/save-metadata", methods=["POST"])
@auth_required
def save_metadata():
    """Save the annotation table metadata"""
    try:
        data = request.json
        storage = create_storage_service()

        current_app.logger.info(f"Received metadata: {data}")
        required_fields = [
            "schema_type",
            "datastack_name",
            "table_name",
            "description",
            "voxel_resolution_nm_x",
            "voxel_resolution_nm_y",
            "voxel_resolution_nm_z",
            "write_permission",
            "read_permission",
        ]

        for field in required_fields:
            if not data.get(field):
                current_app.logger.error(f"Missing required field: {field}")
                return (
                    jsonify(
                        {
                            "status": "error",
                            "message": f"Missing required field: {field}",
                        }
                    ),
                    400,
                )

        if data.get("reference_table"):
            table_metadata = {"reference_table": data["reference_table"]}
        else:
            table_metadata = None

        schema_model = DynamicSchemaClient.create_annotation_model(
            "temp_table",
            data["schema_type"],
            table_metadata=table_metadata,
            with_crud_columns=False,
        )

        has_target_id = any(
            column.name == "target_id" for column in schema_model.__table__.columns
        )

        if has_target_id and not data.get("reference_table"):
            return (
                jsonify(
                    {
                        "status": "error",
                        "message": "Reference table is required for this schema type",
                    }
                ),
                400,
            )

        success, result = storage.save_metadata(
            filename=data["table_name"], metadata=data
        )
        return jsonify({"status": "success", "result": result})
    except Exception as e:
        current_app.logger.error(f"Error saving metadata: {str(e)}")


@upload_bp.route("/api/aligned_volumes", methods=["GET"])
@auth_required
def get_aligned_volumes():
    """Get list of available aligned volumes (databases)"""
    try:
        datastacks = current_app.config["DATASTACKS"]

        aligned_volumes = []
        for datastack in datastacks:
            datastack_info = get_datastack_info(datastack)
            aligned_volumes.append(
                {
                    "datastack": datastack,
                    "aligned_volume": datastack_info["aligned_volume"]["name"],
                    "description": datastack_info["aligned_volume"]["description"],
                }
            )

        return jsonify({"status": "success", "aligned_volumes": aligned_volumes})
    except Exception as e:
        current_app.logger.error(f"Error getting aligned volumes: {str(e)}")
        return (
            jsonify({"status": "error", "message": "Failed to get aligned volumes"}),
            500,
        )


@upload_bp.route("/api/aligned_volumes/<aligned_volume>/versions", methods=["GET"])
@auth_required
def get_materialized_versions(aligned_volume):
    """Get available materialized versions for an aligned volume"""
    try:
        with db_manager.session_scope(aligned_volume) as session:

            versions = (
                session.query(AnalysisVersion)
                .filter(AnalysisVersion.valid == True)
                .filter(AnalysisVersion.datastack == aligned_volume)
                .order_by(AnalysisVersion.version.desc())
                .all()
            )

            versions_list = []
            for version in versions:
                versions_list.append(
                    {
                        "version": version.version,
                        "created": version.time_stamp.isoformat(),
                        "expires": (
                            version.expires_on.isoformat()
                            if version.expires_on
                            else None
                        ),
                        "status": version.status,
                        "is_merged": version.is_merged,
                    }
                )

            return jsonify({"status": "success", "versions": versions_list})
    except Exception as e:
        current_app.logger.error(
            f"Error getting versions for {aligned_volume}: {str(e)}"
        )
        return (
            jsonify(
                {
                    "status": "error",
                    "message": f"Failed to get versions for {aligned_volume}",
                }
            ),
            500,
        )


@upload_bp.route("/api/process/start", methods=["POST"])
@auth_required
def start_csv_processing():
    """Start CSV processing job"""
    r = request.get_json()
    schema = UploadRequestSchema()
    file_metadata = schema.load(r)

    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_NAME")

    file_path = f"gs://{bucket_name}/{file_metadata.get('filename')}"

    sql_instance_name = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    database_name = current_app.config.get("STAGING_DATABASE_NAME")
    datastack_name = file_metadata["metadata"]["datastack_name"]
    datastack_info = get_datastack_info(datastack_name)
    user_id = str(g.auth_user["id"])

    file_metadata["metadata"]["user_id"] = user_id

    if not all([sql_instance_name, bucket_name, database_name]):
        return (
            jsonify({"status": "error", "message": "Missing required configuration"}),
            500,
        )

    result = process_and_upload.s(
        file_path, file_metadata, datastack_info, user_id=user_id
    ).apply_async()

    return jsonify({"status": "start", "task_id": result.id})


@upload_bp.route("/api/process/status/<job_id>", methods=["GET"])
@auth_required
def check_processing_status(job_id):
    """Get processing job status"""
    status = get_job_status(job_id)
    if not status:
        return jsonify({"status": "error", "message": "Job not found"}), 404

    if _check_authorization(status):
        return jsonify({"status": "error", "message": "Forbidden"}), 403

    _set_item_type(status)

    if status.get("active_workflow_part") == "spatial_lookup":
        _handle_spatial_lookup(status, job_id)

    return jsonify(status)


def _check_authorization(status):
    """Check if user has permission to access this job"""
    if is_auth_disabled() or not g.get("auth_user"):
        return False

    user_id = str(g.auth_user["id"])
    job_user_id = status.get("user_id")

    if job_user_id == user_id:
        return False

    datastack_name = status.get("datastack_name")
    return not datastack_name or not _has_datastack_permission(
        g.auth_user, "view", datastack_name
    )


def _set_item_type(status):
    """Set the item_type based on workflow state"""
    if status.get("active_workflow_part") == "transfer":
        status["item_type"] = "steps"
    elif status.get("status") == "completed":
        status["item_type"] = "done"
    else:
        status["item_type"] = "rows"


def _handle_spatial_lookup(status, job_id):
    """Process spatial lookup workflow details"""
    status["item_type"] = "chunks"

    spatial_config = status.get("spatial_lookup_config")
    if not spatial_config:
        status["phase"] = "Spatial Lookup: Configuration not found"
        return

    table_name = spatial_config.get("table_name")
    db_name = spatial_config.get("database_name")

    if not table_name or not db_name:
        status["phase"] = "Spatial Lookup: Configuration missing"
        return

    try:
        spatial_data = _get_spatial_data(db_name, table_name)
        if not spatial_data:
            status["phase"] = "Spatial Lookup: Awaiting status data..."
            return

        _update_status_with_spatial_data(status, spatial_data)
    except Exception as e:
        current_app.logger.error(
            f"Error fetching spatial lookup status for job {job_id}: {e}"
        )
        status["phase"] = "Spatial Lookup: Error fetching details"


def _get_spatial_data(db_name, table_name):
    """Retrieve spatial data from checkpoint manager"""
    checkpoint_manager = RedisCheckpointManager(db_name)
    return checkpoint_manager.get_workflow_data(table_name)


def _update_status_with_spatial_data(status, spatial_data):
    """Update status object with spatial data details"""
    status["phase"] = f"Spatial Lookup: {spatial_data.status}"
    status["progress"] = round(spatial_data.progress, 2)
    status["total_rows"] = spatial_data.total_chunks
    status["processed_rows"] = spatial_data.completed_chunks

    status["spatial_lookup_details"] = {
        "total_chunks": spatial_data.total_chunks,
        "completed_chunks": spatial_data.completed_chunks,
        "rows_processed_in_spatial": spatial_data.rows_processed,
        "processing_rate_spatial": spatial_data.processing_rate,
        "estimated_completion_spatial": spatial_data.estimated_completion,
        "chunking_strategy": spatial_data.chunking_strategy,
        "used_chunk_size": spatial_data.used_chunk_size,
    }

    if spatial_data.status and spatial_data.status.lower() in ["failed", "error"]:
        status["status"] = "error"
        status["error"] = spatial_data.last_error or "Spatial lookup failed."
    elif (
        spatial_data.status
        and spatial_data.status.lower()
        == get_config_param("CHUNK_STATUS_COMPLETED", "completed").lower()
    ):
        if status.get("status") not in ["error", "completed"]:
            status["status"] = "processing"
            status["phase"] = "Spatial Lookup: Completed, awaiting transfer"


@upload_bp.route("/api/process/cancel/<job_id>", methods=["POST"])
@auth_required
def cancel_job(job_id):
    """Cancel processing job"""
    try:
        if not is_auth_disabled() and g.get("auth_user"):
            status = get_job_status(job_id)
            if not status:
                return jsonify({"status": "error", "message": "Job not found"}), 404
            user_id = str(g.auth_user["id"])
            job_user_id = status.get("user_id")
            datastack_name = status.get("datastack_name")

            can_cancel = False
            if job_user_id == user_id:
                can_cancel = True
            elif datastack_name and _has_datastack_permission(
                g.auth_user, "admin", datastack_name
            ):
                can_cancel = True

            if not can_cancel:
                return (
                    jsonify(
                        {"status": "error", "message": "Forbidden to cancel this job"}
                    ),
                    403,
                )

        result = cancel_processing_job.delay(job_id)
        status = result.get(timeout=10)

        return jsonify(
            {"status": "success", "message": "Processing cancelled", "details": status}
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/process/dismiss/<job_id>", methods=["DELETE"])
@auth_required
def dismiss_job(job_id):
    """Dismiss/delete a completed, failed, or cancelled job"""
    try:
        status = get_job_status(job_id)
        if not status:
            return jsonify({"status": "error", "message": "Job not found"}), 404

        if not is_auth_disabled() and g.get("auth_user"):
            user_id = str(g.auth_user["id"])
            job_user_id = status.get("user_id")
            datastack_name = status.get("datastack_name")

            can_dismiss = False
            if job_user_id == user_id:
                can_dismiss = True
            elif datastack_name and _has_datastack_permission(
                g.auth_user, "admin", datastack_name
            ):
                can_dismiss = True

            if not can_dismiss:
                return (
                    jsonify(
                        {"status": "error", "message": "Forbidden to dismiss this job"}
                    ),
                    403,
                )

        job_status = status.get("status", "").lower()
        if job_status in ["processing", "pending", "preparing"]:
            return (
                jsonify(
                    {
                        "status": "error", 
                        "message": "Cannot dismiss active jobs. Cancel the job first."
                    }
                ),
                400,
            )

        job_key = f"csv_processing:{job_id}"
        deleted = REDIS_CLIENT.delete(job_key)
        
        if deleted:
            current_app.logger.info(f"Job {job_id} dismissed by user {user_id if g.get('auth_user') else 'unknown'}")
            return jsonify(
                {"status": "success", "message": "Job dismissed successfully"}
            )
        else:
            return jsonify(
                {"status": "error", "message": "Job not found or already dismissed"}
            ), 404

    except Exception as e:
        current_app.logger.error(f"Error dismissing job {job_id}: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/process/user-jobs", methods=["GET"])
@auth_required
def get_user_jobs():
    """Get all job statuses for the current user, filtered by permissions."""
    if not g.get("auth_user") and not is_auth_disabled():
        return jsonify({"status": "error", "message": "Authentication required"}), 401

    user_jobs = []
    try:
        user_id = str(g.auth_user["id"]) if g.get("auth_user") else None
        job_keys = REDIS_CLIENT.scan_iter("csv_processing:*")

        for job_key in job_keys:
            job_data_raw = REDIS_CLIENT.get(job_key)
            if job_data_raw:
                try:
                    job_data = json.loads(job_data_raw.decode("utf-8"))
                    job_data["job_id"] = job_key.decode("utf-8").split(":", 1)[1]

                    if is_auth_disabled():
                        user_jobs.append(job_data)
                        continue

                    job_user_id = job_data.get("user_id")
                    datastack_name = job_data.get("datastack_name")

                    if job_user_id == user_id:
                        user_jobs.append(job_data)
                    elif datastack_name and _has_datastack_permission(
                        g.auth_user, "view", datastack_name
                    ):
                        user_jobs.append(job_data)

                except json.JSONDecodeError as e:
                    current_app.logger.error(
                        f"Error decoding JSON for job key {job_key}: {e}"
                    )
                except Exception as e:
                    current_app.logger.error(f"Error processing job key {job_key}: {e}")

        user_jobs.sort(key=lambda x: x.get("last_updated", ""), reverse=True)

        return jsonify({"status": "success", "jobs": user_jobs})
    except Exception as e:
        current_app.logger.error(f"Error fetching user jobs: {str(e)}")
        return (
            jsonify(
                {"status": "error", "message": f"Failed to get user jobs: {str(e)}"}
            ),
            500,
        )


@spatial_lookup_bp.expect(spatial_svid_parser)
@spatial_lookup_bp.route(
    "/run/spatial_lookup/datastack/<string:datastack_name>/<string:table_name>"
)
class SpatialSVIDLookupTableResource(Resource):
    @reset_auth
    @auth_requires_permission("edit", table_arg="datastack_name")
    @spatial_lookup_bp.doc("Lookup spatially chunked svid workflow", security="apikey")
    def post(self, datastack_name: str, table_name: str):
        """Process newly added annotations and lookup segmentation data using
        a spatially chunked svid lookup strategy. Optionally also lookups root ids.

        Args:
            datastack_name (str): name of datastack from infoservice
            table_name (str): name of table
        """
        from materializationengine.workflows.spatial_lookup import (
            run_spatial_lookup_workflow,
        )

        args = spatial_svid_parser.parse_args()

        if datastack_name not in current_app.config["DATASTACKS"]:
            abort(404, f"datastack {datastack_name} not configured for materialization")

        datastack_info = get_datastack_info(datastack_name)

        chunk_scale_factor = args["chunk_scale_factor"]
        supervoxel_batch_size = args["supervoxel_batch_size"]
        use_staging_database = args["use_staging_database"]
        resume_from_checkpoint = args["resume_from_checkpoint"]
        try:
            run_spatial_lookup_workflow.si(
                datastack_info,
                table_name=table_name,
                chunk_scale_factor=chunk_scale_factor,
                supervoxel_batch_size=supervoxel_batch_size,
                use_staging_database=use_staging_database,
                resume_from_checkpoint=resume_from_checkpoint,
            ).apply_async()
        except Exception as e:
            current_app.error(e)
            return abort(400, f"Error running spatial lookup workflow: {e}")
        return 200


@spatial_lookup_bp.expect(spatial_lookup_status)
@spatial_lookup_bp.route(
    "/status/datastack/<string:datastack_name>/<string:table_name>"
)
class SpatialLookupStatus(Resource):
    @reset_auth
    @auth_requires_permission("edit", table_arg="datastack_name")
    @spatial_lookup_bp.doc("get_spatial_lookup_status", security="apikey")
    def get(self, datastack_name: str, table_name: str):
        """Get the status of a spatial lookup workflow for a specific table."""
        try:
            if datastack_name not in current_app.config["DATASTACKS"]:
                abort(
                    404,
                    f"datastack {datastack_name} not configured for materialization",
                )

            datastack_info = get_datastack_info(datastack_name)
            aligned_volume_name = datastack_info["aligned_volume"]["name"]

            args = spatial_lookup_status.parse_args()
            use_staging_database = args["use_staging_database"]

            if use_staging_database:

                staging_database = get_config_param("STAGING_DATABASE_NAME")
                database = request.args.get("database", staging_database)
            else:
                database = aligned_volume_name

            checkpoint_manager = RedisCheckpointManager(database)
            checkpoint = checkpoint_manager.get_workflow_data(table_name)

            if checkpoint:
                status = {
                    "status": checkpoint.status,
                    "table_name": checkpoint.table_name,
                    "database": database,
                    "total_chunks": checkpoint.total_chunks,
                    "completed_chunks": checkpoint.completed_chunks,
                    "progress": checkpoint.progress,
                    "submitted_chunks": checkpoint.submitted_chunks,
                    "rows_processed": checkpoint.rows_processed,
                    "processing_rate": checkpoint.processing_rate,
                    "estimated_completion": checkpoint.estimated_completion,
                    "last_error": checkpoint.last_error,
                    "updated_at": checkpoint.updated_at,
                    "created_at": checkpoint.created_at,
                    "chunking_strategy": checkpoint.chunking_strategy,
                    "used_chunk_size": checkpoint.used_chunk_size,
                }

                if checkpoint.last_processed_chunk:
                    status["last_processed_chunk"] = {
                        "min_corner": checkpoint.last_processed_chunk.min_corner,
                        "max_corner": checkpoint.last_processed_chunk.max_corner,
                        "index": checkpoint.last_processed_chunk.index,
                    }

                if checkpoint.start_time and checkpoint.status not in [
                    "completed",
                    "failed",
                ]:
                    try:
                        start_time = datetime.datetime.fromisoformat(
                            checkpoint.start_time
                        )
                        now = datetime.datetime.now(datetime.timezone.utc)
                        elapsed_seconds = (now - start_time).total_seconds()

                        hours, remainder = divmod(elapsed_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        status["elapsed_time_formatted"] = (
                            f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                        )
                        status["elapsed_time_seconds"] = elapsed_seconds
                    except Exception as e:
                        current_app.logger.error(f"Error calculating elapsed time: {e}")

                return {"status": status}

            return {
                "status": "error",
                "message": f"No workflow data found for table {table_name}",
            }, 404

        except Exception as e:
            current_app.logger.error(f"Error getting spatial lookup status: {str(e)}")
            return {"status": "error", "message": str(e)}, 500


@spatial_lookup_bp.expect(spatial_lookup_status)
@spatial_lookup_bp.route("/active/datastack/<string:datastack_name>")
class ActiveSpatialLookups(Resource):
    @reset_auth
    @auth_requires_permission("edit", table_arg="datastack_name")
    @spatial_lookup_bp.doc("get_active_spatial_lookups", security="apikey")
    def get(self, datastack_name: str):
        """Get a list of all active spatial lookup workflows for a specific datastack."""
        try:
            if datastack_name not in current_app.config["DATASTACKS"]:
                abort(
                    404,
                    f"datastack {datastack_name} not configured for materialization",
                )

            datastack_info = get_datastack_info(datastack_name)
            aligned_volume_name = datastack_info["aligned_volume"]["name"]

            args = spatial_lookup_status.parse_args()
            use_staging_database = args.get("use_staging_database", False)

            target_db_for_datastack = None
            if use_staging_database:
                target_db_for_datastack = get_config_param("STAGING_DATABASE_NAME")
            else:
                target_db_for_datastack = aligned_volume_name

            if not target_db_for_datastack:
                return {
                    "status": "error",
                    "message": "Could not determine database for datastack.",
                }, 500

            checkpoint_manager = RedisCheckpointManager(target_db_for_datastack)
            workflows = checkpoint_manager.get_active_workflows()

            for wf in workflows:
                wf["datastack_name"] = datastack_name
                wf["database_name"] = target_db_for_datastack
                if (
                    wf.get("completed_chunks")
                    and wf.get("total_chunks")
                    and wf["total_chunks"] > 0
                ):
                    wf["progress_percentage"] = round(
                        (wf["completed_chunks"] / wf["total_chunks"]) * 100,
                        2,
                    )
                if wf.get("start_time") and wf.get("status") not in [
                    "completed",
                    "failed",
                ]:
                    try:
                        start_time = datetime.datetime.fromisoformat(wf["start_time"])
                        now = datetime.datetime.now(datetime.timezone.utc)
                        elapsed_seconds = (now - start_time).total_seconds()

                        hours, remainder = divmod(elapsed_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        wf["elapsed_time_formatted"] = (
                            f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                        )
                        wf["elapsed_time_seconds"] = elapsed_seconds
                    except Exception as e:
                        current_app.logger.debug(
                            f"Error calculating elapsed time for workflow {wf.get('table_name')}: {e}"
                        )

            return {"status": "success", "workflows": workflows}

        except Exception as e:
            current_app.logger.error(
                f"Error getting active spatial lookups for {datastack_name}: {str(e)}"
            )
            return {"status": "error", "message": str(e)}, 500


@spatial_lookup_bp.route("/all-active-direct")
class AllActiveDirectSpatialLookups(Resource):
    @reset_auth
    @auth_required
    @spatial_lookup_bp.doc("get_all_active_direct_spatial_lookups", security="apikey")
    def get(self):
        """Get a list of all active directly-run spatial lookup workflows from all relevant databases."""
        all_workflows = []
        processed_db_workflow_pairs = set()

        try:
            configured_datastacks = current_app.config.get("DATASTACKS", [])
            for datastack_name_iter in configured_datastacks:
                staging_db_name = get_config_param("STAGING_DATABASE_NAME")
                if staging_db_name:
                    try:
                        checkpoint_manager_staging = RedisCheckpointManager(
                            staging_db_name
                        )
                        staging_workflows = (
                            checkpoint_manager_staging.get_active_workflows()
                        )
                        for wf in staging_workflows:

                            wf["database_name"] = staging_db_name
                            wf["datastack_name"] = datastack_name_iter

                            pair = (staging_db_name, wf.get("table_name"))
                            if pair not in processed_db_workflow_pairs:
                                all_workflows.append(wf)
                                processed_db_workflow_pairs.add(pair)
                    except Exception as e_staging:
                        current_app.logger.error(
                            f"Error fetching workflows from staging DB {staging_db_name}: {str(e_staging)}"
                        )

                try:
                    ds_info = get_datastack_info(datastack_name_iter)
                    prod_db_name = ds_info["aligned_volume"]["name"]
                    if prod_db_name and prod_db_name != staging_db_name:
                        checkpoint_manager_prod = RedisCheckpointManager(prod_db_name)
                        prod_workflows = checkpoint_manager_prod.get_active_workflows()
                        for wf in prod_workflows:
                            wf["datastack_name"] = datastack_name_iter
                            wf["database_name"] = prod_db_name
                            pair = (prod_db_name, wf.get("table_name"))
                            if pair not in processed_db_workflow_pairs:
                                all_workflows.append(wf)
                                processed_db_workflow_pairs.add(pair)
                except Exception as e_prod:
                    current_app.logger.error(
                        f"Error fetching workflows from production DB for datastack {datastack_name_iter}: {str(e_prod)}"
                    )

            for wf in all_workflows:
                if (
                    wf.get("completed_chunks") is not None
                    and wf.get("total_chunks") is not None
                    and wf["total_chunks"] > 0
                ):
                    wf["progress_percentage"] = round(
                        (wf["completed_chunks"] / wf["total_chunks"]) * 100,
                        2,
                    )
                else:
                    wf["progress_percentage"] = 0

                if wf.get("start_time") and wf.get("status") not in [
                    "completed",
                    "failed",
                    "ERROR",
                ]:
                    try:
                        start_time = datetime.datetime.fromisoformat(wf["start_time"])
                        now = datetime.datetime.now(datetime.timezone.utc)
                        elapsed_seconds = (now - start_time).total_seconds()
                        hours, remainder = divmod(elapsed_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        wf["elapsed_time_formatted"] = (
                            f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                        )
                        wf["elapsed_time_seconds"] = elapsed_seconds
                    except Exception as e_time:
                        current_app.logger.debug(
                            f"Error calculating elapsed time for workflow {wf.get('table_name')} in DB {wf.get('database_name')}: {e_time}"
                        )
                        wf["elapsed_time_formatted"] = "Error"

            return {"status": "success", "workflows": all_workflows}

        except Exception as e:
            current_app.logger.error(
                f"Error getting all active direct spatial lookups: {str(e)}",
                exc_info=True,
            )
            return {"status": "error", "message": str(e)}, 500


@spatial_lookup_bp.route(
    "/workflow-details/datastack/<string:datastack_name>/workflow/<string:workflow_name>"
)
@spatial_lookup_bp.doc("get_spatial_lookup_workflow_details", security="apikey")
class SpatialLookupWorkflowDetails(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    def get(self, datastack_name: str, workflow_name: str):
        """Get detailed status and chunk information for a spatial lookup workflow."""
        try:
            if datastack_name not in current_app.config["DATASTACKS"]:
                abort(
                    404,
                    f"Datastack {datastack_name} not configured for materialization.",
                )

            database_name_arg = request.args.get("database_name")
            use_staging_db_arg = request.args.get(
                "use_staging_database", type=inputs.boolean
            )

            target_database = None
            if database_name_arg:
                target_database = database_name_arg
            elif use_staging_db_arg is True:
                target_database = get_config_param("STAGING_DATABASE_NAME")
            elif use_staging_db_arg is False:
                datastack_info = get_datastack_info(datastack_name)
                target_database = datastack_info["aligned_volume"]["name"]
            else:
                target_database = get_config_param("STAGING_DATABASE_NAME")

            if not target_database:
                abort(400, "Could not determine the target database for the workflow.")

            checkpoint_manager = RedisCheckpointManager(target_database)

            workflow_data = checkpoint_manager.get_workflow_data(workflow_name)
            if not workflow_data:
                return {
                    "status": "error",
                    "message": f"No workflow data found for workflow '{workflow_name}' in database '{target_database}'.",
                }, 404

            all_chunk_statuses_raw = (
                checkpoint_manager.get_all_chunk_statuses(workflow_name) or {}
            )
            all_failed_details_raw = (
                checkpoint_manager.get_all_failed_chunk_details(workflow_name) or {}
            )

            chunks_details = []
            status_counts = {
                CHUNK_STATUS_PENDING: 0,
                CHUNK_STATUS_PROCESSING: 0,
                CHUNK_STATUS_COMPLETED: 0,
                CHUNK_STATUS_FAILED_RETRYABLE: 0,
                CHUNK_STATUS_FAILED_PERMANENT: 0,
                "UNKNOWN": 0,
            }

            total_chunks_from_workflow = (
                workflow_data.total_chunks
                if workflow_data.total_chunks is not None
                else 0
            )

            if total_chunks_from_workflow > 0:
                for i in range(total_chunks_from_workflow):
                    chunk_idx_str = str(i)
                    status = all_chunk_statuses_raw.get(
                        chunk_idx_str, CHUNK_STATUS_PENDING
                    )

                    detail = {"chunk_index": i, "status": status}

                    if status in [
                        CHUNK_STATUS_FAILED_RETRYABLE,
                        CHUNK_STATUS_FAILED_PERMANENT,
                    ]:
                        failed_info = all_failed_details_raw.get(chunk_idx_str, {})
                        detail["error_message"] = failed_info.get(
                            "error_message", "N/A"
                        )
                        detail["error_type"] = failed_info.get("error_type", "N/A")
                        detail["attempt_count"] = failed_info.get("attempt_count", 0)

                    chunks_details.append(detail)
                    status_counts[status] = status_counts.get(status, 0) + 1

            defined_statuses = [
                CHUNK_STATUS_PENDING,
                CHUNK_STATUS_PROCESSING,
                CHUNK_STATUS_COMPLETED,
                CHUNK_STATUS_FAILED_RETRYABLE,
                CHUNK_STATUS_FAILED_PERMANENT,
            ]
            for ds in defined_statuses:
                if ds not in status_counts:
                    status_counts[ds] = 0

            if total_chunks_from_workflow > 0:
                explicitly_statused_chunks = sum(
                    count
                    for st, count in status_counts.items()
                    if st != CHUNK_STATUS_PENDING and st != "UNKNOWN"
                )
                calculated_pending = (
                    total_chunks_from_workflow - explicitly_statused_chunks
                )
                status_counts[CHUNK_STATUS_PENDING] = max(0, calculated_pending)
                current_total_from_counts = sum(status_counts.values())
                if (
                    current_total_from_counts > total_chunks_from_workflow
                    and status_counts.get("UNKNOWN", 0) > 0
                ):
                    status_counts["UNKNOWN"] = max(
                        0,
                        status_counts["UNKNOWN"]
                        - (current_total_from_counts - total_chunks_from_workflow),
                    )

            workflow_summary_dict = asdict(workflow_data)
            workflow_summary_dict["progress"] = workflow_data.progress

            return {
                "status": "success",
                "datastack_name": datastack_name,
                "workflow_name": workflow_name,
                "target_database": target_database,
                "workflow_summary": workflow_summary_dict,
                "status_counts": status_counts,
                "chunks": chunks_details,
            }

        except Exception as e:
            current_app.logger.error(
                f"Error getting spatial lookup workflow details for {workflow_name}: {str(e)}",
                exc_info=True,
            )
            return {"status": "error", "message": str(e)}, 500


@upload_bp.route("/active-spatial-workflows")
@auth_required
def active_spatial_workflows_page():
    """Render the page for active directly-run spatial workflows."""
    return render_template(
        "upload/active_spatial_workflows.html",
        version=__version__,
        current_user=g.get("auth_user", {}),
    )
