import datetime
import functools
import os
from typing import Any, Dict

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
from flask_accepts import accepts
from flask_restx import Namespace, Resource, fields, inputs, reqparse
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
from middle_auth_client import auth_requires_admin, auth_requires_permission, auth_required
from redis import StrictRedis

from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.blueprints.upload.checkpoint_manager import (
    RedisCheckpointManager,
)
from materializationengine.blueprints.upload.schema_helper import get_schema_types
from materializationengine.blueprints.upload.schemas import UploadRequestSchema
from materializationengine.blueprints.upload.storage import (
    StorageConfig,
    StorageService,
)
from materializationengine.blueprints.upload.tasks import (
    cancel_processing,
    get_job_status,
    process_and_upload,
)
from materializationengine.database import db_manager
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
        datastacks.sort()
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


def create_storage_service():
    config = StorageConfig(
        allowed_origin=current_app.config.get("ALLOWED_ORIGIN"),
    )
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
    return StorageService(bucket_name, logger=current_app.logger)


@upload_bp.route("/generate-presigned-url/<string:datastack_name>", methods=["POST"])
@auth_requires_permission("edit", table_arg="datastack_name")
def generate_presigned_url(datastack_name: str):
    data = request.json
    filename = data["filename"]
    content_type = data["contentType"]
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
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
        current_app.logger.error(f"GCS Forbidden error generating presigned URL: {str(e)}")
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

    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")

    file_path = f"gs://{bucket_name}/{file_metadata.get('filename')}"

    sql_instance_name = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
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

    result = process_and_upload.si(
        file_path, file_metadata, datastack_info
    ).apply_async()

    return jsonify({"status": "start", "task_id": result.id})


@upload_bp.route("/api/process/status/<job_id>", methods=["GET"])
@auth_required
def check_processing_status(job_id):
    """Get processing job status"""
    try:
        status = get_job_status(job_id)
        if not status:
            return jsonify({"status": "error", "message": "Job not found"}), 404

        return jsonify(status)

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/api/process/cancel/<job_id>", methods=["POST"])
@auth_required
def cancel_processing_job(job_id):
    """Cancel processing job"""
    try:
        result = cancel_processing.delay(job_id)
        status = result.get(timeout=10)

        return jsonify(
            {"status": "success", "message": "Processing cancelled", "details": status}
        )

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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
        """Get a list of all active spatial lookup workflows."""
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
            # This already returns a list of dictionaries5
            workflows = checkpoint_manager.get_active_workflows()

            for workflow in workflows:
                # Process each workflow to add calculated fields
                if (
                    workflow.get("completed_chunks")
                    and workflow.get("total_chunks")
                    and workflow["total_chunks"] > 0
                ):
                    workflow["progress_percentage"] = round(
                        (workflow["completed_chunks"] / workflow["total_chunks"]) * 100,
                        2,
                    )

                if workflow.get("start_time") and workflow.get("status") not in [
                    "completed",
                    "failed",
                ]:
                    try:
                        start_time = datetime.datetime.fromisoformat(
                            workflow["start_time"]
                        )
                        now = datetime.datetime.now(datetime.timezone.utc)
                        elapsed_seconds = (now - start_time).total_seconds()

                        hours, remainder = divmod(elapsed_seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)
                        workflow["elapsed_time_formatted"] = (
                            f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
                        )
                        workflow["elapsed_time_seconds"] = elapsed_seconds
                    except Exception as e:
                        current_app.logger.debug(f"Error calculating elapsed time: {e}")

            return {"workflows": workflows}

        except Exception as e:
            current_app.logger.error(f"Error getting active spatial lookups: {str(e)}")
            return {"status": "error", "message": str(e)}, 500
