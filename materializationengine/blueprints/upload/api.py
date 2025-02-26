from typing import Any, Dict

from dynamicannotationdb.models import AnalysisVersion
from dynamicannotationdb.schema import DynamicSchemaClient
from flask import (
    Blueprint,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    url_for,
)
from google.cloud import storage
from redis import StrictRedis

from materializationengine.blueprints.upload.schema_helper import get_schema_types
from materializationengine.blueprints.upload.storage import (
    StorageConfig,
    StorageService,
)
from materializationengine.blueprints.upload.schemas import UploadRequestSchema
from materializationengine.blueprints.upload.tasks import (
    cancel_processing,
    get_job_status,
    process_and_upload,
)
from materializationengine.database import sqlalchemy_cache
from materializationengine.info_client import get_datastack_info
from materializationengine.utils import get_config_param
from middle_auth_client import auth_requires_admin, auth_requires_permission

__version__ = "4.35.0"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

upload_bp = Blueprint(
    "upload",
    __name__,
    url_prefix="/materialize/upload",
)

REDIS_CLIENT = StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)


def create_storage_service():
    config = StorageConfig(
        allowed_origin=current_app.config.get("ALLOWED_ORIGIN"),
    )
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
    return StorageService(bucket_name, logger=current_app.logger)


@upload_bp.route("/")
def index():
    """Redirect to step 1 of the wizard"""
    return redirect(url_for("upload.wizard_step", step_number=1))


@upload_bp.route("/step<int:step_number>")
def wizard_step(step_number):
    if step_number < 1 or step_number > 4:
        return redirect(url_for("upload.wizard_step", step_number=1))

    return render_template(
        "upload_wizard.html",
        current_step=step_number,
        step_template=f"upload/step{step_number}.html",
        version=__version__,
    )


@upload_bp.route("/generate-presigned-url", methods=["POST"])
def generate_presigned_url():
    data = request.json
    filename = data["filename"]
    content_type = data["contentType"]
    bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(filename)
    origin = request.headers.get("Origin") or current_app.config.get(
        "ALLOWED_ORIGIN", "http://localhost:5000"
    )

    try:
        resumable_url = blob.create_resumable_upload_session(
            content_type=content_type,
            origin=origin,  # Allow cross-origin requests for uploads
            timeout=3600,  # Set the session timeout to 1 hour
        )

        return jsonify({"resumableUrl": resumable_url, "origin": origin})
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
def get_materialized_versions(aligned_volume):
    """Get available materialized versions for an aligned volume"""
    try:
        session = sqlalchemy_cache.get(aligned_volume)

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
                        version.expires_on.isoformat() if version.expires_on else None
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
@auth_requires_permission("edit", table_arg="datastack_name")
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

    if not all([sql_instance_name, bucket_name, database_name]):
        return (
            jsonify({"status": "error", "message": "Missing required configuration"}),
            500,
        )

    result = process_and_upload.si(
        file_path, file_metadata, datastack_info
    ).apply_async()

    return jsonify({"status": "start", "jobId": result.id})


@upload_bp.route("/api/process/status/<job_id>", methods=["GET"])
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
