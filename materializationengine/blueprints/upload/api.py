import datetime
import json
from typing import Any, Dict

from dynamicannotationdb.schema import DynamicSchemaClient
from flask import Blueprint, current_app, jsonify, render_template, request, session
from flask_restx import Namespace, Resource, inputs, reqparse, fields

from google.cloud import storage
from redis import StrictRedis

from materializationengine.blueprints.upload.schema_helper import get_schema_types
from materializationengine.utils import get_config_param

__version__ = "4.35.0"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

upload_bp = Namespace(
    "Upload API",
    authorizations=authorizations,
    description="Upload API",
    path="/upload",
)

REDIS_CLIENT = StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)


@upload_bp.route("/generate-presigned-url", methods=["POST"])
def generate_presigned_url():
    data = request.json
    filename = data["filename"]
    content_type = data["contentType"]

    storage_client = storage.Client()
    bucket = storage_client.bucket("test_annotation_csv_upload")
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
        print(f"Error creating resumable upload session: {str(e)}")
        return jsonify({"error": str(e)}), 500


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


def store_metadata(filename: str, metadata: Dict[str, Any]) -> tuple[bool, str]:
    """Store metadata in Google Cloud Storage"""
    try:
        metadata["created"] = datetime.datetime.now().isoformat()
        metadata["schema_type"] = metadata["schema_info"]["name"]
        is_valid, error_msg = validate_metadata(metadata)
        if not is_valid:
            return False, error_msg

        metadata_filename = f"{filename}.metadata.json"

        storage_client = storage.Client()
        bucket = storage_client.bucket("test_annotation_csv_upload")

        blob = bucket.blob(metadata_filename)
        blob.upload_from_string(
            data=json.dumps(metadata, indent=2), content_type="application/json"
        )

        return True, metadata_filename

    except Exception as e:
        current_app.logger.error(f"Error storing metadata: {str(e)}")
        return False, f"Error storing metadata: {str(e)}"


@upload_bp.route("/store-metadata", methods=["POST"])
def handle_metadata():
    """Handle metadata storage request"""
    try:
        data = request.get_json()
        if not data or "filename" not in data or "metadata" not in data:
            return jsonify({"error": "Missing filename or metadata"}), 400
        metadata = data["metadata"]
        for key in ["voxel_resolution_x", "voxel_resolution_y", "voxel_resolution_z"]:
            if key in metadata:
                try:
                    metadata[key] = float(metadata[key])
                except (TypeError, ValueError):
                    return (
                        jsonify(
                            {
                                "status": "error",
                                "message": f"Invalid value for {key}. Must be a number.",
                            }
                        ),
                        400,
                    )

        success, result = store_metadata(data["filename"], data["metadata"])

        if success:
            return jsonify({"status": "success", "metadata_file": result}), 200
        else:
            return jsonify({"status": "error", "message": result}), 400

    except Exception as e:
        current_app.logger.error(f"Error handling metadata: {str(e)}")
        return jsonify({"status": "error", "message": f"Server error: {str(e)}"}), 500


@upload_bp.route("/get-schema-model", methods=["GET"])
def get_schema_model():
    """Endpoint to get schema model for a specific schema type"""
    try:
        schema_name = request.args.get("schema_name", None)
        table_metadata = {"reference_table": "your_target_table"}
        schema_model = DynamicSchemaClient.create_annotation_model(
            "example_table",
            schema_name,
            table_metadata=table_metadata,
            reset_cache=True,
        )

        def filter_crud_columns(x):
            return x not in [
                "created",
                "deleted",
                "updated",
                "superceded_id",
                "valid",
            ]

        annotation_columns = []
        for column in schema_model.__table__.columns:
            if filter_crud_columns(column.name):
                annotation_columns.append(column.name)
        return jsonify({"status": "success", "schema": annotation_columns}), 200

    except Exception as e:
        current_app.logger.error(f"Error getting schema model: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/get-schema-types", methods=["GET"])
def get_schema_types_endpoint():
    """Endpoint to get available schema types or specific schema details"""
    try:
        schema_name = request.args.get("schema_name", None)
        name_only = request.args.get("name_only", "true").lower() == "true"

        current_app.logger.info(
            f"Getting schemas with params: schema_name={schema_name}, name_only={name_only}"
        )

        schemas = get_schema_types(schema_name=schema_name, name_only=name_only)
        current_app.logger.info(f"Retrieved schemas: {schemas}")

        if schema_name and schemas and not name_only:
            response_data = {"status": "success", "schema": schemas[0]}
        else:
            response_data = {"status": "success", "schemas": schemas}

        current_app.logger.info(f"Returning response: {response_data}")
        return jsonify(response_data), 200

    except Exception as e:
        current_app.logger.error(f"Error getting schema types: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/upload-complete", methods=["POST"])
def upload_complete():
    filename = request.json["filename"]
    # TODO maybe add some callback logic here
    return jsonify(
        {"status": "success", "message": f"{filename} uploaded successfully"}
    )

@upload_bp.route("/update-step", methods=["POST"])
def update_step():
    """Update wizard step in the session"""
    try:
        data = request.get_json()
        current_step = data.get("current_step", session.get("current_step", 0))

        session["current_step"] = current_step
        session.modified = True
        return jsonify({"status": "success", "current_step": session["current_step"]})
    except Exception as e:
        current_app.logger.error(f"Error updating step: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@upload_bp.route("/step/<int:step_number>", methods=["GET"])
def get_step(step_number: int):
    """Update wizard step in the session"""
    current_step = request.args.get("current_step", type=int)
    session["current_step"] = current_step

    if step_number < 0 or step_number >= 5:  # TODO needs to not be hardcoded
        return jsonify({"status": "error", "message": "Invalid step"}), 400

    session["current_step"] = step_number

    return render_template(
        "/csv_upload/main.html", current_step=step_number, total_steps=5
    )

@upload_bp.route("/databases", methods=["GET"])
def get_databases():
    try:
        databases = [
            {
                "id": "default",
                "name": "Default Database",
                "description": "Primary annotation database",
                "isDefault": True,
                "isRequired": True,
            }
            # Add other databases as needed
        ]
        return jsonify({"status": "success", "databases": databases}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/save-session", methods=["POST"])
def save_session():
    try:
        session_data = request.get_json()
        if not session_data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        session["wizard_data"] = session_data
        session.modified = True

        return jsonify({"status": "success"}), 200
    except Exception as e:
        current_app.logger.error(f"Error saving session: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@upload_bp.route("/restore-session", methods=["GET"])
def restore_session():
    """Restore wizard session data from the server"""
    try:
        wizard_data = session.get('wizard_data')
        if wizard_data:
            return jsonify(json.loads(wizard_data)), 200
        return jsonify({"status": "error", "message": "No session found"}), 404
    except Exception as e:
        current_app.logger.error(f"Error restoring session: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

@upload_bp.route("/clear-session", methods=["POST"])
def clear_session():
    try:
        session.clear()
        return jsonify({"status": "success"}), 200
    except Exception as e:
        current_app.logger.error(f"Error clearing session: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500

# @views_bp.route("/upload_tasks")
# def upload_tasks():
#     return render_template("upload_tasks.html")


# @views_bp.route("/csv_upload")
# def csv_upload():
#     return render_template("csv_upload.html", version=1)


@upload_bp.route("/get_persisted_uploads", methods=["GET"])
def get_persisted_uploads():
    persisted_uploads = {}
    for key in REDIS_CLIENT.scan_iter("upload_*"):
        upload_id = key.decode("utf-8").split("_")[1]
        upload_info = json.loads(REDIS_CLIENT.get(key))
        persisted_uploads[upload_id] = upload_info

    return jsonify(persisted_uploads)
