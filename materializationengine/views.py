import datetime
import json
import logging
from typing import Any, Dict

import caveclient
import nglui
import numpy as np
import pandas as pd
from celery.result import AsyncResult
from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
    AnalysisView,
    MaterializedMetadata,
    VersionErrorTable,
)
from dynamicannotationdb.schema import DynamicSchemaClient
from flask import (
    Blueprint,
    abort,
    current_app,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from google.cloud import storage
from middle_auth_client import auth_required, auth_requires_permission
from nglui.statebuilder.helpers import make_point_statebuilder, package_state
from redis import StrictRedis
from sqlalchemy import and_, func, or_
from sqlalchemy.sql import text

from materializationengine.blueprints.client.datastack import validate_datastack
from materializationengine.blueprints.client.query_manager import QueryManager
from materializationengine.blueprints.client.schemas import AnalysisViewSchema
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.info_client import (
    get_datastack_info,
    get_datastacks,
    get_relevant_datastack_info,
)
from materializationengine.schema_helper import get_schema_types
from materializationengine.schemas import (
    AnalysisTableSchema,
    AnalysisVersionSchema,
    AnalysisViewSchema,
    VersionErrorTableSchema,
)
from materializationengine.utils import check_read_permission, get_config_param

__version__ = "4.35.0"

views_bp = Blueprint("views", __name__, url_prefix="/materialize/views")


def make_flat_model(db, table: AnalysisTable):
    anno_metadata = db.database.get_table_metadata(table.table_name)
    ref_table = anno_metadata.get("reference_table", None)
    if ref_table:
        table_metadata = {"reference_table": ref_table}
    else:
        table_metadata = None
    Model = db.schema.create_flat_model(
        table_name=table.table_name,
        schema_type=table.schema,
        table_metadata=table_metadata,
    )
    return Model, anno_metadata


@views_bp.before_request
@reset_auth
def before_request():
    pass


REDIS_CLIENT = StrictRedis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)


@views_bp.route("/generate-presigned-url", methods=["POST"])
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


@views_bp.route("/store-metadata", methods=["POST"])
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


@views_bp.route("/get-schema-model", methods=["GET"])
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


@views_bp.route("/get-schema-types", methods=["GET"])
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


@views_bp.route("/upload-complete", methods=["POST"])
def upload_complete():
    filename = request.json["filename"]
    # TODO maybe add some callback logic here
    return jsonify(
        {"status": "success", "message": f"{filename} uploaded successfully"}
    )

@views_bp.route("/upload")
def wizard():
    try:
        if "current_step" not in session:
            session["current_step"] = 0

        current_step = session["current_step"]

        return render_template(
            "/csv_upload/main.html",
            current_step=current_step,
            total_steps=5,
            version=current_app.config.get("VERSION", __version__),
        )
    except Exception as e:
        current_app.logger.error(f"Error rendering wizard: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


@views_bp.route("/update-step", methods=["POST"])
def update_step():
    """Update wizard step in the session"""
    data = request.json
    session["current_step"] = data.get("current_step", session.get("current_step", 0))
    return jsonify({"status": "success", "current_step": session["current_step"]})


@views_bp.route("/step/<int:step_number>", methods=["GET"])
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


@views_bp.route("/save-session", methods=["POST"])
def save_session():
    try:
        session_data = request.get_json()
        if not session_data:
            return jsonify({"status": "error", "message": "No data provided"}), 400

        session["wizard_data"] = session_data

        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@views_bp.route("/databases", methods=["GET"])
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


@views_bp.route("/restore-session", methods=["GET"])
def restore_session():
    """Restore wizard session data from the server"""
    try:
        session_key = request.cookies.get("wizard_session")
        saved_data = REDIS_CLIENT.get(f"wizard_{session_key}")
        if saved_data:
            return jsonify(json.loads(saved_data)), 200
        return jsonify({"status": "error", "message": "No session found"}), 404
    except Exception as e:
        current_app.logger.error(f"Error restoring session: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


# @views_bp.route("/upload_tasks")
# def upload_tasks():
#     return render_template("upload_tasks.html")


# @views_bp.route("/csv_upload")
# def csv_upload():
#     return render_template("csv_upload.html", version=1)


@views_bp.route("/get_persisted_uploads", methods=["GET"])
def get_persisted_uploads():
    persisted_uploads = {}
    for key in REDIS_CLIENT.scan_iter("upload_*"):
        upload_id = key.decode("utf-8").split("_")[1]
        upload_info = json.loads(REDIS_CLIENT.get(key))
        persisted_uploads[upload_id] = upload_info

    return jsonify(persisted_uploads)


@views_bp.route("/")
@views_bp.route("/index")
@auth_required
def index():
    datastacks = get_datastacks()
    datastack_payload = []
    for datastack in datastacks:
        datastack_data = {"name": datastack}
        try:
            datastack_info = get_datastack_info(datastack)
        except Exception as e:
            logging.warning(e)
            datastack_info = None
        aligned_volume_info = datastack_info.get("aligned_volume")

        if aligned_volume_info:
            datastack_data["description"] = aligned_volume_info.get("description")

        datastack_payload.append(datastack_data)
    return render_template(
        "datastacks.html", datastacks=datastack_payload, version=__version__
    )


@views_bp.route("/cronjobs")
@auth_required
def jobs():
    return render_template("jobs.html", jobs=get_jobs(), version=__version__)


def get_jobs():
    return celery.conf.beat_schedule


@views_bp.route("/cronjobs/<job_name>")
@auth_required
def get_job_info(job_name: str):
    job = celery.conf.beat_schedule[job_name]
    c = job["schedule"]
    now = datetime.datetime.utcnow()
    due = c.is_due(now)

    seconds = now.timestamp()

    next_time_to_run = datetime.datetime.fromtimestamp(seconds + due.next).strftime(
        "%A, %B %d, %Y %I:%M:%S"
    )

    job_info = {
        "cron_schema": c,
        "task": job["task"],
        "kwargs": job["kwargs"],
        "next_time_to_run": next_time_to_run,
    }
    return render_template("job.html", job=job_info, version=__version__)


def make_df_with_links_to_version(
    objects, schema, url, col, col_value, df=None, **urlkwargs
):
    if df is None:
        df = pd.DataFrame(data=schema.dump(objects, many=True))
    if urlkwargs is None:
        urlkwargs = {}
    if url is not None:
        df[col] = df.apply(
            lambda x: "<a href='{}'>{}</a>".format(
                url_for(url, version=getattr(x, col_value), **urlkwargs), x[col]
            ),
            axis=1,
        )
    return df


def make_df_with_links_to_id(
    objects, schema, url, col, col_value, df=None, **urlkwargs
):
    if df is None:
        df = pd.DataFrame(data=schema.dump(objects, many=True))
    if urlkwargs is None:
        urlkwargs = {}
    if url is not None:
        df[col] = df.apply(
            lambda x: "<a href='{}'>{}</a>".format(
                url_for(url, id=getattr(x, col_value), **urlkwargs), x[col]
            ),
            axis=1,
        )
    return df


@views_bp.route("/datastack/<datastack_name>")
@auth_requires_permission("view", table_arg="datastack_name")
def datastack_view(datastack_name):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)

    version_query = session.query(AnalysisVersion).filter(
        AnalysisVersion.datastack == datastack_name
    )
    show_all = request.args.get("all", False) is not False
    if not show_all:
        version_query = version_query.filter(AnalysisVersion.valid == True)
    versions = version_query.order_by(AnalysisVersion.version.desc()).all()

    if len(versions) > 0:
        schema = AnalysisVersionSchema(many=True)
        column_order = schema.declared_fields.keys()
        df = pd.DataFrame(data=schema.dump(versions, many=True))

        df = make_df_with_links_to_id(
            objects=versions,
            schema=schema,
            url="views.version_error",
            col="status",
            col_value="version",
            df=df,
            datastack_name=datastack_name,
        )
        df = make_df_with_links_to_version(
            objects=versions,
            schema=schema,
            url="views.version_view",
            col="version",
            col_value="version",
            df=df,
            datastack_name=datastack_name,
        )
        df = df.reindex(columns=column_order)

        classes = ["table table-borderless"]
        with pd.option_context("display.max_colwidth", -1):
            output_html = df.to_html(
                escape=False, classes=classes, index=False, justify="left", border=0
            )
    else:
        output_html = ""

    return render_template(
        "datastack.html",
        datastack=datastack_name,
        table=output_html,
        version=__version__,
    )


@views_bp.route("/datastack/<datastack_name>/version/<int(signed=True):id>/failed")
@auth_requires_permission("view", table_arg="datastack_name")
def version_error(datastack_name: str, id: int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

    session = sqlalchemy_cache.get(aligned_volume_name)

    version = (
        session.query(AnalysisVersion).filter(AnalysisVersion.version == id).first()
    )
    error = (
        session.query(VersionErrorTable)
        .filter(VersionErrorTable.analysisversion_id == version.id)
        .first()
    )
    if error:
        schema = VersionErrorTableSchema()
        error_data = schema.dump(error)
        return render_template(
            "version_error.html",
            traceback=json.dumps(error_data["error"]),
            error_type=error_data["exception"],
            datastack=datastack_name,
            version=__version__,
            mat_version=version.version,
        )

    else:
        return render_template(
            "version_error.html",
            error_type=None,
            datastack=datastack_name,
            version=__version__,
            mat_version=version.version,
        )


def make_seg_prop_ng_link(datastack_name, table_name, version, client, is_view=False):
    seg_layer = client.info.segmentation_source(format_for="neuroglancer")
    seg_layer.replace("graphene://https://", "graphene://middleauth+https://")
    if is_view:
        seginfo_url = url_for(
            "api.Materialization Client2_mat_view_segment_info",
            datastack_name=datastack_name,
            version=version,
            view_name=table_name,
            _external=True,
        )
    else:
        seginfo_url = url_for(
            "api.Materialization Client2_mat_table_segment_info",
            datastack_name=datastack_name,
            version=version,
            table_name=table_name,
            _external=True,
        )

    seg_info_source = f"precomputed://middleauth+{seginfo_url}".format(
        seginfo_url=seginfo_url
    )
    # strip off the /info
    seg_info_source = seg_info_source[:-5]

    seg_layer = nglui.statebuilder.SegmentationLayerConfig(
        source=[seg_layer, seg_info_source], name="seg"
    )
    img_layer = nglui.statebuilder.ImageLayerConfig(
        source=client.info.image_source(), name="img"
    )
    sb = nglui.statebuilder.StateBuilder([img_layer, seg_layer], client=client)
    url_link = sb.render_state(
        None,
        return_as="url",
        url_prefix="https://spelunker.cave-explorer.org",
        target_site="mainline",
    )
    return url_link


@views_bp.route("/datastack/<datastack_name>/version/<int(signed=True):version>")
@validate_datastack
@auth_requires_permission("view", table_arg="datastack_name")
def version_view(
    datastack_name: str, version: int, target_datastack=None, target_version=None
):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

    session = sqlalchemy_cache.get(aligned_volume_name)

    anal_version = (
        session.query(AnalysisVersion)
        .filter(AnalysisVersion.version == target_version)
        .filter(AnalysisVersion.datastack == target_datastack)
        .first()
    )

    table_query = session.query(AnalysisTable).filter(
        AnalysisTable.analysisversion == anal_version
    )
    tables = table_query.all()
    schema = AnalysisTableSchema(many=True)

    df = make_df_with_links_to_id(
        objects=tables,
        schema=AnalysisTableSchema(many=True),
        url="views.table_view",
        col="id",
        col_value="id",
        datastack_name=target_datastack,
    )

    column_order = schema.declared_fields.keys()
    schema_url = "<a href='{}/schema/views/type/{}/view'>{}</a>"
    client = caveclient.CAVEclient(
        datastack_name, server_address=current_app.config["GLOBAL_SERVER_URL"]
    )
    df["ng_link"] = df.apply(
        lambda x: f"<a href='{make_seg_prop_ng_link(target_datastack, x.table_name, target_version, client)}'>seg prop link</a>",
        axis=1,
    )
    df["schema"] = df.schema.map(
        lambda x: schema_url.format(current_app.config["GLOBAL_SERVER_URL"], x, x)
    )
    df["table_name"] = df.table_name.map(
        lambda x: f"<a href='/annotation/views/aligned_volume/{aligned_volume_name}/table/{x}'>{x}</a>"
    )

    df = df.reindex(columns=list(column_order) + ["ng_link"])

    classes = ["table table-borderless"]
    with pd.option_context("display.max_colwidth", -1):
        output_html = df.to_html(
            escape=False, classes=classes, index=False, justify="left", border=0
        )

    mat_session = sqlalchemy_cache.get(f"{datastack_name}__mat{version}")

    views = mat_session.query(AnalysisView).all()

    views_df = make_df_with_links_to_id(
        objects=views,
        schema=AnalysisViewSchema(many=True),
        url=None,
        col=None,
        col_value=None,
        datastack_name=target_datastack,
    )
    if len(views_df) > 0:
        views_df["ng_link"] = views_df.apply(
            lambda x: f"<a href='{make_seg_prop_ng_link(target_datastack, x.table_name, target_version, client, is_view=True)}'>seg prop link</a>",
            axis=1,
        )
        classes = ["table table-borderless"]
        with pd.option_context("display.max_colwidth", -1):
            output_view_html = views_df.to_html(
                escape=False, classes=classes, index=False, justify="left", border=0
            )
    else:
        output_view_html = "<h4> No views in datastack </h4>"

    return render_template(
        "version.html",
        datastack=target_datastack,
        analysisversion=target_version,
        table=output_html,
        view_table=output_view_html,
        version=__version__,
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>")
@auth_requires_permission("view", table_arg="datastack_name")
def table_view(datastack_name, id: int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    check_read_permission(db, table.table_name)
    # mapping = {
    #     "synapse": url_for(
    #         "views.synapse_report", id=id, datastack_name=datastack_name
    #     ),
    #     "cell_type_local": url_for(
    #         "views.cell_type_local_report", id=id, datastack_name=datastack_name
    #     ),
    # }
    # if table.schema in mapping:
    #     return redirect(mapping[table.schema])
    # else:
    return redirect(
        url_for("views.generic_report", datastack_name=datastack_name, id=id)
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/cell_type_local")
@auth_requires_permission("view", table_arg="datastack_name")
def cell_type_local_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    db = dynamic_annotation_cache.get_db(aligned_volume_name)

    if not table:
        abort(404, "Table not found")
    if table.schema != "cell_type_local":
        abort(504, "this table is not a cell_type_local table")
    check_read_permission(db, table.table_name)

    Model, anno_metadata = make_flat_model(db, table)
    mat_db_name = f"{datastack_name}__mat{table.analysisversion.version}"
    matsession = sqlalchemy_cache.get(mat_db_name)

    n_annotations = (
        matsession.query(MaterializedMetadata)
        .filter(MaterializedMetadata.table_name == table.table_name)
        .first()
        .row_count
    )

    # AnnoCellTypeModel, SegCellTypeModel = schema_client.get_split_models(
    #     table.table_name, table.schema, pcg_table_name
    # )
    cell_type_merge_query = (
        matsession.query(
            Model.cell_type,
            func.count(Model.cell_type).label("num_cells"),
        )
        .group_by(Model.cell_type)
        .order_by(text("num_cells DESC"))
    ).limit(100)

    df = pd.read_sql(
        cell_type_merge_query.statement,
        sqlalchemy_cache.get_engine(mat_db_name),
        coerce_float=False,
    )
    classes = ["table table-borderless"]

    return render_template(
        "cell_type_local.html",
        version=__version__,
        schema_name=table.schema,
        n_annotations=n_annotations,
        table_name=table.table_name,
        dataset=table.analysisversion.datastack,
        table=df.to_html(
            escape=False, classes=classes, index=False, justify="left", border=0
        ),
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/synapse")
@auth_requires_permission("view", table_arg="datastack_name")
def synapse_report(datastack_name, id):
    synapse_task = get_synapse_info.s(datastack_name, id)
    task = synapse_task.apply_async()
    return redirect(
        url_for(
            "views.check_if_complete",
            datastack_name=datastack_name,
            id=id,
            task_id=str(task.id),
        )
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/synapse/<task_id>/loading")
@auth_requires_permission("view", table_arg="datastack_name")
def check_if_complete(datastack_name, id, task_id):
    res = AsyncResult(task_id, app=celery)
    if res.ready() is True:
        synapse_data = res.result

        return render_template(
            "synapses.html",
            synapses=synapse_data["synapses"],
            num_autapses=synapse_data["n_autapses"],
            num_no_root=synapse_data["n_no_root"],
            dataset=datastack_name,
            analysisversion=synapse_data["version"],
            version=__version__,
            table_name=synapse_data["table_name"],
            schema_name="synapse",
        )
    else:
        return render_template("loading.html", version=__version__)


@celery.task(name="process:get_synapse_info", acks_late=True, bind=True)
def get_synapse_info(self, datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    if table.schema != "synapse":
        abort(504, "this table is not a synapse table")
    schema_client = DynamicSchemaClient()

    AnnoSynapseModel, SegSynapseModel = schema_client.get_split_models(
        table.table_name, table.schema, pcg_table_name
    )

    synapses = session.query(AnnoSynapseModel).count()

    n_autapses = (
        session.query(AnnoSynapseModel)
        .filter(SegSynapseModel.pre_pt_root_id == SegSynapseModel.post_pt_root_id)
        .filter(
            and_(
                SegSynapseModel.pre_pt_root_id != 0,
                SegSynapseModel.post_pt_root_id != 0,
            )
        )
        .count()
    )
    n_no_root = (
        session.query(AnnoSynapseModel)
        .filter(
            or_(
                SegSynapseModel.pre_pt_root_id == 0,
                SegSynapseModel.post_pt_root_id == 0,
            )
        )
        .count()
    )

    return {
        "table": table,
        "synapses": synapses,
        "n_autapses": n_autapses,
        "n_no_root": n_no_root,
        "version": table.analysisversion.version,
    }


@views_bp.route(
    "/datastack/<datastack_name>/table/<int:id>/generic", methods=("GET", "POST")
)
@auth_requires_permission("view", table_arg="datastack_name")
def generic_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    if table is None:
        abort(404, "this table does not exist")
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    check_read_permission(db, table.table_name)

    parent_version_id = table.analysisversion.parent_version
    if parent_version_id is not None:
        parent_version = session.query(AnalysisVersion).get(parent_version_id)
        target_version = datastack_name
        datastack_name = parent_version.datastack
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )

    mat_db_name = f"{datastack_name}__mat{table.analysisversion.version}"
    anno_metadata = db.database.get_table_metadata(table.table_name)

    qm = QueryManager(
        mat_db_name,
        segmentation_source=pcg_table_name,
        meta_db_name=aligned_volume_name,
        split_mode=not table.analysisversion.is_merged,
    )

    matsession = sqlalchemy_cache.get(mat_db_name)

    n_annotations = (
        matsession.query(MaterializedMetadata)
        .filter(MaterializedMetadata.table_name == table.table_name)
        .first()
        .row_count
    )
    qm.add_table(table.table_name)
    qm.select_all_columns(table.table_name)
    df, column_names = qm.execute_query()

    if request.method == "POST":
        pos_column = request.form["position"]
        grp_column = request.form["group"]
        if not grp_column:
            grp_column = None

        linked_cols = request.form.get("linked", None)
        print(pos_column, grp_column, linked_cols)
        data_res = [
            anno_metadata["voxel_resolution_x"],
            anno_metadata["voxel_resolution_y"],
            anno_metadata["voxel_resolution_z"],
        ]
        client = caveclient.CAVEclient(
            datastack_name, server_address=current_app.config["GLOBAL_SERVER_URL"]
        )
        sb = make_point_statebuilder(
            client,
            point_column=pos_column,
            group_column=grp_column,
            linked_seg_column=linked_cols,
            data_resolution=data_res,
        )
        url = package_state(
            df,
            sb,
            client,
            shorten="always",
            return_as="url",
            ngl_url=None,
            link_text="link",
        )

        return redirect(url)

    classes = ["table table-borderless"]
    with pd.option_context("display.max_colwidth", -1):
        output_html = df.to_html(
            escape=False,
            classes=classes,
            index=False,
            justify="left",
            border=0,
            table_id="datatable",
        )

    root_columns = [c for c in df.columns if c.endswith("_root_id")]
    pt_columns = [c for c in df.columns if c.endswith("_position")]
    sv_id_columns = [c for c in df.columns if c.endswith("_supervoxel_id")]
    id_valid = ["id", "valid"]
    all_system_cols = np.concatenate(
        [root_columns, pt_columns, sv_id_columns, id_valid]
    )
    other_columns = df.columns[~df.columns.isin(all_system_cols)]
    return render_template(
        "generic.html",
        pt_columns=pt_columns,
        root_columns=root_columns,
        other_columns=other_columns,
        dataset=datastack_name,
        analysisversion=table.analysisversion.version,
        version=__version__,
        table_name=table.table_name,
        schema_name=table.schema,
        n_annotations=n_annotations,
        anno_metadata=anno_metadata,
        table=output_html,
    )
