import datetime
import grp
import json
import logging
from struct import pack

import pandas as pd
import numpy as np
from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
    VersionErrorTable,
    AnnoMetadata,
    MaterializedMetadata,
)
from dynamicannotationdb.schema import DynamicSchemaClient
from flask import (
    Blueprint,
    abort,
    redirect,
    render_template,
    request,
    url_for,
    current_app,
)
from middle_auth_client import auth_required, auth_requires_permission
from sqlalchemy import and_, func, or_
from sqlalchemy.sql import text
from materializationengine.celery_init import celery
from celery.result import AsyncResult
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.celery_init import celery
from materializationengine.blueprints.client.query import specific_query
from materializationengine.database import sqlalchemy_cache, dynamic_annotation_cache
from materializationengine.blueprints.client.query_manager import QueryManager

from materializationengine.info_client import (
    get_datastack_info,
    get_datastacks,
    get_relevant_datastack_info,
)
from materializationengine.schemas import (
    AnalysisTableSchema,
    AnalysisVersionSchema,
    VersionErrorTableSchema,
)
from materializationengine.utils import check_read_permission
from nglui.statebuilder import from_client
from nglui.statebuilder.helpers import package_state, make_point_statebuilder

import caveclient


__version__ = "4.15.4"

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


def make_df_with_links_to_id(
    objects, schema, url, col, col_value, df=None, **urlkwargs
):
    if df is None:
        df = pd.DataFrame(data=schema.dump(objects, many=True))
    if urlkwargs is None:
        urlkwargs = {}
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
        df = make_df_with_links_to_id(
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


@views_bp.route("/datastack/<datastack_name>/version/<int:id>/failed")
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


@views_bp.route("/datastack/<datastack_name>/version/<int:id>")
@auth_requires_permission("view", table_arg="datastack_name")
def version_view(datastack_name: str, id: int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

    session = sqlalchemy_cache.get(aligned_volume_name)

    version = (
        session.query(AnalysisVersion)
        .filter(AnalysisVersion.version == id)
        .filter(AnalysisVersion.datastack == datastack_name)
        .first()
    )

    table_query = session.query(AnalysisTable).filter(
        AnalysisTable.analysisversion == version
    )
    tables = table_query.all()
    schema = AnalysisTableSchema(many=True)

    df = make_df_with_links_to_id(
        objects=tables,
        schema=AnalysisTableSchema(many=True),
        url="views.table_view",
        col="id",
        col_value="id",
        datastack_name=datastack_name,
    )

    column_order = schema.declared_fields.keys()
    schema_url = "<a href='{}/schema/views/type/{}/view'>{}</a>"
    df["schema"] = df.schema.map(
        lambda x: schema_url.format(current_app.config["GLOBAL_SERVER_URL"], x, x)
    )
    df["table_name"] = df.table_name.map(
        lambda x: f"<a href='/annotation/views/aligned_volume/{aligned_volume_name}/table/{x}'>{x}</a>"
    )
    df = df.reindex(columns=column_order)

    logging.info(version)

    classes = ["table table-borderless"]
    with pd.option_context("display.max_colwidth", -1):
        output_html = df.to_html(
            escape=False, classes=classes, index=False, justify="left", border=0
        )

    return render_template(
        "version.html",
        datastack=datastack_name,
        analysisversion=version,
        table=output_html,
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
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

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
