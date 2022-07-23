import datetime

import pandas as pd
from dateutil import parser
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion
from dynamicannotationdb.schema import DynamicSchemaClient
from flask import (
    Blueprint,
    abort,
    current_app,
    redirect,
    render_template,
    request,
    url_for,
)
from middle_auth_client import (
    auth_required,
    auth_requires_admin,
    auth_requires_permission,
)
from sqlalchemy import and_, func, or_

from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.celery_init import celery
from materializationengine.database import sqlalchemy_cache
from materializationengine.info_client import get_datastack_info, get_datastacks
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema

__version__ = "4.0.21"

views_bp = Blueprint("views", __name__, url_prefix="/materialize/views")


@views_bp.before_request
@reset_auth
def before_request():
    pass


@views_bp.route("/")
@views_bp.route("/index")
@auth_required
def index():
    return render_template(
        "datastacks.html", datastacks=get_datastacks(), version=__version__
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


def make_df_with_links_to_id(objects, schema, url, col, **urlkwargs):
    df = pd.DataFrame(data=schema.dump(objects, many=True))
    if urlkwargs is None:
        urlkwargs = {}
    df[col] = df.apply(
        lambda x: "<a href='{}'>{}</a>".format(
            url_for(url, id=x.id, **urlkwargs), x[col]
        ),
        axis=1,
    )
    return df


def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info["segmentation_source"]
    pcg_table_name = seg_source.split("/")[-1]
    aligned_volume_name = ds_info["aligned_volume"]["name"]
    return aligned_volume_name, pcg_table_name


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
        df = make_df_with_links_to_id(
            versions,
            schema,
            "views.version_view",
            "version",
            datastack_name=datastack_name,
        )
        df_html_table = df.to_html(escape=False)
    else:
        df_html_table = ""

    return render_template(
        "datastack.html",
        datastack=datastack_name,
        table=df_html_table,
        version=__version__,
    )


@views_bp.route("/datastack/<datastack_name>/version/<int:id>")
@auth_requires_permission("view", table_arg="datastack_name")
def version_view(datastack_name: str, id: int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)

    version = session.query(AnalysisVersion).filter(AnalysisVersion.id == id).first()

    table_query = session.query(AnalysisTable).filter(
        AnalysisTable.analysisversion == version
    )
    tables = table_query.all()

    df = make_df_with_links_to_id(
        tables,
        AnalysisTableSchema(many=True),
        "views.table_view",
        "id",
        datastack_name=datastack_name,
    )
    schema_url = "<a href='{}/schema/views/type/{}/view'>{}</a>"
    df["schema"] = df.schema.map(
        lambda x: schema_url.format(current_app.config["GLOBAL_SERVER_URL"], x, x)
    )
    df["table_name"] = df.table_name.map(
        lambda x: f"<a href='/annotation/views/aligned_volume/{aligned_volume_name}/table/{x}'>{x}</a>"
    )

    with pd.option_context("display.max_colwidth", -1):
        output_html = df.to_html(escape=False)

    return render_template(
        "version.html",
        datastack=version.datastack,
        analysisversion=version.version,
        table=output_html,
        version=__version__,
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>")
@auth_requires_permission("view", table_arg="datastack_name")
def table_view(datastack_name, id: int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    mapping = {
        "synapse": url_for(
            "views.synapse_report", id=id, datastack_name=datastack_name
        ),
        "cell_type_local": url_for(
            "views.cell_type_local_report", id=id, datastack_name=datastack_name
        ),
    }
    if table.schema in mapping:
        return redirect(mapping[table.schema])
    else:
        return redirect(
            url_for("views.generic_report", datastack_name=datastack_name, id=id)
        )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/cell_type_local")
@auth_requires_permission("view", table_arg="datastack_name")
def cell_type_local_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    if table.schema != "cell_type_local":
        abort(504, "this table is not a cell_type_local table")
    schema_client = DynamicSchemaClient()

    CellTypeModel = schema_client.create_annotation_model(
        table.tablename,
        table.schema,
    )

    n_annotations = CellTypeModel.query.count()

    cell_type_merge_query = (
        session.query(
            CellTypeModel.pt_root_id,
            CellTypeModel.cell_type,
            func.count(CellTypeModel.pt_root_id).label("num_cells"),
        )
        .group_by(CellTypeModel.pt_root_id, CellTypeModel.cell_type)
        .order_by("num_cells DESC")
    ).limit(100)

    df = pd.read_sql(
        cell_type_merge_query.statement,
        sqlalchemy_cache.get_engine(aligned_volume_name),
        coerce_float=False,
    )
    return render_template(
        "cell_type_local.html",
        version=__version__,
        schema_name=table.schema,
        table_name=table.tablename,
        dataset=table.analysisversion.dataset,
        table=df.to_html(),
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/synapse")
@auth_requires_permission("view", table_arg="datastack_name")
def synapse_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    if table.schema != "synapse":
        abort(504, "this table is not a synapse table")
    schema_client = DynamicSchemaClient()
    SynapseModel = schema_client.create_annotation_model(
        table.tablename,
        table.schema,
    )

    synapses = SynapseModel.query.count()
    n_autapses = (
        SynapseModel.query.filter(
            SynapseModel.pre_pt_root_id == SynapseModel.post_pt_root_id
        )
        .filter(
            and_(SynapseModel.pre_pt_root_id != 0, SynapseModel.post_pt_root_id != 0)
        )
        .count()
    )
    n_no_root = SynapseModel.query.filter(
        or_(SynapseModel.pre_pt_root_id == 0, SynapseModel.post_pt_root_id == 0)
    ).count()

    return render_template(
        "synapses.html",
        num_synapses=synapses,
        num_autapses=n_autapses,
        num_no_root=n_no_root,
        dataset=table.analysisversion.dataset,
        analysisversion=table.analysisversion.version,
        version=__version__,
        table_name=table.tablename,
        schema_name="synapses",
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/generic")
@auth_requires_permission("view", table_arg="datastack_name")
def generic_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    schema_client = DynamicSchemaClient()
    Model = schema_client.create_annotation_model(
        table.tablename,
        table.schema,
    )

    n_annotations = Model.query.count()

    return render_template(
        "generic.html",
        n_annotations=n_annotations,
        dataset=table.analysisversion.dataset,
        analysisversion=table.analysisversion.version,
        version=__version__,
        table_name=table.tablename,
        schema_name=table.schema,
    )
