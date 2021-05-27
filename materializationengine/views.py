import datetime

import pandas as pd
from dateutil import parser
from emannotationschemas.models import (make_annotation_model,
                                        make_dataset_models)
from flask import Blueprint, abort, redirect, render_template, request, url_for, current_app
from sqlalchemy import and_, func, or_

from materializationengine.celery_init import celery
from materializationengine.database import sqlalchemy_cache
from materializationengine.info_client import (get_datastack_info,
                                               get_datastacks)
from materializationengine.models import AnalysisTable, AnalysisVersion
from materializationengine.schemas import (AnalysisTableSchema,
                                           AnalysisVersionSchema)

__version__ = "1.4.2"

views_bp = Blueprint("views", __name__, url_prefix="/materialize/views")


@views_bp.route("/")
@views_bp.route("/index")
def index():
    return render_template("datastacks.html",
                           datastacks=get_datastacks(),
                           version=__version__)

@views_bp.route("/cronjobs")
def jobs():
    return render_template("jobs.html",
                           jobs=get_jobs(),
                           version=__version__)

def get_jobs():
    return celery.conf.beat_schedule


@views_bp.route("/cronjobs/<job_name>")
def get_job_info(job_name: str):
    job = celery.conf.beat_schedule[job_name]
    c = job['schedule']        
    now = datetime.datetime.utcnow()
    due = c.is_due(now)

    seconds = now.timestamp()

    next_time_to_run = datetime.datetime.fromtimestamp(
        seconds + due.next).strftime("%A, %B %d, %Y %I:%M:%S")

    job_info ={
        'cron_schema': c,
        'task': job['task'],
        'kwargs': job['kwargs'],
        'next_time_to_run': next_time_to_run
    }
    return render_template(
        "job.html", job=job_info, version=__version__)


def make_df_with_links_to_id(objects, schema, url, col, **urlkwargs):
    df = pd.DataFrame(data=schema.dump(objects, many=True))
    if urlkwargs is None:
        urlkwargs={}
    df[col] = df.apply(
        lambda x: "<a href='{}'>{}</a>".format(url_for(url, id=x.id, **urlkwargs), x[col]), axis=1
    )
    return df

def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info['segmentation_source']
    pcg_table_name = seg_source.split('/')[-1]
    aligned_volume_name = ds_info['aligned_volume']['name']
    return aligned_volume_name, pcg_table_name

@views_bp.route("/datastack/<datastack_name>")
def datastack_view(datastack_name):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)

    version_query = session.query(AnalysisVersion)\
        .filter(AnalysisVersion.datastack == datastack_name)
    show_all = request.args.get("all", False) is not False
    if not show_all:
        version_query = version_query.filter(AnalysisVersion.valid == True)
    versions = version_query.order_by(AnalysisVersion.version.desc()).all()

    if len(versions) > 0:
        schema = AnalysisVersionSchema(many=True)
        df = make_df_with_links_to_id(versions, schema, "views.version_view", "version",
                                      datastack_name=datastack_name)
        df_html_table = df.to_html(escape=False)
    else:
        df_html_table = ""

    return render_template(
        "datastack.html", datastack=datastack_name, table=df_html_table, version=__version__
    )


@views_bp.route("/datastack/<datastack_name>/version/<int:id>")
def version_view(datastack_name:str, id:int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)

    version = session.query(AnalysisVersion).filter(AnalysisVersion.id == id).first()
    
    table_query = session.query(AnalysisTable).filter(AnalysisTable.analysisversion == version)
    tables = table_query.all()

    df = make_df_with_links_to_id(
        tables, AnalysisTableSchema(many=True), "views.table_view", "id",
        datastack_name=datastack_name
    )
    schema_url = "<a href='{}/schema/views/type/{}/view'>{}</a>"
    df["schema"] = df.schema.map(lambda x: schema_url.format(current_app.config['GLOBAL_SERVER'],x, x))
    df["table_name"] = df.table_name.map(lambda x: "<a href='/annotation/views/aligned_volume/{}/table/{}'>{}</a>".format(aligned_volume_name, x, x))
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
def table_view(datastack_name, id:int):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    mapping = {
        "synapse": url_for("views.synapse_report",
                            id=id, datastack_name=datastack_name),
        "cell_type_local": url_for("views.cell_type_local_report",
                            id=id, datastack_name=datastack_name),
    }
    if table.schema in mapping:
        return redirect(mapping[table.schema])
    else:
        return redirect(url_for("views.generic_report",
                        datastack_name=datastack_name, id=id))


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/cell_type_local")
def cell_type_local_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = AnalysisTable.query.filter(AnalysisTable.id == id).first_or_404()
    if table.schema != "cell_type_local":
        abort(504, "this table is not a cell_type_local table")

    make_dataset_models(table.analysisversion.dataset, [], version=table.analysisversion.version)
    CellTypeModel = make_annotation_model(
        table.analysisversion.dataset,
        table.schema,
        table.tablename,
        version=table.analysisversion.version,
    )

    n_annotations = CellTypeModel.query.count()

    cell_type_merge_query = (
        db.session.query(
            CellTypeModel.pt_root_id,
            CellTypeModel.cell_type,
            func.count(CellTypeModel.pt_root_id).label("num_cells"),
        )
        .group_by(CellTypeModel.pt_root_id, CellTypeModel.cell_type)
        .order_by("num_cells DESC")
    ).limit(100)

    df = pd.read_sql(cell_type_merge_query.statement, db.get_engine(), coerce_float=False)
    return render_template(
        "cell_type_local.html",
        version=__version__,
        schema_name=table.schema,
        table_name=table.tablename,
        dataset=table.analysisversion.dataset,
        table=df.to_html(),
    )


@views_bp.route("/datastack/<datastack_name>/table/<int:id>/synapse")
def synapse_report(datastack_name, id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()
    if table.schema != "synapse":
        abort(504, "this table is not a synapse table")
    
    make_dataset_models(table.analysisversion.datastack, [], version=table.analysisversion.version)

    SynapseModel = make_annotation_model(
        table.analysisversion.dataset,
        "synapse",
        table.tablename,
        version=table.analysisversion.version,
    )
    synapses = SynapseModel.query.count()
    n_autapses = (
        SynapseModel.query.filter(SynapseModel.pre_pt_root_id == SynapseModel.post_pt_root_id)
        .filter(and_(SynapseModel.pre_pt_root_id != 0, SynapseModel.post_pt_root_id != 0))
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
def generic_report(id):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    table = session.query(AnalysisTable).filter(AnalysisTable.id == id).first()

    make_dataset_models(table.analysisversion.dataset, [],
                        version=table.analysisversion.version)

    Model = make_annotation_model(
        table.analysisversion.dataset,
        table.schema,
        table.tablename,
        version=table.analysisversion.version,
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

