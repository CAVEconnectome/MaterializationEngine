from dynamicannotationdb.models import AnalysisVersion, AnalysisTable
from cachetools import LRUCache, cached
from flask import abort, current_app
from materializationengine.blueprints.client.query_manager import QueryManager
from materializationengine.blueprints.client.utils import (
    update_notice_text_warnings,
    create_query_response,
    collect_crud_columns,
)
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.models import MaterializedMetadata
from materializationengine.utils import check_read_permission
from materializationengine.info_client import (
    get_relevant_datastack_info,
)
import numpy as np
import textwrap
from flask import g, request, jsonify
import traceback


def unhandled_exception(e):
    status_code = 500
    user_ip = str(request.remote_addr)
    tb = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)

    current_app.logger.error(
        {
            "message": str(e),
            "user_id": user_ip,
            "user_ip": user_ip,
            "request_url": request.url,
            "request_data": request.data,
            "response_code": status_code,
            "traceback": tb,
        }
    )

    resp = {
        "code": status_code,
        "message": str(e),
        "traceback": tb,
    }

    return resp, status_code


@cached(cache=LRUCache(maxsize=64))
def get_analysis_version(datastack_name: str, version: int, Session):
    """query database for the analysis version

    Args:
        datastack_name (str): datastack name
        version (int): integer
        Session ([type]): sqlalchemy session

    Returns:
        AnalysisVersion: instances of AnalysisVersion
    """

    analysis_version = (
        Session.query(AnalysisVersion)
        .filter(AnalysisVersion.datastack == datastack_name)
        .filter(AnalysisVersion.version == version)
        .first()
    )
    if analysis_version is None:
        abort(404, f"Version {version} not found in datastack {datastack_name}")
    return analysis_version


@cached(cache=LRUCache(maxsize=64))
def get_analysis_version_and_tables(datastack_name: str, version: int, Session):
    """query database for the analysis version and table name

    Args:
        datastack_name (str): datastack name
        version (int): integer
        Session ([type]): sqlalchemy session

    Returns:
        AnalysisVersion, List[AnalysisTable]: tuple of instances of AnalysisVersion and AnalysisTable
    """

    analysis_version = get_analysis_version(
        datastack_name=datastack_name, version=version, Session=Session
    )
    if analysis_version is None:
        return None, None
    analysis_tables = (
        Session.query(AnalysisTable)
        .filter(AnalysisTable.analysisversion_id == analysis_version.id)
        .all()
    )
    if analysis_version is None:
        return analysis_version, None
    return analysis_version, analysis_tables


@cached(cache=LRUCache(maxsize=64))
def get_analysis_version_and_table(
    datastack_name: str, table_name: str, version: int, Session
):
    """query database for the analysis version and table name

    Args:
        datastack_name (str): datastack name
        table_name (str): table name
        version (int): integer
        Session ([type]): sqlalchemy session

    Returns:
        AnalysisVersion, AnalysisTable: tuple of instances of AnalysisVersion and AnalysisTable
    """

    analysis_version = get_analysis_version(
        datastack_name=datastack_name, version=version, Session=Session
    )
    if analysis_version is None:
        return None, None
    analysis_table = (
        Session.query(AnalysisTable)
        .filter(AnalysisTable.analysisversion_id == analysis_version.id)
        .filter(AnalysisTable.table_name == table_name)
        .first()
    )
    if analysis_version is None:
        return analysis_version, None
    return analysis_version, analysis_table


@cached(cache=LRUCache(maxsize=32))
def get_flat_model(datastack_name: str, table_name: str, version: int, Session):
    """get a flat model for a frozen table

    Args:
        datastack_name (str): datastack name
        table_name (str): table name
        version (int): version of table
        Session (Sqlalchemy session): session to connect to database

    Returns:
        sqlalchemy.Model: model of table
    """
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    analysis_version, analysis_table = get_analysis_version_and_table(
        datastack_name, table_name, version, Session
    )
    if analysis_table is None:
        abort(
            404,
            "Cannot find table {} in datastack {} at version {}".format(
                table_name, datastack_name, version
            ),
        )
    if not analysis_version.valid:
        abort(410, "This materialization version is not available")

    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    metadata = db.database.get_table_metadata(table_name)
    reference_table = metadata.get("reference_table")
    if reference_table:
        table_metadata = {"reference_table": reference_table}
    else:
        table_metadata = None
    return db.schema.create_flat_model(
        table_name=table_name,
        schema_type=analysis_table.schema,
        table_metadata=table_metadata,
    )


def validate_table_args(tables, datastack_name, version):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    session = sqlalchemy_cache.get(aligned_volume_name)
    for table in tables:
        analysis_version, analysis_table = get_analysis_version_and_table(
            datastack_name, table, version, session
        )
        if not (analysis_table and analysis_version):
            abort(
                404,
                f"analysis table {table} not found for version {version} in datastack {datastack_name}",
            )


def generate_simple_query_dataframe(
    datastack_name,
    version,
    table_name,
    target_datastack,
    target_version,
    args,
    data,
    convert_desired_resolution=False,
):
    validate_table_args([table_name], target_datastack, target_version)
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    check_read_permission(db, table_name)

    ann_md = db.database.get_table_metadata(table_name)

    Session = sqlalchemy_cache.get(aligned_volume_name)
    analysis_version, analysis_table = get_analysis_version_and_table(
        datastack_name, table_name, version, Session
    )

    max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

    limit = data.get("limit", max_limit)
    if limit > max_limit:
        limit = max_limit

    get_count = args.get("count", False)
    if get_count:
        limit = None

    mat_db_name = f"{datastack_name}__mat{version}"

    if convert_desired_resolution:
        if not data.get("desired_resolution", None):
            des_res = [
                ann_md["voxel_resolution_x"],
                ann_md["voxel_resolution_y"],
                ann_md["voxel_resolution_z"],
            ]
            data["desired_resolution"] = des_res
    else:
        data["desired_resolution"] = None

    random_sample = args.get("random_sample", None)
    if random_sample is not None:
        session = sqlalchemy_cache.get(mat_db_name)
        mat_row_count = (
            session.query(MaterializedMetadata.row_count)
            .filter(MaterializedMetadata.table_name == table_name)
            .scalar()
        )
        if random_sample >= mat_row_count:
            random_sample = None
        else:
            random_sample = (100.0 * random_sample) / mat_row_count

    qm = QueryManager(
        mat_db_name,
        segmentation_source=pcg_table_name,
        meta_db_name=aligned_volume_name,
        split_mode=not analysis_version.is_merged,
        limit=limit,
        offset=data.get("offset", 0),
        get_count=get_count,
        random_sample=random_sample,
    )
    qm.add_table(table_name, random_sample=True)
    qm.apply_filter(data.get("filter_in_dict", None), qm.apply_isin_filter)
    qm.apply_filter(data.get("filter_out_dict", None), qm.apply_notequal_filter)
    qm.apply_filter(data.get("filter_equal_dict", None), qm.apply_equal_filter)
    qm.apply_filter(data.get("filter_greater_dict", None), qm.apply_greater_filter)
    qm.apply_filter(data.get("filter_less_dict", None), qm.apply_less_filter)
    qm.apply_filter(data.get("filter_greater_equal_dict", None), qm.apply_greater_equal_filter)
    qm.apply_filter(data.get("filter_less_equal_dict", None), qm.apply_less_equal_filter)
    qm.apply_filter(data.get("filter_spatial_dict", None), qm.apply_spatial_filter)
    qm.apply_filter(data.get("filter_regex_dict", None), qm.apply_regex_filter)
    qm.apply_filter({table_name: {"valid": True}}, qm.apply_equal_filter)
    select_columns = data.get("select_columns", None)
    if select_columns:
        for column in select_columns:
            qm.select_column(table_name, column)
    else:
        qm.select_all_columns(table_name)

    df, column_names = qm.execute_query(desired_resolution=data["desired_resolution"])
    df.drop(columns=["deleted", "superceded"], inplace=True, errors="ignore")
    warnings = []
    current_app.logger.info("query: {}".format(data))
    current_app.logger.info("args: {}".format(args))
    user_id = str(g.auth_user["id"])
    current_app.logger.info(f"user_id: {user_id}")

    if len(df) == limit:
        warnings.append(f'201 - "Limited query to {limit} rows')
    warnings = update_notice_text_warnings(ann_md, warnings, table_name)

    return df, warnings, column_names


def handle_simple_query(
    datastack_name,
    version,
    table_name,
    target_datastack,
    target_version,
    args,
    data,
    convert_desired_resolution=False,
):
    df, warnings, column_names = generate_simple_query_dataframe(
        datastack_name,
        version,
        table_name,
        target_datastack,
        target_version,
        args,
        data,
        convert_desired_resolution=convert_desired_resolution,
    )
    return create_query_response(
        df,
        warnings=warnings,
        column_names=column_names,
        desired_resolution=data["desired_resolution"],
        return_pyarrow=args["return_pyarrow"],
        arrow_format=args["arrow_format"],
        ipc_compress=args["ipc_compress"],
    )


def generate_complex_query_dataframe(
    datastack_name,
    version,
    target_datastack,
    target_version,
    args,
    data,
    convert_desired_resolution=False,
):
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    db = dynamic_annotation_cache.get_db(aligned_volume_name)

    Session = sqlalchemy_cache.get(aligned_volume_name)

    validate_table_args(
        [t[0] for t in data["tables"]], target_datastack, target_version
    )
    warnings = []

    for table_desc in data["tables"]:
        table_name = table_desc[0]
        ann_md = check_read_permission(db, table_name)
        warnings = update_notice_text_warnings(ann_md, warnings, table_name)

    db_name = f"{datastack_name}__mat{version}"

    analysis_version = (
        Session.query(AnalysisVersion)
        .filter(AnalysisVersion.datastack == datastack_name)
        .filter(AnalysisVersion.version == version)
        .first()
    )
    if analysis_version is None:
        abort(404, f"Analysis version {version} not found")

    max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

    limit = data.get("limit", max_limit)
    if limit > max_limit:
        limit = max_limit

    suffixes = data.get("suffixes", None)

    if suffixes is not None:
        warn_text = textwrap.dedent(
            """\
            Suffixes is deprecated for complex queries as it
            can be ambiguous what you desire,
            please pass suffix_map as a dictionary to explicitly
            set suffixes for individual tables.
            Upgrade caveclient to >=5.0.0 """
        )
        warnings.append(warn_text)
        all_tables = []
        for table_desc in data["tables"]:
            all_tables.append(table_desc[0])
        u, ind = np.unique(all_tables, return_index=True)
        uniq_tables = u[np.argsort(ind)]
        suffixes = {t: s for t, s in zip(uniq_tables, suffixes)}
    else:
        suffixes = data.get("suffix_map")

    random_sample = args.get("random_sample", None)
    if random_sample is not None:
        session = sqlalchemy_cache.get(db_name)
        mat_row_count = (
            session.query(MaterializedMetadata.row_count)
            .filter(MaterializedMetadata.table_name == data["tables"][0][0])
            .scalar()
        )
        if random_sample >= mat_row_count:
            random_sample = None
        else:
            random_sample = (100.0 * random_sample) / mat_row_count

    qm = QueryManager(
        db_name,
        segmentation_source=pcg_table_name,
        meta_db_name=aligned_volume_name,
        split_mode=not analysis_version.is_merged,
        suffixes=suffixes,
        limit=limit,
        offset=data.get("offset", 0),
        get_count=False,
        random_sample=random_sample,
    )
    if convert_desired_resolution:
        if not data.get("desired_resolution", None):
            des_res = [
                ann_md["voxel_resolution_x"],
                ann_md["voxel_resolution_y"],
                ann_md["voxel_resolution_z"],
            ]
            data["desired_resolution"] = des_res
    else:
        data["desired_resolution"] = None

    qm.join_tables(
        data["tables"][0][0],
        data["tables"][0][1],
        data["tables"][1][0],
        data["tables"][1][1],
    )

    qm.apply_filter(data.get("filter_in_dict", None), qm.apply_isin_filter)
    qm.apply_filter(data.get("filter_out_dict", None), qm.apply_notequal_filter)
    qm.apply_filter(data.get("filter_equal_dict", None), qm.apply_equal_filter)
    qm.apply_filter(data.get("filter_greater_dict", None), qm.apply_greater_filter)
    qm.apply_filter(data.get("filter_less_dict", None), qm.apply_less_filter)
    qm.apply_filter(data.get("filter_greater_equal_dict", None), qm.apply_greater_equal_filter)
    qm.apply_filter(data.get("filter_less_equal_dict", None), qm.apply_less_equal_filter)
    qm.apply_filter(data.get("filter_spatial_dict", None), qm.apply_spatial_filter)
    qm.apply_filter(data.get("filter_regex_dict", None), qm.apply_regex_filter)
    for table_info in data["tables"]:
        table_name = table_info[0]
        qm.apply_filter({table_name: {"valid": True}}, qm.apply_equal_filter)

    qm.apply_filter({table_name: {"valid": True}}, qm.apply_equal_filter)
    select_columns = data.get("select_columns", None)
    select_column_map = data.get("select_column_map", None)
    if select_columns:
        warn_text = textwrap.dedent(
            """\
            Select_columns is deprecated for join queries,
            please use select_column_map a dictionary which is more explicit
            about what columns to select from what tables.
            This query result will attempt to select the first column it finds
            of this name in any table, but if there are more than one such column
            it will not select both.
            Upgrade caveclient to >=5.0.0 ."""
        )
        warnings.append(warn_text)

        for column in select_columns:
            found = False
            for table in qm._tables:
                try:
                    qm.select_column(table, column)
                    found = True
                    break
                except ValueError:
                    pass
            if not found:
                abort(400, f"column {column} not found in any table referenced")
    elif select_column_map:
        for table, columns in select_column_map.items():
            for column in columns:
                try:
                    qm.select_column(table, column)
                except ValueError:
                    abort(400, f"column {column} not found in {table}")
    else:
        for table in qm._tables:
            qm.select_all_columns(table)

    df, column_names = qm.execute_query(desired_resolution=data["desired_resolution"])
    crud_columns, created_columns = collect_crud_columns(column_names)
    df.drop(crud_columns, axis=1, errors="ignore", inplace=True)

    if len(df) == limit:
        warnings.append(f'201 - "Limited query to {limit} rows')

    return df, warnings, column_names


def handle_complex_query(
    datastack_name,
    version,
    target_datastack,
    target_version,
    args,
    data,
    convert_desired_resolution=False,
):
    df, warnings, column_names = generate_complex_query_dataframe(
        datastack_name,
        version,
        target_datastack,
        target_version,
        args,
        data,
        convert_desired_resolution=convert_desired_resolution,
    )
    return create_query_response(
        df,
        warnings=warnings,
        column_names=column_names,
        desired_resolution=data["desired_resolution"],
        return_pyarrow=args["return_pyarrow"],
        arrow_format=args["arrow_format"],
        ipc_compress=args["ipc_compress"],
    )
