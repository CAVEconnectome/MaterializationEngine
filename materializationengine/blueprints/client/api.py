import pyarrow as pa
from cachetools import LRUCache, cached
from cloudfiles import compression
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion
from flask import Response, abort, current_app, request
from flask_accepts import accepts
from flask_restx import Namespace, Resource, inputs, reqparse
from materializationengine.blueprints.client.schemas import (
    ComplexQuerySchema,
    SimpleQuerySchema,
)
from materializationengine.blueprints.client.query_manager import QueryManager
from materializationengine.blueprints.client.utils import (
    add_warnings_to_headers,
    update_notice_text_warnings,
    after_request,
    create_query_response,
)
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.database import dynamic_annotation_cache, sqlalchemy_cache
from materializationengine.utils import check_read_permission
from materializationengine.info_client import (
    get_aligned_volumes,
    get_relevant_datastack_info,
)
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema
from middle_auth_client import auth_requires_permission
from materializationengine.blueprints.client.datastack import validate_datastack
from flask import g
import numpy as np
import textwrap

__version__ = "4.6.10"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

client_bp = Namespace(
    "Materialization Client",
    authorizations=authorizations,
    description="Materialization Client",
)

annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument(
    "annotation_ids", type=int, action="split", help="list of annotation ids"
)
annotation_parser.add_argument(
    "pcg_table_name", type=str, help="name of pcg segmentation table"
)


query_parser = reqparse.RequestParser()
query_parser.add_argument(
    "return_pyarrow",
    type=inputs.boolean,
    default=True,
    required=False,
    location="args",
    help="whether to return query in pyarrow compatible binary format (faster), false returns json",
)
query_parser.add_argument(
    "split_positions",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to return position columns as seperate x,y,z columns (faster)",
)
query_parser.add_argument(
    "count",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to only return the count of a query",
)


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


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

    analysis_version = (
        Session.query(AnalysisVersion)
        .filter(AnalysisVersion.datastack == datastack_name)
        .filter(AnalysisVersion.version == version)
        .first()
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


@cached(cache=LRUCache(maxsize=32))
def get_split_models(datastack_name: str, table_name: str):
    """get split models

    Args:
        datastack_name (str): datastack name
        table_name (str): table_name
        Session (_type_): session to live database

    Returns:
        (Model, Model): get annotation and segmentation model
    """
    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    schema_type = db.database.get_table_metadata(table_name, filter_col="schema_type")

    SegModel = db.schema.create_annotation_model(
        table_name, schema_type, segmentation_source=pcg_table_name
    )
    AnnModel = db.schema.create_annotation_model(table_name, schema_type)
    return AnnModel, SegModel


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


@client_bp.route("/datastack/<string:datastack_name>/versions")
class DatastackVersions(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("datastack_versions", security="apikey")
    def get(self, datastack_name: str):
        """get available versions

        Args:
            datastack_name (str): datastack name

        Returns:
            list(int): list of versions that are available
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        session = sqlalchemy_cache.get(aligned_volume_name)

        response = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack_name)
            .filter(AnalysisVersion.valid == True)
            .all()
        )

        versions = [av.version for av in response]
        return versions, 200


@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>")
class DatastackVersion(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("version metadata", security="apikey")
    def get(self, datastack_name: str, version: int):
        """get version metadata

        Args:
            datastack_name (str): datastack name
            version (int): version number

        Returns:
            dict: metadata dictionary for this version
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        session = sqlalchemy_cache.get(aligned_volume_name)

        response = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack_name)
            .filter(AnalysisVersion.version == version)
            .first()
        )
        if response is None:
            return "No version found", 404
        schema = AnalysisVersionSchema()
        return schema.dump(response), 200


@client_bp.route("/datastack/<string:datastack_name>/metadata")
class DatastackMetadata(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("all valid version metadata", security="apikey")
    def get(self, datastack_name: str):
        """get materialized metadata for all valid versions
        Args:
            datastack_name (str): datastack name
        Returns:
            list: list of metadata dictionaries
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        session = sqlalchemy_cache.get(aligned_volume_name)
        response = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack_name)
            .filter(AnalysisVersion.valid == True)
            .all()
        )
        if response is None:
            return "No valid versions found", 404
        schema = AnalysisVersionSchema()
        return schema.dump(response, many=True), 200


@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/tables")
class FrozenTableVersions(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("get_frozen_tables", security="apikey")
    def get(self, datastack_name: str, version: int):
        """get frozen tables

        Args:
            datastack_name (str): datastack name
            version (int): version number

        Returns:
            list(str): list of frozen tables in this version
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        session = sqlalchemy_cache.get(aligned_volume_name)

        av = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.version == version)
            .filter(AnalysisVersion.datastack == datastack_name)
            .first()
        )
        if av is None:
            abort(404, f"version {version} does not exist for {datastack_name} ")
        response = (
            session.query(AnalysisTable)
            .filter(AnalysisTable.analysisversion_id == av.id)
            .filter(AnalysisTable.valid == True)
            .all()
        )

        if response is None:
            return None, 404
        return [r.table_name for r in response], 200


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/metadata"
)
class FrozenTableMetadata(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("get_frozen_table_metadata", security="apikey")
    def get(self, datastack_name: str, version: int, table_name: str):
        """get frozen table metadata

        Args:
            datastack_name (str): datastack name
            version (int): version number
            table_name (str): table name

        Returns:
            dict: dictionary of table metadata
        """
        validate_table_args([table_name], datastack_name, version)
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        session = sqlalchemy_cache.get(aligned_volume_name)
        analysis_version, analysis_table = get_analysis_version_and_table(
            datastack_name, table_name, version, session
        )

        schema = AnalysisTableSchema()
        tables = schema.dump(analysis_table)

        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        ann_md = db.database.get_table_metadata(table_name)
        ann_md.pop("id")
        ann_md.pop("deleted")

        warnings = update_notice_text_warnings(ann_md, [])
        headers = add_warnings_to_headers({}, warnings)

        tables.update(ann_md)
        return tables, 200, headers


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/count"
)
class FrozenTableCount(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @validate_datastack
    @client_bp.doc("simple_query", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        table_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """get annotation count in table

        Args:
            datastack_name (str): datastack name of table
            version (int): version of table
            table_name (str): table name

        Returns:
            int: number of rows in this table
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        validate_table_args([table_name], target_datastack, target_version)
        Session = sqlalchemy_cache.get(aligned_volume_name)
        Model = get_flat_model(datastack_name, table_name, version, Session)

        Session = sqlalchemy_cache.get("{}__mat{}".format(datastack_name, version))
        return Session().query(Model).count(), 200


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/query"
)
class FrozenTableQuery(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @validate_datastack
    @client_bp.doc("simple_query", security="apikey")
    @accepts("SimpleQuerySchema", schema=SimpleQuerySchema, api=client_bp)
    def post(
        self,
        datastack_name: str,
        version: int,
        table_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            version (int): version number
            table_name (str): table name

        Payload:
        All values are optional.  Limit has an upper bound set by the server.
        Consult the schema of the table for column names and appropriate values
        {
            "filter_out_dict": {
                "tablename":{
                    "column_name":[excluded,values]
                }
            },
            "offset": 0,
            "limit": 200000,
            "select_columns": [
                "column","names"
            ],
            "filter_in_dict": {
                "tablename":{
                    "column_name":[included,values]
                }
            },
            "filter_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            "filter_spatial_dict": {
                "tablename": {
                "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """
        validate_table_args([table_name], target_datastack, target_version)
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        check_read_permission(db, table_name)

        ann_md = db.database.get_table_metadata(table_name)

        args = query_parser.parse_args()

        Session = sqlalchemy_cache.get(aligned_volume_name)
        analysis_version = (
            Session.query(AnalysisVersion)
            .filter(AnalysisVersion.version == version)
            .one()
        )
        max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)
        data = request.parsed_obj
        limit = data.get("limit", max_limit)
        if limit > max_limit:
            limit = max_limit

        get_count = args.get("count", False)
        if get_count:
            limit = None

        mat_db_name = f"{datastack_name}__mat{version}"

        if not data.get("desired_resolution", None):
            des_res = [
                ann_md["voxel_resolution_x"],
                ann_md["voxel_resolution_y"],
                ann_md["voxel_resolution_z"],
            ]
            data["desired_resolution"] = des_res

        qm = QueryManager(
            mat_db_name,
            segmentation_source=pcg_table_name,
            meta_db_name=aligned_volume_name,
            split_mode=not analysis_version.is_merged,
            limit=limit,
            offset=data.get("offset", 0),
            get_count=get_count,
        )
        qm.add_table(table_name)
        qm.apply_filter(data.get("filter_in_dict", None), qm.apply_isin_filter)
        qm.apply_filter(data.get("filter_out_dict", None), qm.apply_notequal_filter)
        qm.apply_filter(data.get("filter_equal_dict", None), qm.apply_equal_filter)
        qm.apply_filter(data.get("filter_spatial_dict", None), qm.apply_spatial_filter)

        select_columns = data.get("select_columns", None)
        if select_columns:
            for column in select_columns:
                qm.select_column(table_name, column)
        else:
            qm.select_all_columns(table_name)

        df, column_names = qm.execute_query(
            desired_resolution=data["desired_resolution"]
        )

        warnings = []
        current_app.logger.info("query: {}".format(data))
        current_app.logger.info("args: {}".format(args))
        user_id = str(g.auth_user["id"])
        current_app.logger.info(f"user_id: {user_id}")

        if len(df) == limit:
            warnings.append(f'201 - "Limited query to {limit} rows')
        warnings = update_notice_text_warnings(ann_md, warnings)
        return create_query_response(
            df,
            warnings=warnings,
            column_names=column_names,
            desired_resolution=data["desired_resolution"],
            return_pyarrow=args["return_pyarrow"],
        )


@client_bp.expect(query_parser)
@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/query")
class FrozenQuery(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @validate_datastack
    @client_bp.doc("complex_query", security="apikey")
    @accepts("ComplexQuerySchema", schema=ComplexQuerySchema, api=client_bp)
    def post(
        self,
        datastack_name: str,
        version: int,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for doing a query with filters and joins

        Args:
            datastack_name (str): datastack name
            version (int): version number

        Payload:
        All values are optional.  Limit has an upper bound set by the server.
        Consult the schema of the table for column names and appropriate values
        {
            "tables":[["table1", "table1_join_column"],
                      ["table2", "table2_join_column"]],
            "filter_out_dict": {
                "tablename":{
                    "column_name":[excluded,values]
                }
            },
            "offset": 0,
            "limit": 200000,
            "select_columns": [
                "column","names"
            ],
            "filter_in_dict": {
                "tablename":{
                    "column_name":[included,values]
                }
            },
            "filter_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            }
            "filter_spatial_dict": {
                "tablename":{
                    "column_name":[[min_x,min_y,minz], [max_x_max_y_max_z]]
                }
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """

        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        db = dynamic_annotation_cache.get_db(aligned_volume_name)

        Session = sqlalchemy_cache.get(aligned_volume_name)
        args = query_parser.parse_args()
        data = request.parsed_obj
        validate_table_args(
            [t[0] for t in data["tables"]], target_datastack, target_version
        )
        warnings = []

        for table_desc in data["tables"]:
            table_name = table_desc[0]
            ann_md = check_read_permission(db, table_name)
            warnings = update_notice_text_warnings(ann_md, warnings)

        db_name = f"{datastack_name}__mat{version}"

        analysis_version = (
            Session.query(AnalysisVersion)
            .filter(AnalysisVersion.version == version)
            .one()
        )
        max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

        data = request.parsed_obj
        limit = data.get("limit", max_limit)
        if limit > max_limit:
            limit = max_limit

        data = request.parsed_obj
        suffixes = data.get("suffixes", None)

        if suffixes is not None:
            warn_text = textwrap.dedent(
                """\
                Suffixes is deprecated for complex queries as it
                can be ambiguous what you desire, 
                please pass suffix_map as a dictionary to explicitly
                set suffixes for individual tables.
                Upgrade caveclient to >X.X.X """
            )
            warnings.append(warn_text)
            all_tables = []
            for table_desc in data["tables"]:
                all_tables.append(table_desc[0])
                all_tables.append(table_desc[1])
            u, ind = np.unique(all_tables, return_index=True)
            uniq_tables = u[np.argsort(ind)]
            suffixes = {t: s for t, s in zip(uniq_tables, suffixes)}
        else:
            suffixes = data.get("suffix_map")

        qm = QueryManager(
            db_name,
            segmentation_source=pcg_table_name,
            meta_db_name=aligned_volume_name,
            split_mode=not analysis_version.is_merged,
            suffixes=suffixes,
            limit=limit,
            offset=data.get("offset", 0),
            get_count=False,
        )
        if not data.get("desired_resolution", None):
            ann_md = db.database.get_table_metadata(data["tables"][0][0])
            des_res = [
                ann_md["voxel_resolution_x"],
                ann_md["voxel_resolution_y"],
                ann_md["voxel_resolution_z"],
            ]
            data["desired_resolution"] = des_res
        qm.join_tables(
            data["tables"][0][0],
            data["tables"][0][1],
            data["tables"][1][0],
            data["tables"][1][1],
        )

        qm.apply_filter(data.get("filter_in_dict", None), qm.apply_isin_filter)
        qm.apply_filter(data.get("filter_out_dict", None), qm.apply_notequal_filter)
        qm.apply_filter(data.get("filter_equal_dict", None), qm.apply_equal_filter)
        qm.apply_filter(data.get("filter_spatial_dict", None), qm.apply_spatial_filter)

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
                Upgrade caveclient to >X.X.X ."""
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

        df, column_names = qm.execute_query(
            desired_resolution=data["desired_resolution"]
        )

        if len(df) == limit:
            warnings.append(f'201 - "Limited query to {limit} rows')

        return create_query_response(
            df,
            warnings=warnings,
            column_names=column_names,
            desired_resolution=data["desired_resolution"],
            return_pyarrow=args["return_pyarrow"],
        )
