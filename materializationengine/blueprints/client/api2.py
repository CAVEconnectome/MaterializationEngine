import logging
import os
import time
from tkinter import FALSE

import pyarrow as pa
from cachetools import LRUCache, TTLCache, cached
from cloudfiles import compression
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion
from emannotationschemas import get_schema
from emannotationschemas.models import (
    Base,
    create_table_dict,
    make_annotation_model,
    make_flat_model,
    make_segmentation_model,
    sqlalchemy_models,
)
from flask import Response, abort, current_app, request, stream_with_context
from flask_accepts import accepts, responds
from flask_restx import Namespace, Resource, inputs, reqparse
from materializationengine.blueprints.client.query import _execute_query, execute_query_manager, specific_query
from materializationengine.blueprints.client.schemas import (
    ComplexQuerySchema,
    CreateTableSchema,
    GetDeleteAnnotationSchema,
    Metadata,
    PostPutAnnotationSchema,
    SegmentationDataSchema,
    SegmentationTableSchema,
    SimpleQuerySchema,
    V2QuerySchema,
)
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.database import (
    create_session,
    dynamic_annotation_cache,
    sqlalchemy_cache,
)
from materializationengine.info_client import (
    get_aligned_volumes,
    get_datastack_info,
    get_datastacks,
)
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema
from middle_auth_client import (
    auth_required,
    auth_requires_admin,
    auth_requires_permission,
)
from sqlalchemy.engine.url import make_url

__version__ = "4.0.20"


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


def after_request(response):

    accept_encoding = request.headers.get("Accept-Encoding", "")

    if "gzip" not in accept_encoding.lower():
        return response

    response.direct_passthrough = False

    if (
        response.status_code < 200
        or response.status_code >= 300
        or "Content-Encoding" in response.headers
    ):
        return response

    response.data = compression.gzip_compress(response.data)

    response.headers["Content-Encoding"] = "gzip"
    response.headers["Vary"] = "Accept-Encoding"
    response.headers["Content-Length"] = len(response.data)

    return response


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
        .filter(AnalysisTable.analysisversion_id == AnalysisVersion.id)
        .filter(AnalysisTable.table_name == table_name)
        .first()
    )
    if analysis_version is None:
        return analysis_version, None
    return analysis_version, analysis_table


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
        segmentation_source=None,
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
            return None, 404
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
        tables.update(ann_md)
        return tables, 200


@client_bp.route(
    @client_bp.route("/datastack/<string:datastack_name>/query/count")
)
class FrozenTableCount(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("simple_query", security="apikey")
    def get(self, datastack_name: str, version: int, table_name: str):
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

        Session = sqlalchemy_cache.get(aligned_volume_name)
        Model = get_flat_model(datastack_name, table_name, version, Session)

        Session = sqlalchemy_cache.get("{}__mat{}".format(datastack_name, version))
        return Session().query(Model).count(), 200


@client_bp.expect(query_parser)
@client_bp.route("/datastack/<string:datastack_name>/query")
class LiveTableQuery(Resource):
    @reset_auth
    @auth_requires_permission("admin_view", table_arg="datastack_name")
    @client_bp.doc("v2_query", security="apikey")
    @accepts("V2QuerySchema", schema=V2QuerySchema, api=client_bp)
    def post(self, datastack_name: str, table_name: str):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            table_name (str): table names

        Payload:
        All values are optional.  Limit has an upper bound set by the server.
        Consult the schema of the table for column names and appropriate values
        {
            "table":"table_name",
            "joins":[[table_name,table_column], [joined_table,joined_column],
                     [joined_table, joincol2],[third_table, joincol_third]]
            "timestamp": "XXXXXXX",
            "offset": 0,
            "limit": 200000,
            "suffixes":{
                "table_name":"suffix1",
                "joined_table":"suffix2",
                "third_table":"suffix3"
            },
            "select_columns": {
                "table_name":[ "column","names"]
            },
            "filter_in_dict": {
                "table_name":{
                    "column_name":[included,values]
                }
            },
            "filter_out_dict": {
                "table_name":{
                    "column_name":[excluded,values]
                }
            },
            "filter_equal_dict": {
                "table_name":{
                    "column_name":value
                }
            "filter_spatial_dict": {
                "table_name": {
                "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """

        

        args = query_parser.parse_args() 
        user_data = request.parsed_obj
        mat_version = get_closest_mat_version(datastack, user_data['timestamp'])
        mat_df = execute_materialized_query(datastack, mat_version, user_data)
        prod_df = execute_production_query(datastack, user_data, mat_version.timestamp, args['timestamp'])
        df = combine_queries(mat_df, prod_df, data)

        return format_dataframe(df)

    


        if len(df) == limit:
            headers = {"Warning": f'201 - "Limited query to {max_limit} rows'}

        if args["return_pyarrow"]:
            context = pa.default_serialization_context()
            serialized = context.serialize(df).to_buffer().to_pybytes()
            time_d["serialize"] = time.time() - now
            logging.info(time_d)
            return Response(
                serialized, headers=headers, mimetype="x-application/pyarrow"
            )
        else:
            dfjson = df.to_json(orient="records")
            time_d["serialize"] = time.time() - now
            logging.info(time_d)
            response = Response(dfjson, headers=headers, mimetype="application/json")
            return after_request(response)

        
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        args = query_parser.parse_args()
        Session = sqlalchemy_cache.get(aligned_volume_name)
        time_d = {}
        now = time.time()

        data = request.parsed_obj

        AnnModel, SegModel = get_split_models(datastack_name, table_name)
        time_d["get Model"] = time.time() - now
        now = time.time()

        engine = sqlalchemy_cache.get_engine(aligned_volume_name)
        time_d["get engine"] = time.time() - now
        now = time.time()
        max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

        data = request.parsed_obj
        time_d["get data"] = time.time() - now
        now = time.time()
        limit = data.get("limit", max_limit)

        if limit > max_limit:
            limit = max_limit

        get_count = args.get("count", False)

        if get_count:
            limit = None

        logging.info("query {}".format(data))
        logging.info("args - {}".format(args))

        time_d["setup query"] = time.time() - now
        now = time.time()
        seg_table = f"{table_name}__{datastack_name}"

        def _format_filter(filter, table_in, seg_table):
            if filter is None:
                return filter
            else:
                table_filter = filter.get(table_in, None)
                root_id_filters = [
                    c for c in table_filter.keys() if c.endswith("root_id")
                ]
                other_filters = [
                    c for c in table_filter.keys() if not c.endswith("root_id")
                ]

                filter[seg_table] = {c: table_filter[c] for c in root_id_filters}
                filter[table_in] = {c: table_filter[c] for c in other_filters}
                return filter

        user_data = request.parsed_obj

        mat_query_manager = QueryManager(mat_engine, mat_Session, live=False)
        # modify user_data for materialized data
        # modify filters back in time
        # add in joins for reference annotations
        mat_query_manager.parse_request(user_data)
        mat_df = execute_query_manager(query_manager)
        # update expired root_ids
        # reapply orignal filters

        # if timestamp doesn't match materialization
        live_query_manager = QueryManager(live_engine, live_Session, live=True)
        user_data = request.parsed_obj
        # modify user_data for live data
        # modify filters back in time to last update
        # modify filters to apply segmenation filters to segmentation tables
        # add in joins for ann>segmentation tables
        # add in joins for reference annotations (and ref_ann>ref_seg)
        # add in filters to exclude things before materialization
        live_query_manager.parse_request(user_data)
        live_df = execute_query_manager(query_manager)
        # update expired root_ids
        # reapply orignal filters
        user_data = request.parsed_obj
        mat_query_manager = QueryManager(mat_engine, matSession)
        # modify user_data for materialized data
        mat_query_manager.parse_request(user_data)

        mat_df = execute_query_manager(query_manager)

        time_d["execute query"] = time.time() - now
        now = time.time()
        headers = None
        if len(df) == limit:
            headers = {"Warning": f'201 - "Limited query to {max_limit} rows'}

        if args["return_pyarrow"]:
            context = pa.default_serialization_context()
            serialized = context.serialize(df).to_buffer().to_pybytes()
            time_d["serialize"] = time.time() - now
            logging.info(time_d)
            return Response(
                serialized, headers=headers, mimetype="x-application/pyarrow"
            )
        else:
            dfjson = df.to_json(orient="records")
            time_d["serialize"] = time.time() - now
            logging.info(time_d)
            response = Response(dfjson, headers=headers, mimetype="application/json")
            return after_request(response)
