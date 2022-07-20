import logging
import os
import time

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
from materializationengine.blueprints.client.query import _execute_query, specific_query
from materializationengine.blueprints.client.schemas import (
    ComplexQuerySchema,
    CreateTableSchema,
    GetDeleteAnnotationSchema,
    Metadata,
    PostPutAnnotationSchema,
    SegmentationDataSchema,
    SegmentationTableSchema,
    SimpleQuerySchema,
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


@cached(cache=TTLCache(maxsize=64, ttl=600))
def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info["segmentation_source"]
    pcg_table_name = seg_source.split("/")[-1]
    aligned_volume_name = ds_info["aligned_volume"]["name"]
    return aligned_volume_name, pcg_table_name


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
def get_split_models(datastack_name: str, table_name: str, with_crud_columns=True):
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
    "/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/count"
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
@client_bp.route("/datastack/<string:datastack_name>/table/<string:table_name>/query")
class LiveTableQuery(Resource):
    @reset_auth
    @auth_requires_permission("admin_view", table_arg="datastack_name")
    @client_bp.doc("live_simple_query", security="apikey")
    @accepts("SimpleQuerySchema", schema=SimpleQuerySchema, api=client_bp)
    def post(self, datastack_name: str, table_name: str):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            table_name (str): table names

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

        filter_equal_dict = data.get("filter_equal_dict", None)
        if filter_equal_dict is None:
            filter_equal_dict = {table_name: {"valid": "t"}}
        else:
            filter_equal_dict["table_name"].update({"valid": "t"})
        print(filter_equal_dict)
        df = specific_query(
            Session,
            engine,
            {table_name: AnnModel, seg_table: SegModel},
            [[table_name, "id"], [seg_table, "id"]],
            filter_in_dict=_format_filter(
                data.get("filter_in_dict", None), table_name, seg_table
            ),
            filter_notin_dict=_format_filter(
                data.get("filter_notin_dict", None), table_name, seg_table
            ),
            filter_equal_dict=_format_filter(filter_equal_dict, table_name, seg_table),
            filter_spatial=data.get("filter_spatial_dict", None),
            select_columns=data.get("select_columns", None),
            consolidate_positions=not args["split_positions"],
            offset=data.get("offset", None),
            limit=limit,
            get_count=get_count,
            outer_join=True,
        )
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


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int:version>/table/<string:table_name>/query"
)
class FrozenTableQuery(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("simple_query", security="apikey")
    @accepts("SimpleQuerySchema", schema=SimpleQuerySchema, api=client_bp)
    def post(self, datastack_name: str, version: int, table_name: str):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            version (int): version number
            table_name (str): table names

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
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        args = query_parser.parse_args()
        Session = sqlalchemy_cache.get(aligned_volume_name)
        time_d = {}
        now = time.time()

        data = request.parsed_obj
        Model = get_flat_model(datastack_name, table_name, version, Session)
        time_d["get Model"] = time.time() - now
        now = time.time()

        Session = sqlalchemy_cache.get("{}__mat{}".format(datastack_name, version))
        time_d["get Session"] = time.time() - now
        now = time.time()

        engine = sqlalchemy_cache.get_engine(
            "{}__mat{}".format(datastack_name, version)
        )
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

        logging.debug("query {}".format(data))
        logging.debug("args - {}".format(args))

        time_d["setup query"] = time.time() - now
        now = time.time()

        df = specific_query(
            Session,
            engine,
            {table_name: Model},
            [table_name],
            filter_in_dict=data.get("filter_in_dict", None),
            filter_notin_dict=data.get("filter_notin_dict", None),
            filter_equal_dict=data.get("filter_equal_dict", None),
            filter_spatial=data.get("filter_spatial_dict", None),
            select_columns=data.get("select_columns", None),
            consolidate_positions=not args["split_positions"],
            offset=data.get("offset", None),
            limit=limit,
            get_count=get_count,
        )
        time_d["execute query"] = time.time() - now
        now = time.time()
        headers = None
        if len(df) == limit:
            headers = {"Warning": f'201 - "Limited query to {max_limit} rows'}

        if args["return_pyarrow"]:
            context = pa.default_serialization_context()
            serialized = context.serialize(df).to_buffer().to_pybytes()
            time_d["serialize"] = time.time() - now
            logging.debug(time_d)
            return Response(
                serialized, headers=headers, mimetype="x-application/pyarrow"
            )
        else:
            dfjson = df.to_json(orient="records")
            time_d["serialize"] = time.time() - now
            logging.debug(time_d)
            response = Response(dfjson, headers=headers, mimetype="application/json")
            return after_request(response)


@client_bp.expect(query_parser)
@client_bp.route("/datastack/<string:datastack_name>/version/<int:version>/query")
class FrozenQuery(Resource):
    @reset_auth
    @auth_requires_permission("view", table_arg="datastack_name")
    @client_bp.doc("complex_query", security="apikey")
    @accepts("ComplexQuerySchema", schema=ComplexQuerySchema, api=client_bp)
    def post(self, datastack_name: str, version: int):
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

        Session = sqlalchemy_cache.get(aligned_volume_name)
        args = query_parser.parse_args()
        data = request.parsed_obj
        model_dict = {}
        for table_desc in data["tables"]:
            table_name = table_desc[0]
            Model = get_flat_model(datastack_name, table_name, version, Session)
            model_dict[table_name] = Model

        db_name = "{}__mat{}".format(datastack_name, version)
        Session = sqlalchemy_cache.get(db_name)
        engine = sqlalchemy_cache.get_engine(db_name)
        max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

        data = request.parsed_obj
        limit = data.get("limit", max_limit)
        if limit > max_limit:
            limit = max_limit
        logging.debug("query {}".format(data))
        df = specific_query(
            Session,
            engine,
            model_dict,
            data["tables"],
            filter_in_dict=data.get("filter_in_dict", None),
            filter_notin_dict=data.get("filter_notin_dict", None),
            filter_equal_dict=data.get("filter_equal_dict", None),
            filter_spatial=data.get("filter_spatial_dict", None),
            select_columns=data.get("select_columns", None),
            consolidate_positions=not args["split_positions"],
            offset=data.get("offset", None),
            limit=limit,
            suffixes=data.get("suffixes", None),
        )
        headers = None
        if len(df) == limit:
            headers = {"Warning": f'201 - "Limited query to {max_limit} rows'}

        if args["return_pyarrow"]:
            context = pa.default_serialization_context()
            serialized = context.serialize(df).to_buffer().to_pybytes()
            return Response(
                serialized, headers=headers, mimetype="x-application/pyarrow"
            )
        else:
            dfjson = df.to_json(orient="records")
            response = Response(dfjson, headers=headers, mimetype="application/json")
            return after_request(response)


# @client_bp.route("/aligned_volume/<string:aligned_volume_name>/table")
# class SegmentationTable(Resource):
#     @auth_required
#     @client_bp.doc("create_segmentation_table", security="apikey")
#     @accepts("SegmentationTableSchema", schema=SegmentationTableSchema, api=client_bp)
#     def post(self, aligned_volume_name: str):
#         """ Create a new segmentation table"""
#         check_aligned_volume(aligned_volume_name)

#         data = request.parsed_obj
#         db = get_db(aligned_volume_name)

#         annotation_table_name = data.get("table_name")
#         pcg_table_name = data.get("pcg_table_name")

#         table_info = db.create_and_attach_seg_table(
#             annotation_table_name, pcg_table_name)

#         return table_info, 200

#     @auth_required
#     @client_bp.doc("get_aligned_volume_tables", security="apikey")
#     def get(self, aligned_volume_name: str):
#         """ Get list of segmentation tables for an aligned_volume"""
#         check_aligned_volume(aligned_volume_name)
#         db = get_db(aligned_volume_name)
#         tables = db.get_existing_segmentation_table_ids()
#         return tables, 200


# @client_bp.route(
#     "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/segmentations"
# )
# class LinkedSegmentations(Resource):
#     @auth_required
#     @client_bp.doc("post linked annotations", security="apikey")
#     @accepts("SegmentationDataSchema", schema=SegmentationDataSchema, api=client_bp)
#     def post(self, aligned_volume_name: str, table_name: str, **kwargs):
#         """ Insert linked segmentations """
#         check_aligned_volume(aligned_volume_name)
#         data = request.parsed_obj
#         segmentations = data.get("segmentations")
#         pcg_table_name = data.get("pcg_table_name")
#         db = get_db(aligned_volume_name)
#         try:
#             db.insert_linked_segmentation(table_name,
#                                           pcg_table_name,
#                                           segmentations)
#         except Exception as error:
#             logging.error(f"INSERT FAILED {segmentations}")
#             abort(404, error)

#         return f"Inserted {len(segmentations)} annotations", 200
# @client_bp.route(
#     "/aligned_volume/<string:aligned_volume_name>/table/<string:table_name>/annotations"
# )
# class LinkedAnnotations(Resource):
#     @auth_required
#     @client_bp.doc("get linked annotations", security="apikey")
#     @client_bp.expect(annotation_parser)
#     def get(self, aligned_volume_name: str, table_name: str, **kwargs):
#         """ Get annotations and segmentation from list of IDs"""
#         check_aligned_volume(aligned_volume_name)
#         args = annotation_parser.parse_args()

#         ids = args["annotation_ids"]
#         pcg_table_name = args["pcg_table_name"]

#         db = get_db(aligned_volume_name)
#         annotations = db.get_linked_annotations(table_name,
#                                                 pcg_table_name,
#                                                 ids)

#         if annotations is None:
#             msg = f"annotation_id {ids} not in {table_name}"
#             abort(404, msg)

#         return annotations, 200

#     @auth_required
#     @client_bp.doc("post linked annotations", security="apikey")
#     @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
#     def post(self, aligned_volume_name: str, table_name: str, **kwargs):
#         """ Insert linked annotations """
#         check_aligned_volume(aligned_volume_name)
#         data = request.parsed_obj
#         annotations = data.get("annotations")
#         pcg_table_name = data.get("pcg_table_name")
#         db = get_db(aligned_volume_name)
#         try:
#             db.insert_linked_annotations(table_name,
#                                          pcg_table_name,
#                                          annotations)
#         except Exception as error:
#             logging.error(f"INSERT FAILED {annotations}")
#             abort(404, error)

#         return f"Inserted {len(annotations)} annotations", 200

#     @auth_required
#     @client_bp.doc("update linked annotations", security="apikey")
#     @accepts("PostPutAnnotationSchema", schema=PostPutAnnotationSchema, api=client_bp)
#     def put(self, aligned_volume_name: str, table_name: str, **kwargs):
#         """ Update linked annotations """
#         check_aligned_volume(aligned_volume_name)
#         data = request.parsed_obj
#         annotations = data.get("annotations")
#         pcg_table_name = data.get("pcg_table_name")
#         db = get_db(aligned_volume_name)

#         metadata = db.get_table_metadata(aligned_volume_name, table_name)
#         schema = metadata.get("schema_type")

#         if schema:
#             for annotation in annotations:
#                 db.update_linked_annotations(table_name,
#                                              pcg_table_name,
#                                              annotation)

#         return f"Updated {len(data)} annotations", 200

#     @auth_required
#     @client_bp.doc("delete linked annotations", security="apikey")
#     @accepts("GetDeleteAnnotationSchema", schema=GetDeleteAnnotationSchema, api=client_bp)
#     def delete(self, aligned_volume_name: str, table_name: str, **kwargs):
#         """ Delete linked annotations """
#         check_aligned_volume(aligned_volume_name)
#         data = request.parsed_obj

#         ids = data.get("annotation_ids")
#         pcg_table_name = data.get("pcg_table_name")
#         db = get_db(aligned_volume_name)

#         for anno_id in ids:
#             ann = db.delete_linked_annotation(table_name,
#                                               pcg_table_name,
#                                               ids)

#         if ann is None:
#             msg = f"annotation_id {ids} not in {table_name}"
#             abort(404, msg)

#         return ann, 200
