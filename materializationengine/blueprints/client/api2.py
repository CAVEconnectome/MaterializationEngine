import json
import pytz
from dynamicannotationdb.models import AnalysisTable, AnalysisVersion

from cachetools import TTLCache, cached, LRUCache
from cachetools.keys import hashkey
from functools import wraps
from flask import Response, abort, request, current_app, g
from flask_accepts import accepts
from flask_restx import Namespace, Resource, inputs, reqparse
from middle_auth_client import (
    auth_requires_permission,
)
import pandas as pd
import numpy as np
from marshmallow import fields as mm_fields
from emannotationschemas.schemas.base import SegmentationField, PostGISField
import datetime
from typing import List
import werkzeug
from sqlalchemy.sql.sqltypes import String, Integer, Float, DateTime, Boolean, Numeric

from geoalchemy2.types import Geometry
import nglui
from materializationengine.blueprints.client.datastack import validate_datastack
from materializationengine.blueprints.client.new_query import (
    remap_query,
    strip_root_id_filters,
    update_rootids,
)
from materializationengine.blueprints.client.query_manager import QueryManager
from materializationengine.blueprints.client.utils import (
    create_query_response,
    collect_crud_columns,
    get_latest_version,
)
from materializationengine.blueprints.client.schemas import (
    ComplexQuerySchema,
    SimpleQuerySchema,
    V2QuerySchema,
    AnalysisViewSchema,
)
from materializationengine.utils import check_read_permission
from materializationengine.models import MaterializedMetadata
from materializationengine.blueprints.reset_auth import reset_auth
from materializationengine.blueprints.client.common import (
    handle_complex_query,
    generate_complex_query_dataframe,
    generate_simple_query_dataframe,
    handle_simple_query,
    validate_table_args,
    get_analysis_version_and_table,
    get_analysis_version_and_tables,
    unhandled_exception as common_unhandled_exception,
)
from materializationengine.chunkedgraph_gateway import chunkedgraph_cache
from materializationengine.limiter import limit_by_category
from materializationengine.database import (
    dynamic_annotation_cache,
    sqlalchemy_cache,
)
from materializationengine.info_client import get_aligned_volumes, get_datastack_info
from materializationengine.schemas import AnalysisTableSchema, AnalysisVersionSchema
from materializationengine.blueprints.client.utils import update_notice_text_warnings
from materializationengine.blueprints.client.utils import after_request

__version__ = "4.34.3"


authorizations = {
    "apikey": {"type": "apiKey", "in": "query", "name": "middle_auth_token"}
}

client_bp = Namespace(
    "Materialization Client2",
    authorizations=authorizations,
    description="Materialization Client",
)


@client_bp.errorhandler(werkzeug.exceptions.BadRequest)
def bad_request_exception(e):
    raise e


@client_bp.errorhandler(Exception)
def unhandled_exception(e):
    return common_unhandled_exception(e)


annotation_parser = reqparse.RequestParser()
annotation_parser.add_argument(
    "annotation_ids", type=int, action="split", help="list of annotation ids"
)
annotation_parser.add_argument(
    "pcg_table_name", type=str, help="name of pcg segmentation table"
)


def _get_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError(f"{value} is not a valid float")


class float_range(object):
    """Restrict input to an float in a range (inclusive)"""

    def __init__(self, low, high, argument="argument"):
        self.low = low
        self.high = high
        self.argument = argument

    def __call__(self, value):
        value = _get_float(value)
        if value < self.low or value > self.high:
            msg = "Invalid {arg}: {val}. {arg} must be within the range {lo} - {hi}"
            raise ValueError(
                msg.format(arg=self.argument, val=value, lo=self.low, hi=self.high)
            )
        return value

    @property
    def __schema__(self):
        return {
            "type": "integer",
            "minimum": self.low,
            "maximum": self.high,
        }


query_parser = reqparse.RequestParser()
query_parser.add_argument(
    "return_pyarrow",
    type=inputs.boolean,
    default=True,
    required=False,
    location="args",
    help=(
        "whether to return query in pyarrow compatible binary format"
        "(faster), false returns json"
    ),
)
query_parser.add_argument(
    "arrow_format",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help=("whether to convert dataframe to pyarrow ipc batch format"),
)
query_parser.add_argument(
    "random_sample",
    type=inputs.positive,
    default=None,
    required=False,
    location="args",
    help="How many samples to randomly get using tablesample on annotation tables, useful for visualization of large tables does not work as a random sample of query",
)
query_parser.add_argument(
    "split_positions",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help=("whether to return position columns" "as seperate x,y,z columns (faster)"),
)
query_parser.add_argument(
    "count",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to only return the count of a query",
)
query_parser.add_argument(
    "allow_missing_lookups",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to return annotation results when there\
 are new annotations that exist but haven't yet had supervoxel and \
rootId lookups. A warning will still be returned, but no 406 error thrown.",
)
query_parser.add_argument(
    "allow_invalid_root_ids",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to let a query proceed when passed a set of root ids\
 that are not valid at the timestamp that is queried. If True the filter will likely \
not be relevant and the user might not be getting data back that they expect, but it will not error.",
)
query_parser.add_argument(
    "ipc_compress",
    type=inputs.boolean,
    default=True,
    required=False,
    location="args",
    help="whether to have arrow compress the result when using \
          return_pyarrow=True and arrow_format=True. \
          If False, the result will not have it's internal data\
          compressed (note that the entire response \
          will be gzip compressed if accept-enconding includes gzip). \
          If True, accept-encoding will determine what \
          internal compression is used",
)


query_seg_prop_parser = reqparse.RequestParser()
# add an argument for a string controlling the label format
query_seg_prop_parser.add_argument(
    "label_format",
    type=str,
    default=None,
    required=False,
    location="args",
    help="string controlling the label format, should be formatted like a python format string,\
          i.e. {cell_type}_{id}, utilizing the columns available in the response",
)
# add an argument which is a list of column strings
query_seg_prop_parser.add_argument(
    "label_columns",
    type=str,
    action="split",
    default=None,
    required=False,
    location="args",
    help="list of column names include in a label (will be overridden by label_format)",
)
metadata_parser = reqparse.RequestParser()
# add a boolean argument for whether to return all expired versions
metadata_parser.add_argument(
    "expired",
    type=inputs.boolean,
    default=False,
    required=False,
    location="args",
    help="whether to return all expired versions",
)


@cached(cache=TTLCache(maxsize=64, ttl=600))
def get_relevant_datastack_info(datastack_name):
    ds_info = get_datastack_info(datastack_name=datastack_name)
    seg_source = ds_info["segmentation_source"]
    pcg_table_name = seg_source.split("/")[-1]
    aligned_volume_name = ds_info["aligned_volume"]["name"]
    return aligned_volume_name, pcg_table_name


def check_aligned_volume(aligned_volume):
    aligned_volumes = get_aligned_volumes()
    if aligned_volume not in aligned_volumes:
        abort(400, f"aligned volume: {aligned_volume} not valid")


def get_closest_versions(datastack_name: str, timestamp: datetime.datetime):
    avn, _ = get_relevant_datastack_info(datastack_name)

    # get session object
    session = sqlalchemy_cache.get(avn)

    # query analysis versions to get a valid version which is
    # the closest to the timestamp while still being older
    # than the timestamp
    past_version = (
        session.query(AnalysisVersion)
        .filter(AnalysisVersion.datastack == datastack_name)
        .filter(AnalysisVersion.valid == True)
        .filter(AnalysisVersion.time_stamp < timestamp)
        .order_by(AnalysisVersion.time_stamp.desc())
        .first()
    )
    # query analysis versions to get a valid version which is
    # the closest to the timestamp while still being newer
    # than the timestamp
    future_version = (
        session.query(AnalysisVersion)
        .filter(AnalysisVersion.datastack == datastack_name)
        .filter(AnalysisVersion.valid == True)
        .filter(AnalysisVersion.time_stamp > timestamp)
        .order_by(AnalysisVersion.time_stamp.asc())
        .first()
    )
    return past_version, future_version, avn


def check_column_for_root_id(col):
    if isinstance(col, str):
        if col.endswith("root_id"):
            abort(400, "we are not presently supporting joins on root_ids")
    elif isinstance(col, list):
        for c in col:
            if c.endwith("root_id"):
                abort(400, "we are not presently supporting joins on root ids")


def check_joins(joins):
    for join in joins:
        check_column_for_root_id(join[1])
        check_column_for_root_id(join[3])


def execute_materialized_query(
    datastack: str,
    aligned_volume: str,
    mat_version: int,
    pcg_table_name: str,
    user_data: dict,
    query_map: dict,
    cg_client,
    random_sample: int = None,
    split_mode: bool = False,
) -> pd.DataFrame:
    """_summary_

    Args:
        datastack (str): datastack to query on
        mat_version (int): verison to query on
        user_data (dict): dictionary of query payload including filters

    Returns:
        pd.DataFrame: a dataframe with the results of the query in the materialized version
        dict[dict]: a dictionary of table names, with values that are a dictionary
        that has keys of model column names, and values of their name in the dataframe with suffixes added
        if necessary to disambiguate.
    """
    mat_db_name = f"{datastack}__mat{mat_version}"
    session = sqlalchemy_cache.get(mat_db_name)
    mat_row_count = (
        session.query(MaterializedMetadata.row_count)
        .filter(MaterializedMetadata.table_name == user_data["table"])
        .scalar()
    )
    if random_sample is not None:
        random_sample = (100.0 * random_sample) / mat_row_count
    if mat_row_count:
        # setup a query manager
        qm = QueryManager(
            mat_db_name,
            segmentation_source=pcg_table_name,
            meta_db_name=aligned_volume,
            split_mode=split_mode,
            random_sample=random_sample,
        )
        qm.configure_query(user_data)
        qm.apply_filter({user_data["table"]: {"valid": True}}, qm.apply_equal_filter)
        # return the result
        df, column_names = qm.execute_query(
            desired_resolution=user_data["desired_resolution"]
        )
        df, warnings = update_rootids(df, user_data["timestamp"], query_map, cg_client)
        if len(df) >= user_data["limit"]:
            warnings.append(
                f"result has {len(df)} entries, which is equal or more \
than limit of {user_data['limit']} there may be more results which are not shown"
            )
        return df, column_names, warnings
    else:
        return None, {}, []


def execute_production_query(
    aligned_volume_name: str,
    segmentation_source: str,
    user_data: dict,
    chosen_timestamp: datetime.datetime,
    cg_client,
    allow_missing_lookups: bool = False,
) -> pd.DataFrame:
    """_summary_

    Args:
        datastack (str): _description_
        user_data (dict): _description_
        timestamp_start (datetime.datetime): _description_
        timestamp_end (datetime.datetime): _description_

    Returns:
        pd.DataFrame: _description_
    """
    user_timestamp = user_data["timestamp"]
    if chosen_timestamp < user_timestamp:
        query_forward = True
        start_time = chosen_timestamp
        end_time = user_timestamp
    elif chosen_timestamp > user_timestamp:
        query_forward = False
        start_time = user_timestamp
        end_time = chosen_timestamp
    else:
        abort(400, "do not use live live query to query a materialized timestamp")

    # setup a query manager on production database with split tables
    qm = QueryManager(
        aligned_volume_name, segmentation_source, split_mode=True, split_mode_outer=True
    )
    user_data_modified = strip_root_id_filters(user_data)

    qm.configure_query(user_data_modified)
    qm.select_column(user_data["table"], "created")
    qm.select_column(user_data["table"], "deleted")
    qm.select_column(user_data["table"], "superceded_id")
    qm.apply_table_crud_filter(user_data["table"], start_time, end_time)
    df, column_names = qm.execute_query(
        desired_resolution=user_data["desired_resolution"]
    )

    df, warnings = update_rootids(
        df, user_timestamp, {}, cg_client, allow_missing_lookups
    )
    if len(df) >= user_data["limit"]:
        warnings.append(
            f"result has {len(df)} entries, which is equal or more \
than limit of {user_data['limit']} there may be more results which are not shown"
        )
    return df, column_names, warnings


def apply_filters(df, user_data, column_names):
    filter_in_dict = user_data.get("filter_in_dict", None)
    filter_out_dict = user_data.get("filter_out_dict", None)
    filter_equal_dict = user_data.get("filter_equal_dict", None)
    filter_greater_dict = user_data.get("filter_greater_dict", None)
    filter_less_dict = user_data.get("filter_less_dict", None)
    filter_greater_equal_dict = user_data.get("filter_greater_equal_dict", None)
    filter_less_equal_dict = user_data.get("filter_less_equal_dict", None)

    if filter_in_dict:
        for table, filter in filter_in_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname].isin(val)]
    if filter_out_dict:
        for table, filter in filter_in_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[~df[colname].isin(val)]
    if filter_equal_dict:
        for table, filter in filter_equal_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname] == val]
    if filter_greater_dict:
        for table, filter in filter_greater_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname] > val]
    if filter_less_dict:
        for table, filter in filter_less_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname] < val]
    if filter_greater_equal_dict:
        for table, filter in filter_greater_equal_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname] >= val]
    if filter_less_equal_dict:
        for table, filter in filter_less_equal_dict.items():
            for col, val in filter.items():
                colname = column_names[table][col]
                df = df[df[colname] <= val]
    return df


def combine_queries(
    mat_df: pd.DataFrame,
    prod_df: pd.DataFrame,
    chosen_version: AnalysisVersion,
    user_data: dict,
    column_names: dict,
) -> pd.DataFrame:
    """combine a materialized query with an production query
       will remove deleted rows from materialized query,
       strip deleted entries from prod_df remove any CRUD columns
       and then append the two dataframes together to be a coherent
       result.

    Args:
        mat_df (pd.DataFrame): _description_
        prod_df (pd.DataFrame): _description_
        user_data (dict): _description_

    Returns:
        pd.DataFrame: _description_
    """
    crud_columns, created_columns = collect_crud_columns(column_names=column_names)
    if mat_df is not None:
        if len(mat_df) == 0:
            if prod_df is None:
                return mat_df.drop(columns=crud_columns, axis=1, errors="ignore")
            else:
                mat_df = None
    user_timestamp = user_data["timestamp"]
    chosen_timestamp = pytz.utc.localize(chosen_version.time_stamp)
    table = user_data["table"]
    if mat_df is not None:
        mat_df = mat_df.set_index(column_names[table]["id"])
    if prod_df is not None:
        prod_df = prod_df.set_index(column_names[table]["id"])
    if (prod_df is None) and (mat_df is None):
        abort(400, f"This query on table {user_data['table']} returned no results")

    crud_columns, created_columns = collect_crud_columns(column_names=column_names)

    if prod_df is not None:
        # if we are moving forward in time
        if chosen_timestamp < user_timestamp:
            deleted_between = (
                prod_df[column_names[table]["deleted"]] > chosen_timestamp
            ) & (prod_df[column_names[table]["deleted"]] < user_timestamp)
            created_between = (
                prod_df[column_names[table]["created"]] > chosen_timestamp
            ) & (prod_df[column_names[table]["created"]] < user_timestamp)

            to_delete_in_mat = deleted_between & ~created_between
            to_add_in_mat = created_between & ~deleted_between
            if len(prod_df[deleted_between].index) > 0:
                cut_prod_df = prod_df.drop(prod_df[deleted_between].index, axis=0)
            else:
                cut_prod_df = prod_df
        else:
            deleted_between = (
                prod_df[column_names[table]["deleted"]] > user_timestamp
            ) & (prod_df[column_names[table]["deleted"]] < chosen_timestamp)
            created_between = (
                prod_df[column_names[table]["created"]] > user_timestamp
            ) & (prod_df[column_names[table]["created"]] < chosen_timestamp)
            to_delete_in_mat = created_between & ~deleted_between
            to_add_in_mat = deleted_between & ~created_between
            if len(prod_df[created_between].index) > 0:
                cut_prod_df = prod_df.drop(prod_df[created_between].index, axis=0)
            else:
                cut_prod_df = prod_df
        # # delete those rows from materialized dataframe

        cut_prod_df = cut_prod_df.drop(crud_columns, axis=1)

        if mat_df is not None:
            created_columns = [c for c in created_columns if c not in mat_df]
            if len(created_columns) > 0:
                cut_prod_df = cut_prod_df.drop(created_columns, axis=1)

            if len(prod_df[to_delete_in_mat].index) > 0:
                mat_df = mat_df.drop(
                    prod_df[to_delete_in_mat].index, axis=0, errors="ignore"
                )
            comb_df = pd.concat([cut_prod_df, mat_df])
        else:
            comb_df = prod_df[to_add_in_mat].drop(
                columns=crud_columns, axis=1, errors="ignore"
            )
    else:
        comb_df = mat_df.drop(columns=crud_columns, axis=1, errors="ignore")

    return comb_df.reset_index()


@client_bp.expect(metadata_parser)
@client_bp.route("/datastack/<string:datastack_name>/versions")
class DatastackVersions(Resource):
    method_decorators = [
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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

        response = session.query(AnalysisVersion).filter(
            AnalysisVersion.datastack == datastack_name
        )
        args = metadata_parser.parse_args()
        if not args.get("expired"):
            response = response.filter(AnalysisVersion.valid == True)

        response = response.all()

        versions = [av.version for av in response]
        return versions, 200


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>"
)
class DatastackVersion(Resource):
    method_decorators = [
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/table/<string:table_name>/count"
)
class FrozenTableCount(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("table count", security="apikey")
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
        db_name = f"{datastack_name}__mat{version}"

        # if the database is a split database get a split model
        # and if its not get a flat model

        session = sqlalchemy_cache.get(db_name)

        mat_row_count = (
            session.query(MaterializedMetadata.row_count)
            .filter(MaterializedMetadata.table_name == table_name)
            .scalar()
        )

        return mat_row_count, 200


class CustomResource(Resource):
    @staticmethod
    def apply_decorators(*decorators):
        def wrapper(func):
            for decorator in reversed(decorators):
                func = decorator(func)
            return func

        return wrapper


@client_bp.expect(metadata_parser)
@client_bp.route("/datastack/<string:datastack_name>/metadata", strict_slashes=False)
class DatastackMetadata(Resource):
    method_decorators = [
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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
        response = session.query(AnalysisVersion).filter(
            AnalysisVersion.datastack == datastack_name
        )
        args = metadata_parser.parse_args()
        if not args.get("expired"):
            response = response.filter(AnalysisVersion.valid == True)

        response = response.all()

        if response is None:
            return "No valid versions found", 404
        schema = AnalysisVersionSchema()
        return schema.dump(response, many=True), 200


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/tables"
)
class FrozenTableVersions(Resource):
    method_decorators = [
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/tables/metadata"
)
class FrozenTablesMetadata(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("get_frozen_tables_metadata", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """get frozen tables metadata

        Args:
            datastack_name (str): datastack name
            version (int): version number


        Returns:
            dict: dictionary of table metadata
        """

        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            target_datastack
        )
        session = sqlalchemy_cache.get(aligned_volume_name)
        analysis_version, analysis_tables = get_analysis_version_and_tables(
            target_datastack, target_version, session
        )

        schema = AnalysisTableSchema()
        tables = schema.dump(analysis_tables, many=True)

        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        for table in tables:
            table_name = table["table_name"]
            ann_md = db.database.get_table_metadata(table_name)
            # the get_table_metadata function joins on the segmentationmetadata which
            # has the segmentation_table in the table_name and the annotation table name in the annotation_table
            # field.  So when we update here, we overwrite the table_name with the segmentation table name,
            # which was not the intent of the API.
            ann_table = ann_md.pop("annotation_table", None)
            if ann_table:
                ann_md["table_name"] = ann_table
            ann_md.pop("id")
            ann_md.pop("deleted")
            table.update(ann_md)

        return tables, 200


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/table/<string:table_name>/metadata"
)
class FrozenTableMetadata(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("get_frozen_table_metadata", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        table_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """get frozen table metadata

        Args:
            datastack_name (str): datastack name
            version (int): version number
            table_name (str): table name

        Returns:
            dict: dictionary of table metadata
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            target_datastack
        )
        session = sqlalchemy_cache.get(aligned_volume_name)
        analysis_version, analysis_table = get_analysis_version_and_table(
            target_datastack, table_name, target_version, session
        )

        schema = AnalysisTableSchema()
        tables = schema.dump(analysis_table)

        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        ann_md = db.database.get_table_metadata(table_name)
        # the get_table_metadata function joins on the segmentationmetadata which
        # has the segmentation_table in the table_name and the annotation table name in the annotation_table
        # field.  So when we update here, we overwrite the table_name with the segmentation table name,
        # which was not the intent of the API.
        ann_table = ann_md.pop("annotation_table", None)
        if ann_table:
            ann_md["table_name"] = ann_table
        ann_md.pop("id")
        ann_md.pop("deleted")
        tables.update(ann_md)
        return tables, 200


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/table/<string:table_name>/query"
)
class FrozenTableQuery(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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
            "desired_resolution: [x,y,z],
            "select_columns": ["column","names"],
            "filter_in_dict": {
                "tablename":{
                    "column_name":[included,values]
                }
            },
            "filter_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_spatial_dict": {
                "tablename": {
                    "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            },
            "filter_regex_dict": {
                "tablename": {
                    "column_name": "regex"
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """
        args = query_parser.parse_args()
        data = request.parsed_obj
        return handle_simple_query(
            datastack_name,
            version,
            table_name,
            target_datastack,
            target_version,
            args,
            data,
            convert_desired_resolution=True,
        )


def process_fields(df, fields, column_names, tags, bool_tags, numerical):
    for field_name, field in fields.items():
        col = column_names[field_name]

        if (
            field_name.endswith("_supervoxel_id")
            or field_name.endswith("_root_id")
            or field_name == "id"
            or field_name == "valid"
            or field_name == "target_id"
        ):
            continue

        if isinstance(field, mm_fields.String):
            if df[col].isnull().all():
                continue
            # check that this column is not all nulls
            tags.append(col)
            print(f"tag col: {col}")
        elif isinstance(field, mm_fields.Boolean):
            if df[col].isnull().all():
                continue
            df[col] = df[col].astype(bool)
            bool_tags.append(col)
            print(f"bool tag col: {col}")
        elif isinstance(field, PostGISField):
            # if all the values are NaNs skip this column
            if df[col + "_x"].isnull().all():
                continue
            numerical.append(col + "_x")
            numerical.append(col + "_y")
            numerical.append(col + "_z")
            print(f"numerical cols: {col}_(x,y,z)")
        elif isinstance(field, mm_fields.Number):
            if df[col].isnull().all():
                continue
            numerical.append(col)
            print(f"numerical col: {col}")


def process_view_columns(df, model, column_names, tags, bool_tags, numerical):
    for table_column_name, table_column in model.columns.items():
        col = column_names[model.name][table_column.key]
        if (
            table_column_name.endswith("_supervoxel_id")
            or table_column_name.endswith("_root_id")
            or table_column_name == "id"
            or table_column_name == "valid"
            or table_column_name == "target_id"
        ):
            continue

        if isinstance(table_column.type, String):
            if df[col].isnull().all():
                continue
            # check that this column is not all nulls
            tags.append(col)
            print(f"tag col: {col}")
        elif isinstance(table_column.type, Boolean):
            if df[col].isnull().all():
                continue
            df[col] = df[col].astype(bool)
            bool_tags.append(col)
            print(f"bool tag col: {col}")
        elif isinstance(table_column.type, PostGISField):
            # if all the values are NaNs skip this column
            if df[col + "_x"].isnull().all():
                continue
            numerical.append(col + "_x")
            numerical.append(col + "_y")
            numerical.append(col + "_z")
            print(f"numerical cols: {col}_(x,y,z)")
        elif isinstance(table_column.type, (Numeric, Integer, Float)):
            if df[col].isnull().all():
                continue
            numerical.append(col)
            print(f"numerical col: {col}")


def preprocess_dataframe(df, table_name, aligned_volume_name, column_names):
    db = dynamic_annotation_cache.get_db(aligned_volume_name)
    # check if this is a reference table
    table_metadata = db.database.get_table_metadata(table_name)
    schema = db.schema.get_flattened_schema(table_metadata["schema_type"])
    fields = schema._declared_fields

    if table_metadata["reference_table"]:
        ref_table = table_metadata["reference_table"]
        ref_table_metadata = db.database.get_table_metadata(ref_table)
        ref_schema = db.schema.get_flattened_schema(ref_table_metadata["schema_type"])
        ref_fields = ref_schema._declared_fields

    # find the first column that ends with _root_id using next
    try:
        root_id_col = next(
            (col for col in df.columns if col.endswith("_root_id")), None
        )
    except StopIteration:
        raise ValueError("No root_id column found in dataframe")

    # pick only the first row with each root_id
    # df = df.drop_duplicates(subset=[root_id_col])
    # drop any row with root_id =0
    df = df[df[root_id_col] != 0]

    # iterate through the columns and put them into
    # categories of 'tags' for strings, 'numerical' for numbers

    tags = []
    numerical = []
    bool_tags = []

    process_fields(df, fields, column_names[table_name], tags, bool_tags, numerical)

    if table_metadata["reference_table"]:
        process_fields(
            df,
            ref_fields,
            column_names[ref_table],
            tags,
            bool_tags,
            numerical,
        )
    # Look across the tag columns and make sure that there are no
    # duplicate string values across distinct columns
    unique_vals = {}
    for tag in tags:
        unique_vals[tag] = df[tag].unique()
        unique_vals[tag] = unique_vals[tag][~pd.isnull(unique_vals[tag])]

    if len(tags) > 0:
        # find all the duplicate values across columns
        vals, counts = np.unique(
            np.concatenate([v for v in unique_vals.values()]), return_counts=True
        )
        duplicates = vals[counts > 1]

    # iterate through the tags and replace any duplicate
    # values in the dataframe with a unique value,
    # based on preprending the column name
    for tag in tags:
        for dup in duplicates:
            if dup in unique_vals[tag]:
                df[tag] = df[tag].replace(dup, f"{tag}:{dup}")

    return df, tags, bool_tags, numerical, root_id_col


def preprocess_view_dataframe(df, view_name, db_name, column_names):
    db = dynamic_annotation_cache.get_db(db_name)
    # check if this is a reference table
    view_table = db.database.get_view_table(view_name)

    # find the first column that ends with _root_id using next
    try:
        root_id_col = next(
            (col for col in df.columns if col.endswith("_root_id")), None
        )
    except StopIteration:
        raise ValueError("No root_id column found in dataframe")

    # pick only the first row with each root_id
    # df = df.drop_duplicates(subset=[root_id_col])
    # drop any row with root_id =0
    df = df[df[root_id_col] != 0]

    # iterate through the columns and put them into
    # categories of 'tags' for strings, 'numerical' for numbers

    tags = []
    numerical = []
    bool_tags = []

    process_view_columns(df, view_table, column_names, tags, bool_tags, numerical)

    # Look across the tag columns and make sure that there are no
    # duplicate string values across distinct columns
    unique_vals = {}
    for tag in tags:
        unique_vals[tag] = df[tag].unique()
        # remove nan values from unique values
        unique_vals[tag] = unique_vals[tag][~pd.isnull(unique_vals[tag])]

    if len(unique_vals) > 0:
        # find all the duplicate values across columns
        vals, counts = np.unique(
            np.concatenate([v for v in unique_vals.values()]), return_counts=True
        )
        duplicates = vals[counts > 1]

        # iterate through the tags and replace any duplicate
        # values in the dataframe with a unique value,
        # based on preprending the column name
        for tag in tags:
            for dup in duplicates:
                if dup in unique_vals[tag]:
                    df[tag] = df[tag].replace(dup, f"{tag}:{dup}")
    return df, tags, bool_tags, numerical, root_id_col


@client_bp.expect(query_seg_prop_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/table/<string:table_name>/info"
)
class MatTableSegmentInfo(Resource):
    method_decorators = [
        cached(LRUCache(maxsize=256)),
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    def get(
        self,
        datastack_name: str,
        version: int,
        table_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for getting a segment properties object for a table

        Args:
            datastack_name (str): datastack name
            version (int): version number
            table_name (str): table name

        Returns:
            json: a precomputed json object with the segment info for this table
        """
        # check how many rows are in this table
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )

        validate_table_args([table_name], target_datastack, target_version)
        db_name = f"{datastack_name}__mat{version}"

        args = query_seg_prop_parser.parse_args()
        label_format = args.get("label_format", None)
        label_columns = args.get("label_columns", None)

        # if the database is a split database get a split model
        # and if its not get a flat model

        session = sqlalchemy_cache.get(db_name)

        mat_row_count = (
            session.query(MaterializedMetadata.row_count)
            .filter(MaterializedMetadata.table_name == table_name)
            .scalar()
        )
        if mat_row_count > current_app.config["QUERY_LIMIT_SIZE"]:
            return "Table too large to return info", 400
        else:
            db = dynamic_annotation_cache.get_db(aligned_volume_name)
            # check if this is a reference table
            table_metadata = db.database.get_table_metadata(table_name)

            if table_metadata["reference_table"]:
                ref_table = table_metadata["reference_table"]
                suffix_map = {table_name: "", ref_table: "_ref"}
                tables = [[table_name, "target_id"], [ref_table, "id"]]
                data = {
                    "tables": tables,
                    "suffix_map": suffix_map,
                    "desired_resolution": [1, 1, 1],
                }

                df, warnings, column_names = generate_complex_query_dataframe(
                    datastack_name,
                    version,
                    target_datastack,
                    target_version,
                    {},
                    data,
                    convert_desired_resolution=True,
                )

            else:
                df, warnings, column_names = generate_simple_query_dataframe(
                    datastack_name,
                    version,
                    table_name,
                    target_datastack,
                    target_version,
                    {},
                    {"desired_resolution": [1, 1, 1]},
                    convert_desired_resolution=True,
                )
            df, tags, bool_tags, numerical, root_id_col = preprocess_dataframe(
                df, table_name, aligned_volume_name, column_names
            )
            if label_columns is None:
                if label_format is None:
                    label_columns = "id"
            seg_prop = nglui.segmentprops.SegmentProperties.from_dataframe(
                df,
                id_col=root_id_col,
                tag_value_cols=tags,
                tag_bool_cols=bool_tags,
                number_cols=numerical,
                label_col=label_columns,
                label_format_map=label_format,
            )
            dfjson = json.dumps(seg_prop.to_dict(), cls=current_app.json_encoder)
            response = Response(dfjson, status=200, mimetype="application/json")
            return after_request(response)


@client_bp.expect(query_seg_prop_parser)
@client_bp.route("/datastack/<string:datastack_name>/table/<string:table_name>/info")
class MatTableSegmentInfoLive(Resource):
    method_decorators = [
        cached(TTLCache(maxsize=256, ttl=120)),
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    def get(
        self,
        datastack_name: str,
        table_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for getting a segment properties object for a table
           based on a live query
        Args:
            datastack_name (str): datastack name
            table_name (str): table name

        Returns:
            json: a precomputed json object with the segment info for this table
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        # check if this is a reference table
        table_metadata = db.database.get_table_metadata(table_name)

        user_data = {
            "table": table_name,
            "timestamp": datetime.datetime.now(tz=pytz.utc),
            "suffixes": {table_name: ""},
            "desired_resolution": [1, 1, 1],
        }

        if table_metadata["reference_table"]:
            ref_table = table_metadata["reference_table"]
            user_data["suffixes"][ref_table] = "_ref"
            user_data["joins"] = [[table_name, "target_id", ref_table, "id"]]

        return_vals = assemble_live_query_dataframe(
            user_data, datastack_name=datastack_name, args={}
        )
        df, column_names, _, _, _ = return_vals

        vals = preprocess_dataframe(df, table_name, aligned_volume_name, column_names)
        df, tags, bool_tags, numerical, root_id_col = vals

        # parse the args
        args = query_seg_prop_parser.parse_args()
        label_format = args.get("label_format", None)
        label_columns = args.get("label_columns", None)
        if label_format is None:
            if label_columns is None:
                label_columns = "id"

        seg_prop = nglui.segmentprops.SegmentProperties.from_dataframe(
            df,
            id_col=root_id_col,
            tag_value_cols=tags,
            tag_bool_cols=bool_tags,
            number_cols=numerical,
            label_col=label_columns,
            label_format_map=label_format,
        )
        dfjson = json.dumps(seg_prop.to_dict(), cls=current_app.json_encoder)
        response = Response(dfjson, status=200, mimetype="application/json")
        return after_request(response)


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/query"
)
class FrozenQuery(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

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
            },
            "filter_greater_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_spatial_dict": {
                "tablename":{
                    "column_name":[[min_x,min_y,minz], [max_x_max_y_max_z]]
                }
            },
            "filter_regex_dict": {
                "tablename":{
                    "column_name": "regex"
                }
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """

        args = query_parser.parse_args()
        data = request.parsed_obj
        return handle_complex_query(
            datastack_name,
            version,
            target_datastack,
            target_version,
            args,
            data,
            convert_desired_resolution=True,
        )


@client_bp.route(
    "/datastack/<string:datastack_name>/table/<string:table_name>/unique_string_values"
)
class TableUniqueStringValues(Resource):
    method_decorators = [
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("get_unique_string_values", security="apikey")
    def get(self, datastack_name: str, table_name: str):
        """get unique string values for a table

        Args:
            datastack_name (str): datastack name
            table_name (str): table name

        Returns:
            list: list of unique string values
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        db = dynamic_annotation_cache.get_db(aligned_volume_name)
        unique_values = db.database.get_unique_string_values(table_name)
        return unique_values, 200


def assemble_live_query_dataframe(user_data, datastack_name, args):
    user_data["limit"] = min(
        current_app.config["QUERY_LIMIT_SIZE"],
        user_data.get("limit", current_app.config["QUERY_LIMIT_SIZE"]),
    )
    past_ver, future_ver, aligned_vol = get_closest_versions(
        datastack_name, user_data["timestamp"]
    )
    db = dynamic_annotation_cache.get_db(aligned_vol)
    check_read_permission(db, user_data["table"])
    allow_invalid_root_ids = args.get("allow_invalid_root_ids", False)
    # TODO add table owner warnings
    # if has_joins:
    #    abort(400, "we are not supporting joins yet")
    # if future_ver is None and has_joins:
    #    abort(400, 'we do not support joins when there is no future version')
    # elif has_joins:
    #     # run a future to past map version of the query
    #     check_joins(joins)
    #     chosen_version = future_ver
    if (past_ver is None) and (future_ver is None):
        abort(
            400,
            "there is no future or past version for this timestamp, is materialization broken?",
        )
    elif past_ver is not None:
        chosen_version = past_ver
    else:
        chosen_version = future_ver

    chosen_timestamp = pytz.utc.localize(chosen_version.time_stamp)

    # map public version datastacks to their private versions
    if chosen_version.parent_version is not None:
        target_datastack = chosen_version.datastack
        session = sqlalchemy_cache.get(aligned_vol)
        target_version = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.id == chosen_version.parent_version)
            .one()
        )
        datastack_name = target_version.datastack

        # query the AnalysisVersion with the oldest timestamp
        newest_version = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == target_datastack)
            .order_by(AnalysisVersion.time_stamp.desc())
            .first()
        )

        # if the users timestamp is newer than the newest version
        # then we set the users timestamp to the newest version
        if user_data["timestamp"] > pytz.utc.localize(newest_version.time_stamp):
            user_data["timestamp"] = pytz.utc.localize(newest_version.time_stamp)

    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)
    cg_client = chunkedgraph_cache.get_client(pcg_table_name)

    meta_db = dynamic_annotation_cache.get_db(aligned_volume_name)
    md = meta_db.database.get_table_metadata(user_data["table"])
    if not user_data.get("desired_resolution", None):
        des_res = [
            md["voxel_resolution_x"],
            md["voxel_resolution_y"],
            md["voxel_resolution_z"],
        ]
        user_data["desired_resolution"] = des_res

    modified_user_data, query_map, remap_warnings = remap_query(
        user_data,
        chosen_timestamp,
        cg_client,
        allow_invalid_root_ids,
    )

    mat_df, column_names, mat_warnings = execute_materialized_query(
        datastack_name,
        aligned_volume_name,
        chosen_version.version,
        pcg_table_name,
        modified_user_data,
        query_map,
        cg_client,
        random_sample=args.get("random_sample", None),
        split_mode=not chosen_version.is_merged,
    )

    last_modified = pytz.utc.localize(md["last_modified"])
    if (last_modified > chosen_timestamp) or (last_modified > user_data["timestamp"]):
        prod_df, column_names, prod_warnings = execute_production_query(
            aligned_volume_name,
            pcg_table_name,
            user_data,
            chosen_timestamp,
            cg_client,
            args.get("allow_missing_lookups", True),
        )
    else:
        prod_df = None
        prod_warnings = []

    df = combine_queries(mat_df, prod_df, chosen_version, user_data, column_names)
    df = apply_filters(df, user_data, column_names)
    return df, column_names, mat_warnings, prod_warnings, remap_warnings


@client_bp.expect(query_parser)
@client_bp.route("/datastack/<string:datastack_name>/query")
class LiveTableQuery(Resource):
    method_decorators = [
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("v2_query", security="apikey")
    @accepts("V2QuerySchema", schema=V2QuerySchema, api=client_bp)
    def post(self, datastack_name: str):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            table_name (str): table names

        Payload:
        All values are optional.  Limit has an upper bound set by the server.
        Consult the schema of the table for column names and appropriate values
        {
            "table":"table_name",
            "joins":[[table_name, table_column, joined_table, joined_column],
                     [joined_table, joincol2, third_table, joincol_third]]
            "timestamp": datetime.datetime.utcnow(),
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
            },
            "filter_greater_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_spatial_dict": {
                "table_name": {
                "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            },
            "filter_regex_dict":{
                "table_name":{
                    "column_name": "regex"
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """
        args = query_parser.parse_args()
        user_data = request.parsed_obj
        return_vals = assemble_live_query_dataframe(user_data, datastack_name, args)

        df, column_names, mat_warnings, prod_warnings, remap_warnings = return_vals
        return create_query_response(
            df,
            warnings=remap_warnings + mat_warnings + prod_warnings,
            column_names=column_names,
            desired_resolution=user_data["desired_resolution"],
            return_pyarrow=args["return_pyarrow"],
            arrow_format=args["arrow_format"],
            ipc_compress=args["ipc_compress"],
        )


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/views"
)
class AvailableViews(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("available_views", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        target_datastack: str = None,
        target_version: int = None,
    ) -> List[AnalysisViewSchema]:
        """endpoint for getting available views

        Args:
            datastack_name (str): datastack name

        Returns:
            dict: a dictionary of views
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        if version == 0:
            mat_db_name = f"{aligned_volume_name}"
        else:
            mat_db_name = f"{datastack_name}__mat{version}"

        meta_db = dynamic_annotation_cache.get_db(mat_db_name)
        views = meta_db.database.get_views(datastack_name)
        views = AnalysisViewSchema().dump(views, many=True)
        view_d = {}
        for view in views:
            name = view.pop("table_name")
            view_d[name] = view
        return view_d


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/views/<string:view_name>/metadata"
)
class ViewMetadata(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("view_metadata", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        view_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ) -> AnalysisViewSchema:
        """endpoint for getting metadata about a view

        Args:
            datastack_name (str): datastack name
            view_name (str): table names

        Returns:
            dict: a dictionary of metadata about the view
        """

        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )
        if version == 0:
            mat_db_name = f"{aligned_volume_name}"
        else:
            mat_db_name = f"{datastack_name}__mat{version}"

        meta_db = dynamic_annotation_cache.get_db(mat_db_name)
        md = meta_db.database.get_view_metadata(datastack_name, view_name)

        return md


def assemble_view_dataframe(datastack_name, version, view_name, data, args):
    """
    Assemble a dataframe from a view
    Args:
        datastack_name (str): datastack name
        version (int): version number
        view_name (str): view name
        data (dict): query data
        args (dict): query arguments
    Returns:
        pd.DataFrame: dataframe
        list: column names
        list: warnings
    """

    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(datastack_name)

    if version == 0:
        mat_db_name = f"{aligned_volume_name}"
    else:
        mat_db_name = f"{datastack_name}__mat{version}"

    # check_read_permission(db, table_name)

    max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

    limit = data.get("limit", max_limit)
    if limit > max_limit:
        limit = max_limit

    get_count = args.get("count", False)
    if get_count:
        limit = None

    mat_db = dynamic_annotation_cache.get_db(mat_db_name)
    md = mat_db.database.get_view_metadata(datastack_name, view_name)

    if not data.get("desired_resolution", None):
        des_res = [
            md["voxel_resolution_x"],
            md["voxel_resolution_y"],
            md["voxel_resolution_z"],
        ]
        data["desired_resolution"] = des_res

    qm = QueryManager(
        mat_db_name,
        segmentation_source=pcg_table_name,
        split_mode=False,
        limit=limit,
        offset=data.get("offset", 0),
        get_count=get_count,
    )
    qm.add_view(datastack_name, view_name)
    qm.apply_filter(data.get("filter_in_dict", None), qm.apply_isin_filter)
    qm.apply_filter(data.get("filter_out_dict", None), qm.apply_notequal_filter)
    qm.apply_filter(data.get("filter_equal_dict", None), qm.apply_equal_filter)
    qm.apply_filter(data.get("filter_greater_dict", None), qm.apply_greater_filter)
    qm.apply_filter(data.get("filter_less_dict", None), qm.apply_less_filter)
    qm.apply_filter(data.get("filter_greater_equal_dict", None), qm.apply_greater_equal_filter)
    qm.apply_filter(data.get("filter_less_equal_dict", None), qm.apply_less_equal_filter)
    qm.apply_filter(data.get("filter_spatial_dict", None), qm.apply_spatial_filter)
    qm.apply_filter(data.get("filter_regex_dict", None), qm.apply_regex_filter)
    select_columns = data.get("select_columns", None)
    if select_columns:
        for column in select_columns:
            qm.select_column(view_name, column)
    else:
        qm.select_all_columns(view_name)

    df, column_names = qm.execute_query(desired_resolution=data["desired_resolution"])
    df.drop(columns=["deleted", "superceded"], inplace=True, errors="ignore")
    warnings = []
    current_app.logger.info("query: {}".format(data))
    current_app.logger.info("args: {}".format(args))
    user_id = str(g.auth_user["id"])
    current_app.logger.info(f"user_id: {user_id}")

    if len(df) == limit:
        warnings.append(f'201 - "Limited query to {limit} rows')
    warnings = update_notice_text_warnings(md, warnings, view_name)

    return df, column_names, warnings


# Define your cache (LRU Cache with a maximum size of 100 items)
view_mat_cache = LRUCache(maxsize=100)
view_live_cache = TTLCache(maxsize=100, ttl=60)
latest_mat_cache = TTLCache(maxsize=100, ttl=60 * 60 * 12)


def conditional_view_cache(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Generate a cache key
        key = request.url
        if kwargs.get("version") == -1:
            # Check if the result is in the live cache
            if key in view_live_cache:
                return view_live_cache[key]
            else:
                result = func(*args, **kwargs)
                view_live_cache[key] = result
                return result

        # Check if the 'version' argument is in the kwargs and if it is set to 0
        if kwargs.get("version") == 0:
            # Check if the result is in the live cache
            if key in view_live_cache:
                return view_live_cache[key]
            else:
                result = func(*args, **kwargs)
                view_live_cache[key] = result
                return result
        else:
            # Check if the result is in the materialized cache
            if key in view_mat_cache:
                return view_mat_cache[key]
            else:
                result = func(*args, **kwargs)
                view_mat_cache[key] = result
                return result

    return wrapper


@client_bp.expect(query_seg_prop_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/view/<string:view_name>/info"
)
class MatViewSegmentInfo(Resource):
    method_decorators = [
        conditional_view_cache,
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    def get(
        self,
        datastack_name: str,
        version: int,
        view_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for getting a segment properties object for a view

        Args:
            datastack_name (str): datastack name
            version (int): version number
            view_name (str): view name

        Returns:
            json: a precomputed json object with the segment info for this view
        """
        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )

        if version == -1:
            version = get_latest_version(datastack_name)
            print(f"using version {version}")
        mat_db_name = f"{datastack_name}__mat{version}"
        if version == 0:
            mat_db_name = f"{aligned_volume_name}"

        df, column_names, warnings = assemble_view_dataframe(
            datastack_name, version, view_name, {}, {}
        )

        df, tags, bool_tags, numerical, root_id_col = preprocess_view_dataframe(
            df, view_name, mat_db_name, column_names
        )

        args = query_seg_prop_parser.parse_args()
        label_format = args.get("label_format", None)
        label_columns = args.get("label_columns", None)
        if label_format is None:
            if label_columns is None:
                label_columns = df.columns[0]

        seg_prop = nglui.segmentprops.SegmentProperties.from_dataframe(
            df,
            id_col=root_id_col,
            tag_value_cols=tags,
            tag_bool_cols=bool_tags,
            number_cols=numerical,
            label_col=label_columns,
            label_format_map=label_format,
        )
        # use the current_app encoder to encode the seg_prop.to_dict()
        # to ensure that the json is serialized correctly
        dfjson = json.dumps(seg_prop.to_dict(), cls=current_app.json_encoder)
        response = Response(dfjson, status=200, mimetype="application/json")
        return after_request(response)


@client_bp.expect(query_parser)
@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/views/<string:view_name>/query"
)
class ViewQuery(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("view_query", security="apikey")
    @validate_datastack
    @accepts("SimpleQuerySchema", schema=SimpleQuerySchema, api=client_bp)
    def post(
        self,
        datastack_name: str,
        version: int,
        view_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for doing a query with filters

        Args:
            datastack_name (str): datastack name
            version (int): version number
            view_name (str): table names


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
            "desired_resolution: [x,y,z],
            "select_columns": ["column","names"],
            "filter_in_dict": {
                "tablename":{
                    "column_name":[included,values]
                }
            },
            "filter_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_greater_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_less_equal_dict": {
                "tablename":{
                    "column_name":value
                }
            },
            "filter_spatial_dict": {
                "tablename": {
                "column_name": [[min_x, min_y, min_z], [max_x, max_y, max_z]]
            },
            "filter_regex_dict": {
                "tablename": {
                    "column_name": "regex"
                }
            }
        }
        Returns:
            pyarrow.buffer: a series of bytes that can be deserialized using pyarrow.deserialize
        """
        args = query_parser.parse_args()
        data = request.parsed_obj
        df, column_names, warnings = assemble_view_dataframe(
            datastack_name, version, view_name, data, args
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


def get_table_schema(table):
    """
    Get the schema of a table as a jsonschema
    Args:
        table (Table): sqlalchemy table object
    Returns:
        dict: jsonschema of the table

    """
    properties = {}

    for column in table.columns:
        column_type = None
        format = None
        if isinstance(column.type, String):
            column_type = "string"
        elif isinstance(column.type, Integer):
            column_type = "integer"
        elif isinstance(column.type, Float):
            column_type = "float"
        elif isinstance(column.type, DateTime):
            column_type = "string"
            format = "date-time"
        elif isinstance(column.type, Boolean):
            column_type = "boolean"
        elif isinstance(column.type, Geometry):
            column_type = "SpatialPoint"
        elif isinstance(column.type, Numeric):
            column_type = "number"
        else:
            raise ValueError(f"Unsupported column type: {column.type}")

        properties[column.name] = {"type": column_type}
        if format:
            properties[column.name]["format"] = format

    return properties


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/views/<string:view_name>/schema"
)
class ViewSchema(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("view_schema", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        view_name: str,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for getting schema about a view

        Args:
            datastack_name (str): datastack name
            version (int): version number
            view_name (str): table names

        Returns:
            dict: a jsonschema of the view
        """

        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )

        if version == 0:
            mat_db_name = f"{aligned_volume_name}"
        else:
            mat_db_name = f"{datastack_name}__mat{version}"

        meta_db = dynamic_annotation_cache.get_db(mat_db_name)
        table = meta_db.database.get_view_table(view_name)

        return get_table_schema(table)


@client_bp.route(
    "/datastack/<string:datastack_name>/version/<int(signed=True):version>/views/schemas"
)
class ViewSchemas(Resource):
    method_decorators = [
        validate_datastack,
        limit_by_category("fast_query"),
        auth_requires_permission("view", table_arg="datastack_name"),
        reset_auth,
    ]

    @client_bp.doc("view_schemas", security="apikey")
    def get(
        self,
        datastack_name: str,
        version: int,
        target_datastack: str = None,
        target_version: int = None,
    ):
        """endpoint for getting view schemas

        Args:
            datastack_name (str): datastack name
            version (int): version number

        Returns:
            dict: a dict of jsonschemas of each view
        """

        aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
            datastack_name
        )

        if version == 0:
            mat_db_name = f"{aligned_volume_name}"
        else:
            mat_db_name = f"{datastack_name}__mat{version}"

        meta_db = dynamic_annotation_cache.get_db(mat_db_name)
        views = meta_db.database.get_views(datastack_name)
        schemas = {}
        for view in views:
            view_name = view.table_name
            table = meta_db.database.get_view_table(view_name)
            schemas[view_name] = get_table_schema(table)
        return schemas
