import datetime
import numpy as np
from flask import abort
from copy import deepcopy
import pandas as pd
import logging
from typing import Iterable
from collections import defaultdict


def update_rootids(
    df: pd.DataFrame,
    timestamp: datetime.datetime,
    future_map: dict,
    cg_client,
    allow_missing_lookups: bool = False,
):
    # post process the dataframe to update all the root_ids columns
    # with the most up to date get roots
    if len(future_map) == 0:
        future_map = None

    sv_columns = [c for c in df.columns if c.endswith("supervoxel_id")]

    all_root_ids = np.empty(0, dtype=np.int64)
    warnings = []
    # go through the columns and collect all the root_ids to check
    # to see if they need updating
    for sv_col in sv_columns:
        num_ones = np.sum(df[sv_col] == 1)
        if num_ones > 0:
            msg = f"There are {num_ones} annotations that need supervoxel lookups, wait till new annotation ingest is done or pick an less recent timestamp"
            if allow_missing_lookups:
                warnings.append(msg)
                not_ones = df[sv_col] != 1
                df = df[not_ones]
            else:
                abort(406, msg)

        root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
        # use the future map to update rootIDs
        if future_map is not None:
            df[root_id_col].replace(future_map, inplace=True)
        all_root_ids = np.append(all_root_ids, df[root_id_col].values.copy())

    uniq_root_ids = np.unique(all_root_ids)

    del all_root_ids
    uniq_root_ids = uniq_root_ids[uniq_root_ids != 0]
    # logging.info(f"uniq_root_ids {uniq_root_ids}")

    is_latest_root = cg_client.is_latest_roots(uniq_root_ids, timestamp=timestamp)
    latest_root_ids = uniq_root_ids[is_latest_root]
    latest_root_ids = np.concatenate([[0], latest_root_ids])

    # go through the columns and collect all the supervoxel ids to update
    all_svids = np.empty(0, dtype=np.int64)
    all_is_latest = []
    all_svid_lengths = []
    for sv_col in sv_columns:
        root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
        svids = df[sv_col].values
        root_ids = df[root_id_col]
        is_latest_root = np.isin(root_ids, latest_root_ids)
        all_is_latest.append(is_latest_root)
        n_svids = len(svids[~is_latest_root])
        all_svid_lengths.append(n_svids)
        logging.info(f"{sv_col} has {n_svids} to update")
        all_svids = np.append(all_svids, svids[~is_latest_root])
    logging.info(f"num zero svids: {np.sum(all_svids==0)}")
    logging.info(f"all_svids dtype {all_svids.dtype}")
    logging.info(f"all_svid_lengths {all_svid_lengths}")

    # find the up to date root_ids for those supervoxels
    updated_root_ids = cg_client.get_roots(all_svids, timestamp=timestamp)
    del all_svids

    # loop through the columns again replacing the root ids with their updated
    # supervoxelids
    k = 0
    for is_latest_root, n_svids, sv_col in zip(
        all_is_latest, all_svid_lengths, sv_columns
    ):
        root_id_col = sv_col[: -len("supervoxel_id")] + "root_id"
        root_ids = df[root_id_col].values.copy()

        uroot_id = updated_root_ids[k : k + n_svids]
        k += n_svids
        root_ids[~is_latest_root] = uroot_id
        # ran into an isssue with pyarrow producing read only columns
        df[root_id_col] = None
        df[root_id_col] = root_ids

    return df, warnings


def strip_root_id_filters(user_data):
    modified_user_data = deepcopy(user_data)

    def strip_filter(filter):
        to_pop = []
        if modified_user_data.get(filter, None):
            for table in modified_user_data.get(filter):
                for k in modified_user_data[filter][table]:
                    if k.endswith("_root_id"):
                        to_pop.append((table, k))
        for table, k in to_pop:
            modified_user_data[filter][table].pop(k)

    strip_filter("filter_in_dict")
    strip_filter("filter_out_dict")
    strip_filter("filter_equal_dict")
    strip_filter("filter_greater_dict")
    strip_filter("filter_less_dict")
    strip_filter("filter_greater_equal_dict")
    strip_filter("filter_less_equal_dict")
    return modified_user_data


def remap_query(user_data, mat_timestamp, cg_client, allow_invalid_root_ids=False):

    query_timestamp = user_data["timestamp"]

    # map filters from the user timestamp to the materialized timestamp
    new_filters, query_map, warnings = map_filters(
        [
            user_data.get("filter_in_dict", None),
            user_data.get("filter_out_dict", None),
            user_data.get("filter_equal_dict", None),
            user_data.get("filter_greater_dict", None),
            user_data.get("filter_less_dict", None),
            user_data.get("filter_greater_equal_dict", None),
            user_data.get("filter_less_equal_dict", None),
        ],
        query_timestamp,
        mat_timestamp,
        cg_client,
        allow_invalid_root_ids,
    )

    new_filter_in_dict, new_filter_out_dict, new_equal_dict, new_greater_dict, new_less_dict, new_greater_equal_dict, new_less_equal_dict = new_filters
    if new_equal_dict is not None:
        if new_filter_in_dict is None:
            new_filter_in_dict = defaultdict(lambda: None)
        # when doing a filter equal in the past
        # we translate it to a filter_in, as 1 ID might
        # be multiple IDs in the past.
        # so we want to update the filter_in dict
        for table, filter in new_equal_dict.items():
            cols = [col for col in filter.keys()]
            for col in cols:
                if col.endswith("root_id"):
                    # if col is iterable move it over
                    if isinstance(new_equal_dict[table][col], Iterable):
                        if new_filter_in_dict[table] is None:
                            new_filter_in_dict[table] = defaultdict(lambda: None)
                        new_filter_in_dict[table][col] = new_equal_dict[table].pop(col)
            if len(new_equal_dict[table]) == 0:
                new_equal_dict = None

    modified_user_data = deepcopy(user_data)
    modified_user_data["filter_equal_dict"] = new_equal_dict
    modified_user_data["filter_greater_dict"] = new_greater_dict
    modified_user_data["filter_less_dict"] = new_less_dict
    modified_user_data["filter_greater_equal_dict"] = new_greater_equal_dict
    modified_user_data["filter_less_equal_dict"] = new_less_equal_dict
    modified_user_data["filter_in_dict"] = new_filter_in_dict
    modified_user_data["filter_out_dict"] = new_filter_out_dict

    return modified_user_data, query_map, warnings


def map_filters(
    filters,
    timestamp_query: datetime,
    timestamp_mat: datetime,
    cg_client,
    allow_invalid_root_ids=False,
):
    """translate a list of filter dictionaries
       from a point in the future, to a point in the past

    Args:
        filters (list[dict]): filter dictionaries with
        timestamp ([datetime.datetime]): timestamp you want to query with
        timestamp_past ([datetime.datetime]): timestamp rootids are passed in

    Returns:
        new_filters: list[dict]
            filters translated to timestamp past
        future_id_map: dict
            mapping from passed IDs to the IDs in the past
    """

    new_filters = []
    root_ids = []
    for filter in filters:
        if filter is not None:
            for table, filter_dict in filter.items():
                if filter_dict is not None:
                    for col, val in filter_dict.items():
                        if col.endswith("root_id"):
                            if not isinstance(val, (Iterable, np.ndarray)):
                                root_ids.append([val])
                            else:
                                root_ids.append(val)

    # if there are no root_ids then we can safely return now
    if len(root_ids) == 0:
        return filters, {}, []
    root_ids = np.unique(np.concatenate(root_ids))
    is_valid_at_query = cg_client.is_latest_roots(root_ids, timestamp=timestamp_query)
    warnings = []
    invalid_roots = root_ids[~is_valid_at_query]
    if not np.all(is_valid_at_query):
        if not allow_invalid_root_ids:
            abort(
                400,
                f"Some root_ids passed are not valid at the query timestamp: {invalid_roots}",
            )
        else:
            warnings.append(
                f"Some root_ids passed are not valid at the query timestamp: {invalid_roots}"
            )
            root_ids = root_ids[is_valid_at_query]

    # is_valid_at_mat = cg_client.is_latest_roots(root_ids, timestamp=timestamp_mat)
    if len(root_ids) == 0:
        raise ValueError(
            f"ALL root id filters passed ({invalid_roots}) \
                         were invalid at timestamp {timestamp_query}. \
                         At least one root id must be valid when using \
                         `allow_invalid_root_ids=True`"
        )
    if timestamp_mat < timestamp_query:
        id_mapping = cg_client.get_past_ids(
            root_ids, timestamp_past=timestamp_mat, timestamp_future=timestamp_query
        )
        mat_map_str = "past_id_map"
        query_map_str = "future_id_map"

    elif timestamp_query > timestamp_mat:
        id_mapping = cg_client.get_past_ids(
            root_ids, timestamp_past=timestamp_query, timestamp_future=timestamp_mat
        )
        mat_map_str = "future_id_map"
        query_map_str = "past_id_map"
    else:
        return filters, {}, warnings

    if len(id_mapping[mat_map_str]) == 0:
        return filters, {}, warnings

    for filter in filters:
        if filter is not None:
            new_filter = defaultdict(lambda: None)
            for table, filter_dict in filter.items():
                if filter_dict is None:
                    new_filter[table] = defaultdict(lambda: None)
                else:
                    new_filter[table] = defaultdict(lambda: None)
                    for col, root_ids_filt in filter_dict.items():
                        if allow_invalid_root_ids:
                            root_ids_filt = np.array(root_ids_filt)[
                                np.isin(root_ids_filt, root_ids)
                            ]
                        if col.endswith("root_id"):
                            if not isinstance(root_ids_filt, (Iterable, np.ndarray)):
                                new_filter[table][col] = id_mapping[mat_map_str].get(
                                    root_ids_filt, np.empty(dtype=np.int64, shape=0)
                                )
                            else:
                                new_filter[table][col] = np.concatenate(
                                    [
                                        id_mapping[mat_map_str].get(
                                            v, np.empty(dtype=np.int64, shape=0)
                                        )
                                        for v in root_ids_filt
                                    ],
                                )
                        else:
                            new_filter[table][col] = root_ids_filt
            new_filters.append(new_filter)
        else:
            new_filters.append(None)
    return new_filters, id_mapping[query_map_str], warnings


# @cached(cache=TTLCache(maxsize=64, ttl=600))
# def get_relevant_datastack_info(datastack_name):
#     ds_info = get_datastack_info(datastack_name=datastack_name)
#     seg_source = ds_info["segmentation_source"]
#     pcg_table_name = seg_source.split("/")[-1]
#     aligned_volume_name = ds_info["aligned_volume"]["name"]
#     return aligned_volume_name, pcg_table_name


#    aligned_volume_name, pcg_table_name = get_relevant_datastack_info(
#             datastack_name
#         )
#         args = query_parser.parse_args()
#         Session = sqlalchemy_cache.get(aligned_volume_name)
#         time_d = {}
#         now = time.time()

#         timestamp = args["timestamp"]

#         engine = sqlalchemy_cache.get_engine(aligned_volume_name)
#         time_d["get engine"] = time.time() - now
#         now = time.time()
#         max_limit = current_app.config.get("QUERY_LIMIT_SIZE", 200000)

#         data = request.parsed_obj
#         time_d["get data"] = time.time() - now
#         now = time.time()
#         limit = data.get("limit", max_limit)

#         if limit > max_limit:
#             limit = max_limit

#         get_count = args.get("count", False)

#         if get_count:
#             limit = None

#         logging.info("query {}".format(data))
#         logging.info("args - {}".format(args))
#         Session = sqlalchemy_cache.get(aligned_volume_name)

#         # find the analysis version that we use to support this timestamp
#         base_analysis_version = (
#             Session.query(AnalysisVersion)
#             .filter(AnalysisVersion.datastack == datastack_name)
#             .filter(AnalysisVersion.valid == True)
#             .filter(AnalysisVersion.time_stamp < str(timestamp))
#             .order_by(AnalysisVersion.time_stamp.desc())
#             .first()
#         )
#         if base_analysis_version is None:
#             return "No version available to support that timestamp", 404

#         timestamp_start = base_analysis_version.time_stamp
#         # check if the table is in this timestamp
#         analysis_table = (
#             Session.query(AnalysisTable)
#             .filter(AnalysisTable.analysisversion_id == base_analysis_version.id)
#             .filter(AnalysisTable.valid == True)
#             .filter(AnalysisTable.table_name == table_name)
#             .first()
#         )
#         filter_in_dict = data.get("filter_in_dict", None)
#         filter_out_dict = data.get("filter_notin_dict", None)
#         filter_equal_dict = data.get("filter_equal_dict", None)
#         filter_greater_dict = data.get("filter_greater_dict", None)
#         filter_less_dict = data.get("filter_less_dict", None)
#         filter_greater_equal_dict = data.get("filter_greater_equal_dict", None)
#         filter_less_equal_dict = data.get("filter_less_equal_dict", None)

#         db = dynamic_annotation_cache.get_db(aligned_volume_name)
#         table_metadata = db.database.get_table_metadata(table_name)
#         cg_client = chunkedgraph_cache.init_pcg(pcg_table_name)
#         if analysis_table is None:
#             df_mat = None
#         else:
#             # if it is translate the query parameters to that moment in time

#             version = base_analysis_version.version
#             mat_engine = sqlalchemy_cache.get_engine(
#                 "{}__mat{}".format(datastack_name, version)
#             )
#             Model = get_flat_model(datastack_name, table_name, version, Session)
#             time_d["get Model"] = time.time() - now
#             now = time.time()

#             MatSession = sqlalchemy_cache.get(
#                 "{}__mat{}".format(datastack_name, version)
#             )

#             past_filters, future_map = map_filters(
#                 [filter_in_dict, filter_out_dict, filter_equal_dict, filter_greater_dict, filter_less_dict, filter_greater_equal_dict, filter_less_equal_dict],
#                 timestamp,
#                 timestamp_start,
#                 cg_client,
#             )
#             past_filter_in_dict, past_filter_out_dict, past_equal_dict = past_filters
#             if past_equal_dict is not None:
#                 # when doing a filter equal in the past
#                 # we translate it to a filter_in, as 1 ID might
#                 # be multiple IDs in the past.
#                 # so we want to update the filter_in dict
#                 cols = [col for col in past_equal_dict.keys()]
#                 for col in cols:
#                     if col.endswith("root_id"):
#                         if past_filter_in_dict is None:
#                             past_filter_in_dict = {}
#                         past_filter_in_dict[col] = past_equal_dict.pop(col)
#                 if len(past_equal_dict) == 0:
#                     past_equal_dict = None

#             time_d["get MatSession"] = time.time() - now
#             now = time.time()

#             # todo fix reference annotations
#             # tables, suffixes = self._resolve_merge_reference(
#             #     True, table_name, datastack_name, version
#             # )
#             # model_dict = {}
#             # for table_desc in data["tables"]:
#             #     table_name = table_desc[0]
#             #     Model = get_flat_model(datastack_name, table_name, version, Session)
#             #     model_dict[table_name] = Model
#             if table_metadata["reference_table"] is None:
#                 model_dict = {table_name: Model}
#                 tables = [table_name]
#                 suffixes = None
#             else:
#                 ref_table = table_metadata["reference_table"]
#                 RefModel = get_flat_model(datastack_name, ref_table, version, Session)
#                 model_dict = {table_name: Model, ref_table: RefModel}
#                 tables = [[table_name, "target_id"], [ref_table, "id"]]
#                 suffixes = ["", "_target"]
#             print("doing mat part")
#             df_mat = specific_query(
#                 MatSession,
#                 mat_engine,
#                 model_dict=model_dict,
#                 tables=tables,
#                 filter_in_dict=past_filter_in_dict,
#                 filter_notin_dict=past_filter_out_dict,
#                 filter_equal_dict=None,
#                 filter_greater_dict=None,
#                 filter_less_dict=None,
#                 filter_greater_equal_dict=None,
#                 filter_less_equal_dict=None,
#                 filter_spatial=data.get("filter_spatial_dict", None),
#                 select_columns=data.get("select_columns", None),
#                 consolidate_positions=False,
#                 offset=data.get("offset", None),
#                 limit=limit,
#                 get_count=get_count,
#                 suffixes=suffixes,
#                 use_pandas=True,
#             )
#             df_mat = _update_rootids(df_mat, timestamp, future_map, cg_client)
#             print("done mat part")
#         print("doing live query part")
#         AnnModel, AnnSegModel, seg_model_valid = get_split_models(
#             datastack_name,
#             table_name,
#             with_crud_columns=True,
#         )

#         time_d["get Model"] = time.time() - now
#         now = time.time()
#         if table_metadata["reference_table"] is None:
#             # get the production database with a timestamp interval query
#             f1 = AnnModel.created.between(str(timestamp_start), str(timestamp))
#             f2 = AnnModel.deleted.between(str(timestamp_start), str(timestamp))
#             f = (or_(f1, f2),)
#             time_filter_args = [f]
#             seg_table = f"{table_name}__{datastack_name}"
#             model_dict = {table_name: AnnModel, seg_table: AnnSegModel}
#             tables = [[table_name, "id"], [seg_table, "id"]]
#             suffixes = ["", "_seg"]
#         else:
#             ref_table = table_metadata["reference_table"]
#             RefModel, RefSegModel, ref_seg_model_valid = get_split_models(
#                 datastack_name,
#                 ref_table,
#                 with_crud_columns=True,
#             )
#             ref_seg_table = f"{ref_table}__{datastack_name}"
#             seg_table = f"{table_name}__{datastack_name}"
#             model_dict = {table_name: AnnModel, ref_table: RefModel}

#             if ref_seg_model_valid:
#                 model_dict[ref_seg_table] = RefSegModel
#                 tables.append([ref_seg_table, "id"])
#             if seg_model_valid:
#                 model_dict[seg_table] = AnnSegModel

#             ann_table.id
#             ann_seg_table.id

#             ann_table.target_id
#             referenced_table.id
#             referenced_seg_table.id

#             suffixes = ["", "_target", "_target_seg"]
#             f1 = RefModel.created.between(str(timestamp_start), str(timestamp))
#             f2 = RefModel.deleted.between(str(timestamp_start), str(timestamp))
#             f = (or_(f1, f2),)
#             time_filter_args = [f]

#         time_d["setup query"] = time.time() - now
#         now = time.time()

#         df_new = specific_query(
#             Session,
#             engine,
#             model_dict=model_dict,
#             tables=tables,
#             filter_in_dict=_format_filter(data.get("filter_in_dict", None), table_name),
#             filter_notin_dict=_format_filter(
#                 data.get("filter_notin_dict", None), table_name
#             ),
#             filter_equal_dict=_format_filter(filter_equal_dict, table_name),
#             filter_greater_dict=_format_filter(filter_greater_dict, table_name),
#             filter_less_dict=_format_filter(filter_less_dict, table_name),
#             filter_greater_equal_dict=_format_filter(filter_greater_equal_dict, table_name),
#             filter_less_equal_dict=_format_filter(filter_less_equal_dict, table_name),
#             filter_spatial=data.get("filter_spatial_dict", None),
#             select_columns=data.get("select_columns", None),
#             consolidate_positions=not args["split_positions"],
#             offset=data.get("offset", None),
#             limit=limit,
#             get_count=get_count,
#             outer_join=False,
#             use_pandas=True,
#             suffixes=suffixes,
#             extra_filter_args=time_filter_args,
#         )
#         time_d["execute query"] = time.time() - now
#         now = time.time()

#         # update the root_ids for the supervoxel_ids from production
#         is_delete = df_new["deleted"] < timestamp
#         is_create = (df_new["created"] > timestamp_start.replace(tzinfo=None)) & (
#             (df_new["deleted"] > timestamp) | df_new["deleted"].isna()
#         )

#         root_cols = [c for c in df_new.columns if c.endswith("pt_root_id")]

#         df_create = df_new[is_create]
#         for c in root_cols:
#             sv_col = c.replace("root_id", "supervoxel_id")
#             svids = df_create[sv_col].values
#             # not_null = ~svids.isna()
#             new_roots = cg_client.get_roots(svids, timestamp=timestamp).astype(np.int64)
#             df_create[c] = new_roots
#         df_create = df_create.drop(["created", "deleted", "superceded_id"], axis=1)
#         time_d["update new roots"] = time.time() - now
#         now = time.time()

#         # merge the dataframes
#         if df_mat is not None:
#             print(df_mat.columns)
#             df = df_mat[~df_mat.id.isin(df_new[is_delete].id)]
#             if len(df_create) > 0:
#                 df = pd.concat([df, df_create], ignore_index=True)
#         else:
#             df = df_create

#         # apply the filters post
#         # apply the original filters to remove rows
#         # from this result which are not relevant

#         if filter_in_dict is not None:
#             for col, val in filter_in_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col].isin(val)]
#         if filter_out_dict is not None:
#             for col, val in filter_out_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[~df[col].isin(val)]
#         if filter_equal_dict is not None:
#             for col, val in filter_equal_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col] == val]
#         if filter_greater_dict is not None:
#             for col, val in filter_greater_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col] > val]
#         if filter_less_dict is not None:
#             for col, val in filter_less_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col] < val]
#         if filter_greater_equal_dict is not None:
#             for col, val in filter_greater_equal_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col] >= val]
#         if filter_less_equal_dict is not None:
#             for col, val in filter_less_equal_dict.items():
#                 if col.endswith("root_id"):
#                     df = df[df[col] <= val]

#         now = time.time()
#         headers = None
#         warnings = []
#         if len(df) >= max_limit:
#             warnings.append(f'201 - "Limited query to {max_limit} rows')
#         # if args["return_pandas"] and args["return_pyarrow"]:
#         #     warnings.append(
#         #         "return_pandas=true is deprecated and may convert columns with nulls to floats, Please upgrade CAVEclient to >XXX with pip install -U caveclient"
#         #     )
#         if len(warnings) > 0:
#             headers = {"Warning": ". ".join(warnings)}
#         if args["return_pyarrow"]:
#             context = pa.default_serialization_context()
#             serialized = context.serialize(df).to_buffer().to_pybytes()
#             time_d["serialize"] = time.time() - now
#             logging.info(time_d)
#             return Response(
#                 serialized, headers=headers, mimetype="x-application/pyarrow"
#             )
#         else:
#             dfjson = df.to_json(orient="records")
#             time_d["serialize"] = time.time() - now
#             logging.info(time_d)
#             response = Response(dfjson, headers=headers, mimetype="application/json")
#             return after_request(response)

# def map_filters(
#                 [filter_in_dict, filter_out_dict, filter_equal_dict, filter_greater_dict, filter_less_dict, filter_greater_equal_dict, filter_less_equal_dict],
#                 timestamp,
#                 timestamp_start,
#                 cg_client,
#             )
#             past_filter_in_dict, past_filter_out_dict, past_equal_dict = past_filters
# def make_materialized_query_manager(
#     datastack_name,
#     mat_version,
#     timestamp=datetime.datetime.utcnow(),
#     table=None,
#     join_tables=None,
#     select_columns=None,
#     filter_in_dict=None,
#     filter_equal_dict=None,
#     filter_greater_dict=None,
#     filter_less_dict=None,
#     filter_greater_equal_dict=None,
#     filter_less_equal_dict=None,
#     filter_out_dict=None,
#     filter_spatial_dict=None,
#     offset=0,
#     limit=None,
#     suffixes=None,
#     joins=None,
# ):
#     if table & join_tables:
#         raise ValueError("Cannot specify tables and join statement")

#     db_name = "{}__mat{}".format(datastack_name, mat_version.version)

#     qm = QueryManager(db_name=db_name, split_mode=False)
#     if table:
#         tables = [table]
#     else:
#         tables = [jt[0][0] for jt in join_tables]
#     qm.add_tables(tables)
#     if join_tables:
#         for join_table in join_tables:
#             qm.join_table(*join_table[0])

#         if select_columns:
#             for table_name, columns in select_columns.items():
#                 qm.select_column(
#                     table_name,
#                 )
#         qm.validate_joins()


# def execute_query_manager(qm):
#     query = qm.make_query()
#     df = _execute_query(
#         qm.session,
#         qm.engine,
#         query=query,
#         fix_wkb=False,
#         index_col=None,
#         get_count=False,
#     )
#     return df


# def execute_materialized_query(datastack, mat_version, timestamp, user_data):
#     qm = make_materialized_query_manager(datastack, mat_version, **user_data)
#     df = execute_query_manager(qm)
#     df = update_root_ids(df, start_time=mat_version.timestamp, end_time=timestamp)

#     return df


# def execute_production_query(datastack, user_data, mat_timestamp, timestamp, matdf):

#     if timestamp > mat_timestamp:
#         qm = make_production_query_manager(
#             datastack, **user_data, start_time=mat_timestamp, end_time=timestamp
#         )
#     else:
#         qm = make_production_query_manager(
#             datastack, **user_data, start_time=timestamp, end_time=mat_timestamp
#         )
#     prod_df = execute_query_manager(qm)
#     prod_df = lookup_root_ids(prod_df, timestamp)


# def combine_queries(mat_df, prod_df, user_data):
#     prod_df = remove_deleted_items(prod_df, prod_df)
#     matdf = remove_deleted_items(mat_df, prod_df)
#     prod_df = remove_crud_columns(prod_df)

#     comb_df = pd.concat([mat_df, prod_df])
#     comb_df = apply_filters(comb_df, user_data)
#     return comb_df
