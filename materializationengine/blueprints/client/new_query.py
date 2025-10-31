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
