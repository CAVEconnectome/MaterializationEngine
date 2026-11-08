"""The frozen-version query endpoint returns the same Arrow response whichever way the server
fetches rows: ADBC (QUERY_FETCH_WITH_ADBC=True, the default) or pd.read_sql (False).

Runs after TestCreateFrozenVersion, which builds test_aligned_volume__mat1. Compares, for each
request, the Arrow schema including its pandas metadata (so every column's name, order, Arrow type
and pandas dtype), the decoded DataFrame, and the response headers.

The frozen test table holds integers, booleans, floats and positions only (the merge drops
created/deleted). Timestamps, strings and NULLs are covered one level down, through
_execute_query, in test_query_adbc.py::TestAdbcFetch::test_adbc_matches_read_sql.
"""
import json

import pandas as pd
import pyarrow as pa
import pytest

DATASTACK, VERSION, TABLE = "test_aligned_volume", 1, "test_synapse_table"
URL = f"/materialize/api/v3/datastack/{DATASTACK}/version/{VERSION}/table/{TABLE}/query"
COMPARED_HEADERS = ("Content-Type", "dataframe_resolution", "column_names", "Warning")

REQUESTS = {
    "all columns": ({}, {}),
    "filter on root id": ({}, "ROOT_FILTER"),
    "select columns": ({}, {"select_columns": ["id", "pre_pt_root_id", "post_pt_position", "size"]}),
    "desired resolution": ({}, {"desired_resolution": [8, 8, 80]}),
    "no rows": ({}, {"filter_equal_dict": {TABLE: {"id": -1}}}),
    "limit": ({}, {"limit": 1}),
    "uncompressed": ({"ipc_compress": "False"}, {}),
    "positions joined": ({"split_positions": "False"}, {}),
}


@pytest.fixture
def api(test_app, monkeypatch):
    import middle_auth_client.decorators as auth
    from materializationengine.blueprints.client import common, datastack

    monkeypatch.setattr(auth, "AUTH_DISABLED", True)
    info = lambda name: ("test_aligned_volume", "test_pcg")  # noqa: E731
    monkeypatch.setattr(datastack, "get_relevant_datastack_info", info)
    monkeypatch.setattr(common, "get_relevant_datastack_info", info)
    # a plain client: the session-wide test_app client keeps its last request context open until the
    # session ends, when request-teardown hooks would then run outside an app context
    return test_app.application.test_client()


def post(api, use_adbc, params, payload):
    api.application.config["QUERY_FETCH_WITH_ADBC"] = use_adbc
    query = {"return_pyarrow": "True", "arrow_format": "True", "split_positions": "True",
             "direct_sql_pandas": "True", **params}
    r = api.post(URL, query_string=query, data=json.dumps(payload),
                 headers={"Content-Type": "application/json", "Accept-Encoding": "zstd"})
    assert r.status_code == 200, r.get_data()[:500]
    return r


def decode(response):
    with pa.ipc.open_stream(response.get_data()) as reader:
        table = reader.read_all()
    return table, table.to_pandas()


class TestQueryEndpointParity:
    @pytest.mark.parametrize("name", list(REQUESTS))
    def test_adbc_and_read_sql_return_the_same_response(self, api, name, monkeypatch):
        from materializationengine.blueprints.client import query as q

        params, payload = REQUESTS[name]
        if payload == "ROOT_FILTER":
            _, everything = decode(post(api, False, {}, {}))
            payload = {"filter_in_dict": {TABLE: {"post_pt_root_id": everything["post_pt_root_id"].iloc[:2].tolist()}}}

        adbc_calls = []
        real = q._read_sql_adbc
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: adbc_calls.append(1) or real(*a))
        new = post(api, True, params, payload)
        assert adbc_calls, "the ADBC path was not taken"
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: pytest.fail("ADBC used with the flag off"))
        old = post(api, False, params, payload)

        old_table, old_df = decode(old)
        new_table, new_df = decode(new)
        assert new_table.schema.equals(old_table.schema, check_metadata=True), (
            f"\nold: {old_table.schema}\nnew: {new_table.schema}")
        assert dict(new_df.dtypes.astype(str)) == dict(old_df.dtypes.astype(str))
        pd.testing.assert_frame_equal(new_df, old_df)
        for h in COMPARED_HEADERS:
            assert new.headers.get_all(h) == old.headers.get_all(h), h
        if name not in ("no rows",):
            assert len(new_df) > 0
