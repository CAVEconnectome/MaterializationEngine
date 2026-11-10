"""Frozen queries answered from Delta Lake exports (blueprints/client/lake_query.py).

TestPlan rewrites QueryManager's Postgres SQL (including the unmerged-version shape
captured from ltv7) without any lake. TestEndpointParity exports the test frozen
version's table with the real exporter into a local directory, records it in the
catalog, and checks that the version query endpoint returns the same response from the
lakes as from Postgres. It runs after TestCreateFrozenVersion, which builds
test_aligned_volume__mat1.
"""
import json
from unittest import mock

import pandas as pd
import pyarrow as pa
import pytest

from materializationengine.blueprints.client import lake_query as lq

SPLIT_SQL = (
    "SELECT synapses_pni_2.id, synapses_pni_2.created, synapses_pni_2.deleted, synapses_pni_2.superceded_id, "
    "synapses_pni_2.valid, CAST(ST_X(synapses_pni_2.ctr_pt_position) AS INTEGER) AS ctr_pt_position_x, "
    "CAST(ST_Y(synapses_pni_2.ctr_pt_position) AS INTEGER) AS ctr_pt_position_y, "
    "CAST(ST_Z(synapses_pni_2.ctr_pt_position) AS INTEGER) AS ctr_pt_position_z, synapses_pni_2.size, "
    "coalesce(synapses_pni_2.post_pt_root_id, 1) AS post_pt_root_id "
    "FROM (SELECT synapses_pni_2.id AS id, synapses_pni_2.created AS created, synapses_pni_2.deleted AS deleted, "
    "synapses_pni_2.superceded_id AS superceded_id, synapses_pni_2.valid AS valid, "
    "synapses_pni_2.ctr_pt_position AS ctr_pt_position, synapses_pni_2.size AS size, "
    "synapses_pni_2__minnie3_v1.post_pt_root_id AS post_pt_root_id "
    "FROM synapses_pni_2 LEFT OUTER JOIN synapses_pni_2__minnie3_v1 ON synapses_pni_2.id = synapses_pni_2__minnie3_v1.id) "
    "AS synapses_pni_2 WHERE synapses_pni_2.post_pt_root_id IN (864691135000000001, 864691135000000002) "
    "AND (synapses_pni_2.ctr_pt_position &&& ST_3DMakeBox('POINTZ(10 0 0)', 'POINTZ(0 10 10)')) "
    "AND synapses_pni_2.valid = true LIMIT 200000 OFFSET 0"
)
LAKE_COLUMNS = {"id", "valid", "size", "post_pt_root_id", "post_pt_supervoxel_id",
                "ctr_pt_position_x", "ctr_pt_position_y", "ctr_pt_position_z"}


class TestVersionLakes:
    def test_reexport_serves_only_the_newest_copy_of_each_sort_order(self, monkeypatch):
        from materializationengine import deltalake_catalog as cat

        def row(table, part, suffix, done):
            return {"table_name": table, "partition_by": part, "spec_name": f"{part}{suffix}",
                    "uri": f"/lakes/{table}/{part}{suffix}", "completed_at": done}

        rows = [row("syn", "post_pt_root_id", "__s7", "2026-11-09T14:54"),
                row("syn", "pre_pt_root_id", "__s7", "2026-11-09T14:54"),
                row("syn", "post_pt_root_id", "__s8", "2026-11-09T15:28"),
                row("syn", "pre_pt_root_id", "__s8", "2026-11-09T15:28"),
                row("nuc", "id", "", "2026-11-09T14:50")]
        monkeypatch.setattr(cat, "list_exports", lambda *a, **k: rows)
        monkeypatch.setattr(lq, "datastack_info", lambda ds: ("av", "pcg"))
        lq._lakes_cache.clear()
        by_table, pcg = lq.version_lakes("ds", 1)
        assert pcg == "pcg"
        assert sorted(l["spec_name"] for l in by_table["syn"]) == ["post_pt_root_id__s8", "pre_pt_root_id__s8"]
        assert [l["spec_name"] for l in by_table["nuc"]] == ["id"]
        lq._lakes_cache.clear()


class TestPlan:
    def plan(self, sql, count=False, columns=LAKE_COLUMNS):
        lakes = {"synapses_pni_2": [{"spec_name": "post_pt_root_id"}]}
        return lq.plan_query(sql, lakes, "minnie3_v1", lambda t: columns, count=count)

    def test_unmerged_subquery_becomes_the_lake(self):
        p = self.plan(SPLIT_SQL)
        assert "LEFT" not in p.sql and "__minnie3_v1" not in p.sql and "ST_" not in p.sql
        assert "created" not in p.sql and "superceded_id" not in p.sql  # left out, as merged versions do
        assert p.columns == {"synapses_pni_2": {"id", "valid", "size", "post_pt_root_id", "ctr_pt_position_x",
                                                "ctr_pt_position_y", "ctr_pt_position_z"}}
        preds = p.predicates["synapses_pni_2"]
        assert ("post_pt_root_id", "in", [864691135000000001, 864691135000000002]) in preds
        assert ("ctr_pt_position_x", "between", (0.0, 10.0)) in preds  # corners in either order
        assert p.limit == 200000 and p.offset == 0 and p.streamable

    def test_runs_on_duckdb(self):
        import duckdb

        p = self.plan(SPLIT_SQL)
        con = duckdb.connect()
        con.register("synapses_pni_2", pa.table({
            "id": [1, 2, 3], "valid": [True, True, True], "size": [5, 6, 7],
            "post_pt_root_id": pa.array([864691135000000001, None, 864691135000000002], pa.int64()),
            "ctr_pt_position_x": pa.array([1, 2, 30], pa.int32()), "ctr_pt_position_y": pa.array([1, 2, 3], pa.int32()),
            "ctr_pt_position_z": pa.array([1, 2, 3], pa.int32())}))
        out = con.sql(p.sql).to_arrow_table().to_pydict()
        assert out["id"] == [1] and out["post_pt_root_id"] == [864691135000000001]

    def test_a_missing_column_is_an_error(self):
        with pytest.raises(lq.LakeQueryError, match="not in its lake"):
            self.plan(SPLIT_SQL, columns=LAKE_COLUMNS - {"size"})

    def test_a_table_without_a_lake_is_an_error(self):
        with pytest.raises(lq.LakeQueryError, match="has no lake"):
            self.plan("SELECT other.id FROM other")

    def test_tablesample_and_count(self):
        p = self.plan("SELECT t.id FROM synapses_pni_2 AS t TABLESAMPLE SYSTEM (2.5) WHERE t.valid = true", count=True)
        assert p.samples == {"synapses_pni_2": 2.5} and "TABLESAMPLE" not in p.sql.upper() and not p.streamable

    def test_hash_sampling_text_matches_postgis(self, mat_metadata):
        """ST_AsText(point) is rebuilt from x/y/z exactly, so md5-based sampling picks the same rows."""
        import duckdb
        import sqlalchemy as sa

        expr = ("mod(ascii(substr(md5(ST_AsText(t.ctr_pt_position)), 1, 1)) * 1000 + "
                "ascii(substr(md5(ST_AsText(t.ctr_pt_position)), 2, 1)) * 100, 7)")
        points = [(1, 2, 3), (169092, 123067, 19534), (0, 0, 0), (2097151, 5, 40000)]
        engine = sa.create_engine(mat_metadata["sql_uri"])
        with engine.connect() as conn:
            want = [conn.execute(f"SELECT {expr.replace('t.ctr_pt_position', f'ST_MakePoint({x}, {y}, {z})')}").scalar()
                    for x, y, z in points]
        engine.dispose()
        p = self.plan(f"SELECT {expr} AS h FROM synapses_pni_2 AS t", columns={"ctr_pt_position_x", "ctr_pt_position_y",
                                                                              "ctr_pt_position_z"})
        con = duckdb.connect()
        con.register("synapses_pni_2", pa.table({f"ctr_pt_position_{a}": pa.array([pt[i] for pt in points], pa.int32())
                                                 for i, a in enumerate("xyz")}))
        assert [r[0] for r in con.sql(p.sql).fetchall()] == want


class TestStatistics:
    @pytest.mark.parametrize("lo,hi,op,value,expected", [
        (5, 10, "in", [1, 7], True), (5, 10, "in", [1, 11], False), (5, 10, "eq", 10, True),
        (5, 10, "gt", 10, False), (5, 10, "ge", 10, True), (5, 10, "lt", 5, False), (5, 10, "le", 5, True),
        (5, 10, "between", (11, 20), False), (5, 10, "between", (0, 5), True), (None, None, "eq", 1, True),
        ("a", "b", "eq", 1, True),
    ])
    def test_may_match(self, lo, hi, op, value, expected):
        assert lq._may_match(lo, hi, op, value) is expected

    def test_row_mask_keeps_rows_matching_every_predicate(self):
        t = pa.table({"a": pa.array([1, 2, 3, 4, None], pa.int64()), "b": pa.array([10, 20, 30, 40, 50], pa.int32())})
        mask = lq._row_mask(t, [("a", "in", [2, 4, 9]), ("b", "between", (15, 45)), ("b", "lt", 41)])
        assert t.filter(mask).column("a").to_pylist() == [2, 4]
        # a literal pyarrow cannot compare is left to DuckDB rather than dropping rows
        assert lq._row_mask(t, [("a", "eq", "x")]) is None
        assert lq._row_mask(t, [("missing", "eq", 1)]) is None


class FakeRedis:
    """The few redis commands lake_index uses."""

    def __init__(self):
        self.kv, self.z = {}, {}

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return None
        self.kv[key] = value
        return True

    def get(self, key):
        return self.kv.get(key)

    def exists(self, key):
        return int(key in self.kv)

    def delete(self, key):
        self.kv.pop(key, None)

    def pipeline(self, transaction=False):
        redis, ops = self, []

        class P:
            def zincrby(self, key, n, member):
                ops.append(lambda: redis.z.setdefault(key, {}).__setitem__(member, redis.z.get(key, {}).get(member, 0) + n))

            def expire(self, key, ttl):
                ops.append(lambda: True)

            def zrevrange(self, key, a, b, withscores=False):
                ops.append(lambda: sorted(redis.z.get(key, {}).items(), key=lambda kv: -kv[1])[a:b + 1])

            def execute(self):
                return [op() for op in ops]

        return P()


@pytest.fixture
def fake_redis(monkeypatch):
    from materializationengine.blueprints.client import lake_index as li

    r = FakeRedis()
    monkeypatch.setattr(li, "_redis", lambda: r)
    return r


def _clear_caches():
    from materializationengine.blueprints.client import lake_index as li

    li.cache.clear()
    lq._footers.clear()


class TestChooseLake:
    """Copies of one table sorted by a and by b, both split into files that each span the
    whole range of the other column, so file statistics cannot tell the copies apart."""

    @pytest.fixture
    def lakes(self, tmp_path, fake_redis):
        from deltalake import WriterProperties, write_deltalake

        n = 20000
        table = pa.table({"id": pa.array(range(n), pa.int64()),
                          "a": pa.array([(i * 7919) % n for i in range(n)], pa.int64()),
                          "b": pa.array([(i * 104729) % n for i in range(n)], pa.int64())})
        out = []
        for key in ("a", "b"):
            uri = str(tmp_path / key)
            # file f holds rows with id % 4 == f, sorted by the key within the file: every file
            # covers the key's whole range, yet its row groups are narrow in the key
            import pyarrow.compute as pc
            for f in range(4):
                part = table.filter(pc.equal(pc.bit_wise_and(table.column("id"), 3), f)).sort_by(key)
                write_deltalake(uri, part, mode="append",
                                writer_properties=WriterProperties(max_row_group_size=500))
            out.append({"uri": uri, "spec_name": key, "partition_by": key})
        _clear_caches()
        yield out
        _clear_caches()

    def test_the_copy_sorted_by_the_filter_wins_when_files_tie(self, lakes):
        preds = [("b", "in", [5, 9000, 17000])]
        lake, index, chunks, how = lq.choose_lake(lakes, preds, ["id", "a", "b"])
        assert lake["spec_name"] == "b" and len(index.paths) == 4 and how == "built"
        assert 0 < sum(len(c.groups) for c in chunks) <= 4 * 3
        assert lq.choose_lake(lakes, [("a", "eq", 3)], ["id", "a", "b"])[0]["spec_name"] == "a"

    def test_only_the_keyed_copy_is_loaded(self, lakes):
        from materializationengine.blueprints.client import lake_index as li

        lq.choose_lake(lakes, [("b", "eq", 7)], ["id", "b"])
        assert li.cache.peek(lakes[1]["uri"]) is not None and li.cache.peek(lakes[0]["uri"]) is None

    def test_read_keeps_only_matching_rows(self, lakes):
        preds = [("b", "in", [5, 9000, 17000])]
        lake, index, chunks, _ = lq.choose_lake(lakes, preds, ["id", "b"])
        fetch = lq.Fetch("t", lake, index, chunks, ["id", "b"], preds)
        got = pa.concat_tables(fetch.read(chunks))
        assert sorted(got.column("b").to_pylist()) == [5, 9000, 17000]
        # read again, now from cached raw footers, in pieces of a file sharing one footer
        lq._footers.clear()
        many = [lq.Chunk(c.file, list(range(int(index.rg_first[c.file + 1] - index.rg_first[c.file]))), 0)
                for c in chunks]
        everything = pa.concat_tables(fetch.read(many))
        assert sorted(everything.column("b").to_pylist()) == [5, 9000, 17000]
        assert lq._footers.size > 0

    def test_the_index_is_stored_next_to_the_lake_and_reloaded(self, lakes):
        from materializationengine.blueprints.client import lake_index as li

        lq.choose_lake(lakes, [("a", "eq", 3)], ["id", "a"])
        li.cache.clear()
        index, how = li.get(lakes[0])
        assert how == "stored" and index.rg_stats.keys() >= {"a"}
        again = li.LakeIndex.from_bytes(index.to_bytes())
        assert again.paths == index.paths and (again.col_sizes == index.col_sizes).all()
        assert again.schema.equals(index.schema)

    def test_an_index_that_does_not_match_the_catalog_is_rebuilt(self, lakes):
        from materializationengine.blueprints.client import lake_index as li

        index, _ = li.get(lakes[0])
        li.cache.clear()
        stale = dict(lakes[0], files=len(index.paths) + 1)
        assert li.get(stale)[1] == "built"

    def test_a_footer_that_disagrees_with_the_index_is_refused(self, lakes):
        lake, index, chunks, _ = lq.choose_lake(lakes, [("a", "eq", 3)], ["id", "a"])
        index.rg_start[int(index.rg_first[chunks[0].file]) + chunks[0].groups[0]] += 1
        with pytest.raises(lq.LakeQueryError, match="does not match"):
            lq._read_chunk(index, chunks[0], ["id", "a"])

    def test_another_workers_build_is_awaited(self, lakes, fake_redis, monkeypatch):
        from materializationengine.blueprints.client import lake_index as li

        fake_redis.set(f"cave:lakeidx:build:{lakes[0]['uri']}", "other")
        monkeypatch.setattr(li, "build", lambda *a, **k: pytest.fail("built while another worker builds"))
        assert li.cache.get(lakes[0], wait=False) == (None, "building elsewhere")
        # the other worker finishes: its stored index is picked up
        monkeypatch.undo()
        monkeypatch.setattr(li, "_redis", lambda: fake_redis)
        li.write(li.build(lakes[0]["uri"]))
        monkeypatch.setattr(li, "build", lambda *a, **k: pytest.fail("built though it was stored"))
        assert li.get(lakes[0])[1] == "stored"

    def test_prewarm_loads_hot_tables_of_recent_versions(self, lakes, fake_redis):
        from materializationengine.blueprints.client import lake_index as li

        for _ in range(3):
            li.record_use("ds", "syn", [lakes[1]])
        li.record_use("ds", "syn", [lakes[0]])
        catalog = {10: {"syn": lakes}}
        targets = li.prewarm_targets("ds", [10], lambda v: catalog[v])
        assert [t[0]["uri"] for t in targets][:2] == [lakes[1]["uri"], lakes[0]["uri"]]
        assert all(may_build for _, may_build in targets)
        li.record_use("ds", "syn", [lakes[1]], version=9)
        li.record_use("other", "syn", [lakes[1]], version=8)
        assert li.hot_versions("ds", 2) == [9]
        done = li.prewarm("ds", [10], lambda v: catalog[v], budget_seconds=30)
        assert len(done) == 2 and li.cache.peek(lakes[1]["uri"]) is not None


class TestIndexOfPartitionedLake:
    def test_hive_partition_column_is_not_a_file_column(self, tmp_path, fake_redis):
        from deltalake import write_deltalake

        from materializationengine.blueprints.client import lake_index as li

        uri = str(tmp_path / "p")
        write_deltalake(uri, pa.table({"x": pa.array(range(100), pa.int64()), "part": [i // 50 for i in range(100)]}),
                        partition_by=["part"])
        index = li.build(uri)
        assert index.columns == ["x"] and len(index.paths) == 2
        assert lq.select(index, [("x", "lt", 10)], ["x"])[0].groups == [0]


DATASTACK, VERSION, TABLE = "test_aligned_volume", 1, "test_synapse_table"
URL = f"/materialize/api/v3/datastack/{DATASTACK}/version/{VERSION}/table/{TABLE}/query"


@pytest.fixture(scope="module")
def exported(test_app, tmp_path_factory, mat_metadata):
    """test_synapse_table of test_aligned_volume__mat1 exported to local lakes and catalogued."""
    from materializationengine import deltalake_catalog as cat
    from materializationengine.workflows import deltalake_export as dx

    import sqlalchemy as sa
    from sqlalchemy.engine.url import make_url

    out = tmp_path_factory.mktemp("lakes")
    app = test_app.application
    # the test build leaves MaterializedMetadata empty; a real build records each table
    url = make_url(mat_metadata["sql_uri"])
    frozen = sa.create_engine(str(url.set(database=f"{DATASTACK}__mat{VERSION}")) if hasattr(url, "set")
                              else f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}/"
                                   f"{DATASTACK}__mat{VERSION}")
    with frozen.begin() as conn:
        conn.execute(
            f"INSERT INTO materializedmetadata (schema, table_name, row_count, materialized_timestamp, is_merged) "
            f"SELECT 'synapse', '{TABLE}', count(*), now(), true FROM {TABLE} "
            f"WHERE NOT EXISTS (SELECT 1 FROM materializedmetadata WHERE table_name = '{TABLE}')")
    frozen.dispose()
    info = {"datastack": DATASTACK, "aligned_volume": {"name": mat_metadata["aligned_volume"]},
            "segmentation_source": "graphene://local/test_pcg"}
    with app.app_context(), \
            mock.patch.dict(app.config, {"DELTALAKE_OUTPUT_BUCKET": str(out), "DELTALAKE_MAX_ROW_GROUP_SIZE": 1}), \
            mock.patch.object(dx, "set_deltalake_export_status"), mock.patch.object(dx, "append_deltalake_log"), \
            mock.patch.object(dx, "_get_redis_client"):
        cat._ensured.clear()  # earlier tests recreate the aligned volume database
        dx.run_deltalake_export(info, VERSION, TABLE)
        lakes = cat.list_exports(mat_metadata["aligned_volume"], DATASTACK, VERSION, table_name=TABLE)
    assert lakes, "the export recorded no complete lakes"
    yield lakes


@pytest.fixture
def api(test_app, monkeypatch, mat_metadata, fake_redis):
    import middle_auth_client.decorators as auth
    from materializationengine.blueprints.client import common, datastack

    monkeypatch.setattr(auth, "AUTH_DISABLED", True)
    info = lambda name: (mat_metadata["aligned_volume"], "test_pcg")  # noqa: E731
    monkeypatch.setattr(datastack, "get_relevant_datastack_info", info)
    monkeypatch.setattr(common, "get_relevant_datastack_info", info)
    monkeypatch.setattr(lq, "datastack_info", info)
    lq._lakes_cache.clear()
    _clear_caches()
    return test_app.application.test_client()


def post(api, backend, params, payload):
    query = {"return_pyarrow": "True", "arrow_format": "True", "split_positions": "True",
             "direct_sql_pandas": "True", **params}
    r = api.post(URL, query_string=query, data=json.dumps(payload),
                 headers={"Content-Type": "application/json", "Accept-Encoding": "zstd", lq.HEADER: backend})
    return r


def decode(response):
    with pa.ipc.open_stream(response.get_data()) as reader:
        table = reader.read_all()
    return table, table.to_pandas()


REQUESTS = {
    "all columns": ({}, {}),
    "filter on root id": ({}, "ROOT_FILTER"),
    "not in": ({}, "NOT_IN"),
    "select columns": ({}, {"select_columns": ["id", "pre_pt_root_id", "post_pt_position", "size"]}),
    "desired resolution": ({}, {"desired_resolution": [8, 8, 80]}),
    "no rows": ({}, {"filter_equal_dict": {TABLE: {"id": -1}}}),
    "limit": ({}, {"limit": 1}),
    "greater": ({}, {"filter_greater_dict": {TABLE: {"size": 0}}}),
    "less equal": ({}, {"filter_less_equal_dict": {TABLE: {"id": 3}}}),
    "spatial": ({}, {"filter_spatial_dict": {TABLE: {"pre_pt_position": [[0, 0, 0], [10 ** 7, 10 ** 7, 10 ** 7]]}}}),
    "positions joined": ({"split_positions": "False"}, {}),
    "count": ({"count": "True"}, {}),
}


class TestLakeEndpointParity:
    @pytest.mark.parametrize("name", list(REQUESTS))
    def test_lake_and_postgres_return_the_same_response(self, api, exported, name, monkeypatch):
        params, payload = REQUESTS[name]
        if payload in ("ROOT_FILTER", "NOT_IN"):
            _, everything = decode(post(api, "postgres", {}, {}))
            roots = everything["post_pt_root_id"].iloc[:2].tolist()
            key = "filter_in_dict" if payload == "ROOT_FILTER" else "filter_notin_dict"
            payload = {key: {TABLE: {"post_pt_root_id": roots}}}

        answered = []
        real = lq.answer
        monkeypatch.setattr(lq, "answer", lambda *a, **k: answered.append(1) or real(*a, **k))
        lake = post(api, "lake", params, payload)
        assert lake.status_code == 200, lake.get_data()[:500]
        assert answered, "the lake path was not taken"
        monkeypatch.setattr(lq, "answer", lambda *a, **k: pytest.fail("lake used with the postgres backend"))
        pg = post(api, "postgres", params, payload)
        assert pg.status_code == 200

        pg_table, pg_df = decode(pg)
        lake_table, lake_df = decode(lake)
        assert lake_table.schema.equals(pg_table.schema, check_metadata=True), (
            f"\npostgres: {pg_table.schema}\nlake: {lake_table.schema}")
        key = [c for c in ("id",) if c in pg_df.columns]
        if key and name != "limit":  # Postgres returns rows in no particular order
            pg_df = pg_df.sort_values(key).reset_index(drop=True)
            lake_df = lake_df.sort_values(key).reset_index(drop=True)
        if name == "limit":
            assert len(lake_df) == len(pg_df) == 1
        else:
            pd.testing.assert_frame_equal(lake_df, pg_df)
        for h in ("Content-Type", "dataframe_resolution", "column_names", "Warning"):
            assert lake.headers.get_all(h) == pg.headers.get_all(h), h

    def test_reads_only_the_row_groups_it_needs(self, api, exported):
        _, everything = decode(post(api, "postgres", {}, {}))
        root = int(everything["post_pt_root_id"].iloc[0])
        sql = (f"SELECT {TABLE}.id, {TABLE}.post_pt_root_id FROM {TABLE} "
               f"WHERE {TABLE}.post_pt_root_id IN ({root}) AND {TABLE}.valid = true LIMIT 10")
        table, facts = lq.answer(sql, DATASTACK, VERSION)
        names = {lake["spec_name"] for lake in exported}
        assert facts["lakes"][TABLE] in names
        if any(n.startswith("post_pt_root_id") for n in names):  # small tables keep only the id copy
            assert facts["lakes"][TABLE].startswith("post_pt_root_id")
        from materializationengine.blueprints.client import lake_index as li

        (lake,) = [lake for lake in exported if lake["spec_name"] == facts["lakes"][TABLE]]
        assert facts["row_groups"] < li.get(lake)[0].n_row_groups
        assert set(table.column("post_pt_root_id").to_pylist()) == {root}

    def test_too_big_is_refused_not_sent_to_postgres(self, api, exported, monkeypatch):
        api.application.config["QUERY_LAKE_MAX_BYTES"] = 1
        try:
            r = post(api, "lake", {}, {"filter_greater_dict": {TABLE: {"size": 0}}})
        finally:
            api.application.config.pop("QUERY_LAKE_MAX_BYTES")
        assert r.status_code == 413 and b"narrow it" in r.get_data()

    def test_shadow_answers_from_postgres_and_logs_the_comparison(self, api, exported, caplog):
        import logging

        with caplog.at_level(logging.INFO, logger=lq.__name__):
            r = post(api, "shadow", {}, {})
        assert r.status_code == 200
        assert any("lake shadow" in m and "'match': True" in m for m in caplog.messages), caplog.messages
