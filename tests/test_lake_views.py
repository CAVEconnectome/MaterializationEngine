"""lake_views: minnie's real view definitions (pg_get_viewdef from v1966), run over lakes by
DuckDB and Daft, must answer exactly what Postgres answers from the same tables."""

import json
import os
import uuid

import duckdb
import numpy as np
import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from materializationengine.blueprints.client import lake_views as lv

PCG = "minnie3_v1"
VIEWS = json.load(open(os.path.join(os.path.dirname(__file__), "test_data", "minnie65_views_v1966.json")))["views"]
DEFS = {v["name"]: v["definition"] for v in VIEWS}
COLS = {v["name"]: v["columns"] for v in VIEWS}
TARGETS = ["nucleus_detection_lookup_v1", "synapses_pni_2_in_out_degree", "connections_with_nuclei"]

CRUD = "id bigint PRIMARY KEY, created timestamp, deleted timestamp, superceded_id bigint, valid boolean"
TABLES = {  # annotation table: (its columns after the CRUD ones, its segmentation columns or None)
    "synapses_pni_2": ("pre_pt_position geometry(PointZ), ctr_pt_position geometry(PointZ), "
                       "post_pt_position geometry(PointZ), size double precision",
                       "pre_pt_supervoxel_id bigint, pre_pt_root_id bigint, "
                       "post_pt_supervoxel_id bigint, post_pt_root_id bigint"),
    "nucleus_detection_v0": ("pt_position geometry(PointZ), volume double precision, "
                             "bb_start_position geometry(PointZ), bb_end_position geometry(PointZ)",
                             "pt_supervoxel_id bigint, pt_root_id bigint"),
    "nucleus_alternative_points": ("target_id bigint, pt_position geometry(PointZ)",
                                   "pt_supervoxel_id bigint, pt_root_id bigint"),
    "nucleus_ref_neuron_svm": ("target_id bigint, classification_system varchar, cell_type varchar", None),
}


def _pt(rng):
    x, y, z = rng.integers(0, 100000, 3)
    return f"SRID=0;POINT Z ({x} {y} {z})"


def _fill(conn, rng):
    """Small tables with the cases the views care about: autapses, annotations without a
    segmentation row, roots with several nuclei or none, nuclei moved by alternative
    points, and neurons and glia."""
    roots = [864691135000000000 + i for i in range(30)]
    ins = lambda t, rows: rows and conn.execute(sa.text(
        f"INSERT INTO {t} ({', '.join(rows[0])}) VALUES ({', '.join(':' + k for k in rows[0])})"), rows)
    syn, syn_seg = [], []
    for i in range(1, 3001):
        pre, post = rng.choice(roots), rng.choice(roots[:20])
        syn.append(dict(id=i, valid=True, size=float(rng.gamma(2, 300)), pre_pt_position=_pt(rng),
                        ctr_pt_position=_pt(rng), post_pt_position=_pt(rng)))
        if i % 50:  # every 50th synapse has no segmentation row
            syn_seg.append(dict(id=i, pre_pt_supervoxel_id=int(pre) + 7, pre_pt_root_id=int(pre),
                                post_pt_supervoxel_id=int(post) + 7, post_pt_root_id=int(post)))
    ins("synapses_pni_2", syn)
    ins(f"synapses_pni_2__{PCG}", syn_seg)
    nuc, nuc_seg, svm = [], [], []
    for i in range(1, 41):
        root = 0 if i % 13 == 0 else roots[i % 25]  # roots[0..14] get two nuclei, some get root 0
        nuc.append(dict(id=i, valid=True, volume=float(rng.random() * 300), pt_position=_pt(rng),
                        bb_start_position=_pt(rng), bb_end_position=_pt(rng)))
        if i != 40:
            nuc_seg.append(dict(id=i, pt_supervoxel_id=root + 3 if root else 0, pt_root_id=int(root)))
        if i % 7:
            svm.append(dict(id=i, valid=True, target_id=i, classification_system="aibs_neuronal",
                            cell_type="neuron" if i % 5 else "glia"))
    ins("nucleus_detection_v0", nuc)
    ins(f"nucleus_detection_v0__{PCG}", nuc_seg)
    ins("nucleus_ref_neuron_svm", svm)
    alt = [dict(id=i, valid=True, target_id=t, pt_position=_pt(rng)) for i, t in enumerate([2, 13, 26, 27, 33], 1)]
    ins("nucleus_alternative_points", alt)
    ins(f"nucleus_alternative_points__{PCG}",
        [dict(id=a["id"], pt_supervoxel_id=roots[25 + a["id"] - 1] + 3, pt_root_id=roots[25 + a["id"] - 1])
         for a in alt])


@pytest.fixture(scope="module")
def frozen(mat_metadata):
    """A database with the split tables, minnie's views created from their real definitions,
    and the lake of each table as the exporter writes it (LEFT JOIN, points as _x/_y/_z,
    CRUD columns dropped). Yields (engine, {table: arrow table})."""
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    name = f"views_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    with engine.begin() as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
        for table, (cols, seg) in TABLES.items():
            conn.execute(f"CREATE TABLE {table} ({CRUD}, {cols})")
            if seg:
                conn.execute(f"CREATE TABLE {table}__{PCG} (id bigint PRIMARY KEY, {seg})")
        _fill(conn, np.random.default_rng(7))
        needed = set()

        def need(n):
            if n in DEFS and n not in needed:
                needed.add(n)
                for r in next(v for v in VIEWS if v["name"] == n)["reads"]:
                    need(r)
        for t in TARGETS:
            need(t)
        for v in lv.view_order([v for v in VIEWS if v["name"] in needed]):
            conn.execute(sa.text(f"CREATE VIEW {v} AS {DEFS[v]}"))
    lakes = {}
    with engine.connect() as conn:
        for table, (cols, seg) in TABLES.items():
            select = ["a.id", "a.valid"]
            for col in (c.strip() for c in cols.split(",")):
                n, t = col.split(" ", 1)
                select += ([f"ST_X(a.{n})::int AS {n}_x", f"ST_Y(a.{n})::int AS {n}_y", f"ST_Z(a.{n})::int AS {n}_z"]
                           if t.startswith("geometry") else [f"a.{n}"])
            join = ""
            if seg:
                select += [f"s.{c.strip().split(' ')[0]}" for c in seg.split(",")]
                join = f" LEFT JOIN {table}__{PCG} s USING (id)"
            rows = conn.execute(f"SELECT {', '.join(select)} FROM {table} a{join} ORDER BY a.id")
            names = list(rows.keys())
            data = [list(r) for r in rows]
            lakes[table] = pa.table({n: [r[i] for r in data] for i, n in enumerate(names)})
    yield engine, lakes
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin.dispose()


def _canonical(table: pa.Table) -> list:
    """Rows as sorted tuples, integers as int, floats rounded: engines sum in different orders."""
    cols = []
    for f, c in zip(table.schema, table.columns):
        values = c.to_pylist()
        if pa.types.is_floating(f.type) or pa.types.is_decimal(f.type):
            values = [None if v is None else round(float(v), 6) for v in values]
        elif pa.types.is_integer(f.type):
            values = [None if v is None else int(v) for v in values]
        cols.append(values)
    rows = list(zip(*cols))
    return sorted(rows, key=lambda r: tuple((v is None, v) for v in r))


def _postgres(engine, view, columns):
    select = []
    for c in COLS[view]:
        n = c["name"]
        select += ([f"ST_X({n})::int AS {n}_x", f"ST_Y({n})::int AS {n}_y", f"ST_Z({n})::int AS {n}_z"]
                   if c["type"].startswith("geometry") else [n])
    with engine.connect() as conn:
        rows = conn.execute(f"SELECT {', '.join(select)} FROM {view}")
        names = list(rows.keys())
        data = [list(r) for r in rows]
    assert names == columns
    return pa.table({n: pa.array([r[i] for r in data]) for i, n in enumerate(names)})


def _plan(view, lakes):
    columns = {t: lakes[t].column_names for t in lakes}
    return lv.lake_sql(view, DEFS, columns, PCG, lv.point_names(COLS, columns))


class TestParity:
    @pytest.mark.parametrize("view", TARGETS)
    def test_duckdb_matches_postgres(self, frozen, view):
        engine, lakes = frozen
        plan = _plan(view, lakes)
        con = duckdb.connect()
        for t in plan.lakes:
            con.register(t, lakes[t])
        got = con.sql(plan.sql).to_arrow_table()
        expected = _postgres(engine, view, plan.columns)
        assert got.column_names == plan.columns
        assert len(expected) > 0
        assert _canonical(got) == _canonical(expected)
        # the cases the views exist for are in the data, so agreeing means something
        rows = expected.to_pylist()
        if view == "nucleus_detection_lookup_v1":
            assert any(r["pt_root_id"] != r["orig_root_id"] for r in rows)  # moved by an alternative point
            assert 40 not in {r["id"] for r in rows}  # no segmentation row: not in the merge view
        if view == "connections_with_nuclei":
            assert {r["pre_nuc_id"] == -1 for r in rows} == {True, False}
            assert {r["post_nuc_id"] == -1 for r in rows} == {True, False}
        if view == "synapses_pni_2_in_out_degree":  # an inner join: roots 20-29 only send, so they're out
            assert {r["root_id"] - 864691135000000000 for r in rows} <= set(range(20))


    @pytest.mark.parametrize("view", TARGETS)
    def test_daft_matches_postgres(self, frozen, view, tmp_path):
        daft = pytest.importorskip("daft")
        from daft.session import Session
        from deltalake import write_deltalake

        engine, lakes = frozen
        plan = _plan(view, lakes)
        sess = Session()
        for t in plan.lakes:  # through real Delta lakes, as the pilot reads them
            write_deltalake(str(tmp_path / t), lakes[t])
            sess.create_temp_table(t, daft.read_deltalake(str(tmp_path / t)))
        got = sess.sql(plan.sql).to_arrow()
        assert _canonical(got) == _canonical(_postgres(engine, view, plan.columns))


class TestRewrites:
    def test_a_merge_view_becomes_its_lake_with_segmentation(self):
        plan = lv.lake_sql("synapses_pni_2_out_degree", DEFS,
                           {"synapses_pni_2": ["id", "size", "pre_pt_root_id", "post_pt_root_id",
                                               "pre_pt_supervoxel_id", "post_pt_supervoxel_id"]}, PCG, set())
        assert plan.lakes == {"synapses_pni_2"} and "__minnie3_v1 " not in plan.sql
        assert '"pre_pt_root_id" IS NOT NULL' in plan.sql and "<>" not in plan.sql
        assert "CAST(COUNT(id) AS BIGINT)" in plan.sql

    def test_a_view_over_a_table_without_a_lake_is_refused(self):
        with pytest.raises(lv.ViewNotSupported, match="no lake"):
            lv.lake_sql("synapses_pni_2_out_degree", DEFS, {"other": ["id"]}, PCG, set())

    def test_a_computed_point_is_refused(self):
        defs = {"v": "SELECT ST_Distance(pt_position, pt_position) AS d FROM t"}
        with pytest.raises(lv.ViewNotSupported, match="point"):
            lv.lake_sql("v", defs, {"t": ["pt_position_x", "pt_position_y", "pt_position_z"]}, PCG, {"pt_position"})

    def test_view_order_puts_dependencies_first(self):
        order = lv.view_order(VIEWS)
        assert order.index("synapses_pni_2__minnie3_v1_merge") < order.index("synapses_pni_2_in_degree") \
            < order.index("synapses_pni_2_in_out_degree")
        assert order.index("single_neurons") < order.index("connections_with_nuclei")
