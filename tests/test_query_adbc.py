"""The ADBC fetch path in client/query.py returns the same DataFrame as pd.read_sql, and API
requests keep (but bound) their connection pools."""
import datetime

import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa
from flask import Flask
from geoalchemy2 import Geometry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from materializationengine import database
from materializationengine.blueprints.client import query as q

Base = declarative_base()


class AdbcProbe(Base):
    __tablename__ = "adbc_probe"
    id = sa.Column(sa.BigInteger, primary_key=True)
    created = sa.Column(sa.DateTime)
    valid = sa.Column(sa.Boolean)
    small = sa.Column(sa.Integer)
    tiny = sa.Column(sa.SmallInteger)  # no dtype_map entry: pd.read_sql infers Int64
    single = sa.Column(sa.REAL)  # float4
    size = sa.Column(sa.Float)
    root_id = sa.Column(sa.BigInteger)
    name = sa.Column(sa.String)
    amount = sa.Column(sa.Numeric)
    pt = sa.Column(Geometry("POINTZ", dimension=3))


@pytest.fixture(scope="module")
def engine(database_uri):
    e = sa.create_engine(database_uri)
    AdbcProbe.__table__.drop(e, checkfirst=True)
    AdbcProbe.__table__.create(e)
    t0 = datetime.datetime(2020, 11, 4, 7, 37, 1, 605960)
    with e.begin() as conn:
        conn.execute(AdbcProbe.__table__.insert(), [
            dict(id=i, created=t0 + datetime.timedelta(seconds=i), valid=i % 3 != 0, small=i * 7, tiny=i,
                 single=i / 3,
                 size=i / 3, root_id=864691136031763387 + i, name=f"n{i}", amount=i,
                 pt=f"POINTZ({i} {2 * i} {3 * i})")
            for i in range(1, 50)
        ] + [dict(id=50, created=None, valid=None, small=None, tiny=None, single=None, size=None, root_id=None, name=None,
                       amount=None, pt=None)])  # a row of NULLs
    yield e
    AdbcProbe.__table__.drop(e, checkfirst=True)
    e.dispose()


def fetch(engine, use_adbc, *columns):
    app = Flask(__name__)
    app.config["QUERY_FETCH_WITH_ADBC"] = use_adbc
    session = sessionmaker(bind=engine)()
    try:
        with app.test_request_context():
            query = session.query(AdbcProbe).with_entities(*columns).order_by(AdbcProbe.id)
            return q._execute_query(session, engine, query, fix_wkb=False, direct_sql_pandas=True)
    finally:
        session.close()


SCALARS = [AdbcProbe.id, AdbcProbe.created, AdbcProbe.valid, AdbcProbe.small, AdbcProbe.tiny, AdbcProbe.size,
           AdbcProbe.root_id, AdbcProbe.name]


class TestAdbcFetch:
    def test_adbc_matches_read_sql(self, engine):
        expected = fetch(engine, False, *SCALARS)
        got = fetch(engine, True, *SCALARS)
        assert dict(got.dtypes.astype(str)) == dict(expected.dtypes.astype(str))
        pd.testing.assert_frame_equal(got, expected)


    def test_nan_is_missing_like_read_sql(self, engine):
        # pd.read_sql turns a Postgres NaN into a missing value; ADBC must too
        with engine.begin() as conn:
            conn.execute(sa.text("UPDATE adbc_probe SET size = 'NaN' WHERE id = 1"))
        try:
            got, expected = fetch(engine, True, *SCALARS), fetch(engine, False, *SCALARS)
            assert expected["size"].isna().tolist()[:2] == [True, False]
            pd.testing.assert_frame_equal(got, expected)
            assert pa.RecordBatch.from_pandas(got).column("size").null_count == \
                pa.RecordBatch.from_pandas(expected).column("size").null_count
        finally:
            with engine.begin() as conn:
                conn.execute(sa.text("UPDATE adbc_probe SET size = 1.0 / 3 WHERE id = 1"))

    def test_only_null_rows_match_read_sql(self, engine):
        # an unmapped column (SmallInteger) that is NULL in every returned row: pd.read_sql infers string
        def only_null(use_adbc):
            app = Flask(__name__)
            app.config["QUERY_FETCH_WITH_ADBC"] = use_adbc
            session = sessionmaker(bind=engine)()
            try:
                with app.test_request_context():
                    query = session.query(AdbcProbe).with_entities(*SCALARS).filter(AdbcProbe.id == 50)
                    return q._execute_query(session, engine, query, fix_wkb=False, direct_sql_pandas=True)
            finally:
                session.close()
        got, expected = only_null(True), only_null(False)
        pd.testing.assert_frame_equal(got, expected)
        assert pa.RecordBatch.from_pandas(got).schema.equals(pa.RecordBatch.from_pandas(expected).schema, check_metadata=True)

    def test_adbc_is_used_for_scalar_columns(self, engine, monkeypatch):
        calls = []
        real = q._read_sql_adbc
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: calls.append(1) or real(*a))
        fetch(engine, True, *SCALARS)
        assert calls == [1]


    def test_positions_as_integers_match(self, engine):
        cols = [AdbcProbe.id, sa.cast(AdbcProbe.pt.ST_X(), sa.Integer).label("pt_x"),
                sa.cast(AdbcProbe.pt.ST_Z(), sa.Integer).label("pt_z")]
        pd.testing.assert_frame_equal(fetch(engine, True, *cols), fetch(engine, False, *cols))


    def test_numeric_falls_back_to_read_sql(self, engine, monkeypatch):
        expected = fetch(engine, False, AdbcProbe.id, AdbcProbe.amount)
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: pytest.fail("ADBC used for numeric"))
        pd.testing.assert_frame_equal(fetch(engine, True, AdbcProbe.id, AdbcProbe.amount), expected)

    def test_float4_falls_back_to_read_sql(self, engine, monkeypatch):
        expected = fetch(engine, False, AdbcProbe.id, AdbcProbe.single)
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: pytest.fail("ADBC used for float4"))
        pd.testing.assert_frame_equal(fetch(engine, True, AdbcProbe.id, AdbcProbe.single), expected)

    def test_a_terminated_connection_is_replaced(self, engine, database_uri):
        with Flask(__name__).test_request_context():
            key, conn = q._adbc_connection(engine)
            with conn.cursor() as cur:
                cur.execute("SELECT pg_backend_pid()")
                pid = cur.fetchone()[0]
            with sa.create_engine(database_uri).connect() as other:
                other.execute(sa.text(f"SELECT pg_terminate_backend({pid})"))
            assert q._read_sql_adbc("SELECT 1 AS one", engine).column("one").to_pylist() == [1]

    def test_a_cancelled_query_is_not_retried(self, engine, monkeypatch):
        app = Flask(__name__)
        app.config["QUERY_STATEMENT_TIMEOUT_SECONDS"] = 0.1
        calls = []
        real = q._adbc_connection
        monkeypatch.setattr(q, "_adbc_connection", lambda e: calls.append(1) or real(e))
        with app.test_request_context(), pytest.raises(Exception) as err:
            q._read_sql_adbc("SELECT pg_sleep(1)", engine)
        assert getattr(err.value, "sqlstate", None) == "57014"
        assert calls == [1]

    def test_cached_connections_are_bounded(self, database_uri):
        app = Flask(__name__)
        app.config["ADBC_CONNECTIONS_KEPT"] = 2
        engines = [sa.create_engine(f"{database_uri}?application_name=adbc_lru_{i}") for i in range(3)]
        with app.test_request_context():
            q._adbc_connections().clear()
            first = q._adbc_connection(engines[0])[1]
            q._adbc_connection(engines[1])
            q._adbc_connection(engines[0])  # reuse: engines[1] is now the least recently used
            q._adbc_connection(engines[2])
            kept = [k[1] for k in q._adbc_connections()]
        assert kept == [str(engines[0].url), str(engines[2].url)]
        with first.cursor() as cur:  # the reused connection survived the eviction
            cur.execute("SELECT 1")
        for e in engines:
            e.dispose()

    def test_raw_geometry_skips_adbc(self, engine, monkeypatch):
        # ADBC returns PostGIS's binary format; keep geometry on the pd.read_sql path
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: pytest.fail("ADBC used for geometry"))
        fetch(engine, True, AdbcProbe.id, AdbcProbe.pt)

    def test_empty_result_matches(self, engine):
        def empty(use_adbc):
            app = Flask(__name__)
            app.config["QUERY_FETCH_WITH_ADBC"] = use_adbc
            session = sessionmaker(bind=engine)()
            try:
                with app.test_request_context():
                    query = session.query(AdbcProbe).with_entities(*SCALARS).filter(AdbcProbe.id < 0)
                    return q._execute_query(session, engine, query, fix_wkb=False, direct_sql_pandas=True)
            finally:
                session.close()
        got, expected = empty(True), empty(False)
        pd.testing.assert_frame_equal(got, expected)
        # the response's schema (Arrow types and pandas metadata) too
        assert pa.RecordBatch.from_pandas(got).schema.equals(pa.RecordBatch.from_pandas(expected).schema, check_metadata=True)

    def test_the_adbc_flag_accepts_config_strings(self, engine, monkeypatch):
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: pytest.fail("ADBC used with the flag off"))
        for off in ("off", "no", "FALSE", "0"):
            fetch(engine, off, *SCALARS)

    def test_a_failed_query_drops_its_connection(self, engine):
        app = Flask(__name__)
        with app.test_request_context():
            with pytest.raises(Exception):
                q._read_sql_adbc("SELECT * FROM no_such_table", engine)
            key, conn = q._adbc_connection(engine)
            assert q._read_sql_adbc("SELECT 1 AS one", engine).column("one").to_pylist() == [1]




class StrBool(Base):
    __tablename__ = "adbc_str_bool"
    id = sa.Column(sa.BigInteger, primary_key=True)
    label = sa.Column(sa.String)       # varchar
    note = sa.Column(sa.Text)          # text
    label_null = sa.Column(sa.String)  # NULL in every row
    flag = sa.Column(sa.Boolean)
    flag_null = sa.Column(sa.Boolean)  # NULL in every row


STRBOOL_COLUMNS = [StrBool.id, StrBool.label, StrBool.note, StrBool.label_null, StrBool.flag, StrBool.flag_null]


@pytest.fixture(scope="module")
def strbool_engine(database_uri):
    e = sa.create_engine(database_uri)
    StrBool.__table__.drop(e, checkfirst=True)
    StrBool.__table__.create(e)
    with e.begin() as conn:
        conn.execute(StrBool.__table__.insert(), [
            dict(id=1, label="L2/3 IT", note="first", label_null=None, flag=True, flag_null=None),
            dict(id=2, label=None, note="naïve ✓", label_null=None, flag=None, flag_null=None),
            dict(id=3, label="", note=None, label_null=None, flag=False, flag_null=None),
        ])
    yield e
    StrBool.__table__.drop(e, checkfirst=True)
    e.dispose()


class TestStringsAndBooleans:
    """Strings and booleans are common, so they must take the ADBC path (no fallback) and come out
    exactly as pd.read_sql returned them: pandas string / boolean, Arrow string / bool."""

    def fetch(self, engine, use_adbc, empty=False, monkeypatch=None):
        app = Flask(__name__)
        app.config["QUERY_FETCH_WITH_ADBC"] = use_adbc
        session = sessionmaker(bind=engine)()
        try:
            with app.test_request_context():
                query = session.query(StrBool).with_entities(*STRBOOL_COLUMNS).order_by(StrBool.id)
                if empty:
                    query = query.filter(StrBool.id < 0)
                return q._execute_query(session, engine, query, fix_wkb=False, direct_sql_pandas=True)
        finally:
            session.close()

    def test_they_use_adbc(self, strbool_engine, monkeypatch):
        calls = []
        real = q._read_sql_adbc
        monkeypatch.setattr(q, "_read_sql_adbc", lambda *a: calls.append(1) or real(*a))
        self.fetch(strbool_engine, True)
        assert calls == [1]

    def test_types_and_values(self, strbool_engine):
        df = self.fetch(strbool_engine, True)
        assert {c: str(df[c].dtype) for c in ("label", "note", "label_null", "flag", "flag_null")} == {
            "label": "string", "note": "string", "label_null": "string", "flag": "boolean", "flag_null": "boolean"}
        assert df["label"].tolist() == ["L2/3 IT", pd.NA, ""]
        assert df["note"].tolist() == ["first", "naïve ✓", pd.NA]
        assert df["flag"].tolist() == [True, pd.NA, False]
        schema = pa.RecordBatch.from_pandas(df).schema  # what the response carries
        assert [str(schema.field(c).type) for c in ("label", "note", "flag")] == ["string", "string", "bool"]
        client = pa.Table.from_batches([pa.RecordBatch.from_pandas(df)]).to_pandas()
        assert str(client["label"].dtype) == "string" and str(client["flag"].dtype) == "boolean"

    @pytest.mark.parametrize("empty", [False, True])
    def test_same_as_read_sql(self, strbool_engine, empty):
        new, old = self.fetch(strbool_engine, True, empty), self.fetch(strbool_engine, False, empty)
        pd.testing.assert_frame_equal(new, old)
        assert pa.RecordBatch.from_pandas(new).schema.equals(pa.RecordBatch.from_pandas(old).schema, check_metadata=True)


class TestWebRequestPools:
    def manager_with(self, names):
        m = database.DatabaseConnectionManager()
        for n in names:
            m._engines[n] = sa.create_engine("sqlite://")
        return m

    def test_trim_keeps_the_most_recently_used(self, monkeypatch):
        m = self.manager_with(["a", "b", "c"])
        m._engines.move_to_end("a")  # what get_engine does on a hit
        m.trim(2)
        assert list(m._engines) == ["c", "a"]

    def app_with_lifecycle(self, kept=4):
        app = Flask(__name__)
        app.config["WEB_DB_ENGINES_KEPT"] = kept
        database.init_db_lifecycle(app)  # the same hooks create_app registers
        app.add_url_rule("/ping", "ping", lambda: "ok")
        return app

    def test_session_scope_marks_its_engine_recent(self, monkeypatch):
        m = self.manager_with(["a", "b", "c"])
        with m.session_scope("a"):  # the path most code uses, through get_session_factory
            pass
        with m.session_scope("a"):  # a cache hit must still refresh recency
            pass
        m.trim(2)
        assert set(m._engines) == {"c", "a"}

    def test_a_request_keeps_pools(self, monkeypatch):
        # through a real request: Flask pops the request context before the app-context
        # teardown runs, so the hook must not rely on has_request_context()
        m = self.manager_with(["a", "b"])
        monkeypatch.setattr(database, "db_manager", m)
        assert self.app_with_lifecycle().test_client().get("/ping").status_code == 200
        assert list(m._engines) == ["a", "b"]

    def test_a_request_bounds_pools(self, monkeypatch):
        m = self.manager_with(["a", "b", "c"])
        monkeypatch.setattr(database, "db_manager", m)
        self.app_with_lifecycle(kept=1).test_client().get("/ping")
        assert list(m._engines) == ["c"]

    def test_a_non_request_app_context_still_disposes(self, monkeypatch):
        m = self.manager_with(["a", "b"])
        monkeypatch.setattr(database, "db_manager", m)
        with self.app_with_lifecycle().app_context():
            pass
        assert not m._engines

    def test_a_task_that_keeps_connections_keeps_them(self, monkeypatch):
        from flask import g
        m = self.manager_with(["a", "b"])
        monkeypatch.setattr(database, "db_manager", m)
        with self.app_with_lifecycle().app_context():
            setattr(g, database.KEEP_DB_CONNECTIONS_FLAG, True)
        assert list(m._engines) == ["a", "b"]


class TestPrewarm:
    def test_web_prewarm_accepts_config_strings_and_the_environment(self, monkeypatch):
        from materializationengine import prewarm
        monkeypatch.setattr(database, "reset_database_connections", lambda close_connections: None)
        monkeypatch.setattr(prewarm, "prewarm_worker", lambda app: pytest.fail("prewarm is disabled"))
        app = Flask(__name__)
        app.config["WEB_PREWARM"] = "False"
        prewarm.after_fork(app)
        app.config.pop("WEB_PREWARM")
        monkeypatch.setenv("WEB_PREWARM", "0")
        prewarm.after_fork(app)

    def test_virtual_versions_resolve_to_their_parent_database(self, test_app, aligned_volume_name):
        from dynamicannotationdb.models import AnalysisVersion
        from materializationengine import prewarm
        now = datetime.datetime.utcnow()
        with test_app.application.app_context():
            with database.db_manager.session_scope(aligned_volume_name) as s:
                parent = AnalysisVersion(datastack="prewarm_parent", version=7, time_stamp=now, valid=True,
                                         expires_on=now, status="AVAILABLE", is_merged=True)
                s.add(parent)
                s.flush()
                s.add(AnalysisVersion(datastack="prewarm_virtual", version=7, time_stamp=now, valid=True,
                                      expires_on=now, parent_version=parent.id, status="AVAILABLE", is_merged=True))
            try:
                assert prewarm._recent_version_databases(aligned_volume_name, "prewarm_virtual", 2) == [
                    "prewarm_parent__mat7"]
            finally:
                with database.db_manager.session_scope(aligned_volume_name) as s:
                    s.query(AnalysisVersion).filter(
                        AnalysisVersion.datastack.in_(["prewarm_parent", "prewarm_virtual"])).delete(
                        synchronize_session=False)

    def test_after_fork_forgets_inherited_clients_even_without_prewarm(self, monkeypatch):
        from materializationengine import prewarm
        calls = []
        monkeypatch.setattr(database, "reset_database_connections", lambda close_connections: calls.append(close_connections))
        monkeypatch.setattr(prewarm, "prewarm_worker", lambda app: pytest.fail("prewarm is disabled"))
        app = Flask(__name__)
        app.config["WEB_PREWARM"] = False
        prewarm.after_fork(app)
        assert calls == [False]  # forget without closing: the sockets are shared with the master


    def test_register_prewarm_is_a_no_op_outside_uwsgi(self):
        from materializationengine.prewarm import register_prewarm
        register_prewarm(Flask(__name__))  # uwsgidecorators is not importable here


class TestSessionTransactions:
    """Cached DynamicAnnotationInterface sessions must not keep a transaction (and its table locks)
    open after the request that used them."""

    def idle_in_transaction(self, database_uri):
        e = sa.create_engine(database_uri)
        try:
            with e.connect() as c:
                return c.execute(sa.text(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() "
                    "AND state = 'idle in transaction'")).scalar()
        finally:
            e.dispose()

    def test_a_request_ends_the_cached_session_transaction(self, database_uri, aligned_volume_name, monkeypatch):
        # a separate app, so the request gets its own app context and its teardown runs when the
        # request returns (not when some enclosing context exits)
        app = Flask(__name__)
        app.config["SQLALCHEMY_DATABASE_URI"] = database_uri
        database.init_db_lifecycle(app)
        monkeypatch.setattr(database, "dynamic_annotation_cache", database.DynamicMaterializationCache())

        def handler():
            db = database.dynamic_annotation_cache.get_db(aligned_volume_name)
            # what QueryManager does: read through the client's scoped session
            db.database.session.execute(sa.text("SELECT count(*) FROM annotation_table_metadata")).scalar()
            return str(self.idle_in_transaction(database_uri))

        app.add_url_rule("/read", "read", handler)
        before = self.idle_in_transaction(database_uri)
        response = app.test_client().get("/read")
        assert response.status_code == 200
        assert int(response.get_data()) == before + 1  # the transaction was open during the request
        assert self.idle_in_transaction(database_uri) == before  # and ended when it returned
