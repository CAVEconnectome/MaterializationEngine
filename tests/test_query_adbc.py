"""The ADBC fetch path in client/query.py returns the same DataFrame as pd.read_sql, and API
requests keep (but bound) their connection pools."""
import datetime

import pandas as pd
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
            dict(id=i, created=t0 + datetime.timedelta(seconds=i), valid=i % 3 != 0, small=i * 7,
                 size=i / 3, root_id=864691136031763387 + i, name=f"n{i}", amount=i,
                 pt=f"POINTZ({i} {2 * i} {3 * i})")
            for i in range(1, 50)
        ] + [dict(id=50, created=None, valid=None, small=None, size=None, root_id=None, name=None,
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


SCALARS = [AdbcProbe.id, AdbcProbe.created, AdbcProbe.valid, AdbcProbe.small, AdbcProbe.size,
           AdbcProbe.root_id, AdbcProbe.name]


class TestAdbcFetch:
    def test_adbc_matches_read_sql(self, engine):
        expected = fetch(engine, False, *SCALARS)
        got = fetch(engine, True, *SCALARS)
        assert dict(got.dtypes.astype(str)) == dict(expected.dtypes.astype(str))
        pd.testing.assert_frame_equal(got, expected)


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
        assert len(got) == len(expected) == 0
        assert list(got.columns) == list(expected.columns)


    def test_a_failed_query_drops_its_connection(self, engine):
        app = Flask(__name__)
        with app.test_request_context():
            with pytest.raises(Exception):
                q._read_sql_adbc("SELECT * FROM no_such_table", engine)
            key, conn = q._adbc_connection(engine)
            assert q._read_sql_adbc("SELECT 1 AS one", engine).column("one").to_pylist() == [1]




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

    def test_request_teardown_keeps_pools(self, monkeypatch):
        m = self.manager_with(["a", "b"])
        monkeypatch.setattr(database, "db_manager", m)
        app = Flask(__name__)
        app.config["WEB_DB_ENGINES_KEPT"] = 4
        with app.test_request_context():
            database.cleanup_after_app_context()
        assert list(m._engines) == ["a", "b"]

    def test_request_teardown_bounds_pools(self, monkeypatch):
        m = self.manager_with(["a", "b", "c"])
        monkeypatch.setattr(database, "db_manager", m)
        app = Flask(__name__)
        app.config["WEB_DB_ENGINES_KEPT"] = 1
        with app.test_request_context():
            database.cleanup_after_app_context()
        assert list(m._engines) == ["c"]

    def test_non_request_context_still_disposes(self, monkeypatch):
        m = self.manager_with(["a", "b"])
        monkeypatch.setattr(database, "db_manager", m)
        with Flask(__name__).app_context():
            database.cleanup_after_app_context()
        assert not m._engines


class TestPrewarm:
    def test_register_prewarm_is_a_no_op_outside_uwsgi(self):
        from materializationengine.prewarm import register_prewarm
        register_prewarm(Flask(__name__))  # uwsgidecorators is not importable here
