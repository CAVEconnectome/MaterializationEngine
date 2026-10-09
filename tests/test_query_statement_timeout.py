"""statement_timeout on the query/precomputed execution path (client/query.py)."""

import pytest
import sqlalchemy as sa
from flask import Flask
from sqlalchemy.orm import sessionmaker
from werkzeug.exceptions import GatewayTimeout

from materializationengine.blueprints.client import query as q


@pytest.fixture
def engine(database_uri):
    e = sa.create_engine(database_uri, pool_size=1, max_overflow=0)
    yield e
    e.dispose()


def app_with(timeout_seconds):
    app = Flask(__name__)
    app.config["QUERY_STATEMENT_TIMEOUT_SECONDS"] = timeout_seconds
    return app


def sleep_query(session, seconds):
    return session.query(sa.func.pg_sleep(seconds).label("slept"))


def server_timeout(engine):
    with engine.connect() as conn:
        return conn.execute(sa.text("SHOW statement_timeout")).scalar()


class TestStatementTimeout:
    def test_default_is_480s_and_zero_disables(self):
        with Flask(__name__).app_context():
            assert q.statement_timeout_ms() == 480000
        with app_with(0).app_context():
            assert q.statement_timeout_ms() == 0

    @pytest.mark.parametrize("direct_sql_pandas", [False, True])  # COPY path / pd.read_sql path
    def test_slow_query_is_cancelled_with_a_504(self, engine, direct_sql_pandas):
        session = sessionmaker(bind=engine)()
        try:
            with app_with(0.2).test_request_context(), pytest.raises(GatewayTimeout, match="0s limit|limit"):
                q._execute_query(session, engine, sleep_query(session, 2), direct_sql_pandas=direct_sql_pandas)
        finally:
            session.rollback()
            session.close()
        # SET LOCAL: the pooled connection is back to the server default afterwards
        assert server_timeout(engine) == "0"

    def test_count_path_is_cancelled_too(self, engine):
        session = sessionmaker(bind=engine)()
        try:
            with app_with(0.2).test_request_context(), pytest.raises(GatewayTimeout):
                q._execute_query(session, engine, sleep_query(session, 2), get_count=True)
        finally:
            session.rollback()
            session.close()

    def test_copy_path_runs_under_the_timeout(self, engine):
        with app_with(5).test_request_context():
            df = q.read_sql_tmpfile("SELECT current_setting('statement_timeout') AS t", engine)
        assert df["t"].astype(str).tolist() == ["5s"]

    def test_session_path_runs_under_the_timeout(self, engine):
        session = sessionmaker(bind=engine)()
        try:
            with app_with(5).test_request_context():
                q._set_local_statement_timeout(lambda sql: session.execute(sa.text(sql)))
                assert session.execute(sa.text("SHOW statement_timeout")).scalar() == "5s"
            session.rollback()
            assert session.execute(sa.text("SHOW statement_timeout")).scalar() == "0"
        finally:
            session.close()

    def test_disabled_sets_nothing(self, engine):
        with app_with(0).test_request_context():
            df = q.read_sql_tmpfile("SELECT current_setting('statement_timeout') AS t", engine)
        assert df["t"].astype(str).tolist() == ["0"]

    def test_copy_path_returns_its_connection_to_the_pool(self, engine):
        # pool_size=1, max_overflow=0: a leaked raw connection would block the second call
        with app_with(5).test_request_context():
            for _ in range(3):
                assert q.read_sql_tmpfile("SELECT 1 AS one", engine)["one"].tolist() == [1]
