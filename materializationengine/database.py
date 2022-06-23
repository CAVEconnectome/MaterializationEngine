from urllib.parse import urlparse

from celery.utils.log import get_task_logger
from dynamicannotationdb import DynamicAnnotationInterface
from flask import current_app
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import scoped_session, sessionmaker

from materializationengine.celery_worker import celery_logger
from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)


def create_session(sql_uri: str = None):
    engine = create_engine(
        sql_uri, pool_recycle=3600, pool_size=20, max_overflow=50, pool_pre_ping=True
    )
    Session = scoped_session(
        sessionmaker(bind=engine, autocommit=False, autoflush=False)
    )
    session = Session()
    return session, engine


def get_sql_url_params(sql_url):
    if not isinstance(sql_url, str):
        sql_url = str(sql_url)
    result = urlparse(sql_url)
    url_mapping = {
        "user": result.username,
        "password": result.password,
        "dbname": result.path[1:],
        "host": result.hostname,
        "port": result.port,
    }
    return url_mapping


def reflect_tables(sql_base, database_name):
    sql_uri = f"{sql_base}/{database_name}"
    engine = create_engine(sql_uri)
    meta = MetaData(engine)
    meta.reflect(views=True)
    tables = [table for table in meta.tables]
    engine.dispose()
    return tables


def ping_connection(session):
    is_database_working = True
    try:
        # to check database we will execute raw query
        session.execute("SELECT 1")
    except Exception as e:
        celery_logger.warn(e)
        is_database_working = False
    return is_database_working


class SqlAlchemyCache:
    def __init__(self):
        self._engines = {}
        self._sessions = {}

    def get_engine(self, aligned_volume: str):
        if aligned_volume not in self._engines:
            SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
            self._engines[aligned_volume] = create_engine(
                sql_uri,
                pool_recycle=3600,
                pool_size=20,
                max_overflow=50,
                pool_pre_ping=True,
            )
        return self._engines[aligned_volume]

    def get(self, aligned_volume: str):
        if aligned_volume not in self._sessions:
            session = self._create_session(aligned_volume)
        session = self._sessions[aligned_volume]
        connection_ok = ping_connection(session)

        if not connection_ok:
            return self._create_session(aligned_volume)
        return session

    def _create_session(self, aligned_volume: str):
        engine = self.get_engine(aligned_volume)
        Session = scoped_session(sessionmaker(bind=engine))
        self._sessions[aligned_volume] = Session
        return self._sessions[aligned_volume]

    def invalidate_cache(self):
        self._engines = {}
        self._sessions = {}


class DynamicMaterializationCache:
    def __init__(self):
        self._clients = {}

    def get_db(self, database: str) -> DynamicAnnotationInterface:
        if database not in self._clients:
            db_client = self._get_mat_client(database)
        db_client = self._clients[database]

        connection_ok = ping_connection(db_client.database.cached_session)

        if not connection_ok:
            db_client = self._get_mat_client(database)

        return self._clients[database]

    def _get_mat_client(self, database: str):
        sql_uri_config = get_config_param("SQLALCHEMY_DATABASE_URI")
        mat_client = DynamicAnnotationInterface(sql_uri_config, database)
        self._clients[database] = mat_client
        return self._clients[database]

    def invalidate_cache(self):
        self._clients = {}


dynamic_annotation_cache = DynamicMaterializationCache()
sqlalchemy_cache = SqlAlchemyCache()
