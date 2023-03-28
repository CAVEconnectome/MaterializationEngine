from materializationengine.database import (
    create_session,
    dynamic_annotation_cache,
    get_sql_url_params,
    ping_connection,
    reflect_tables,
    sqlalchemy_cache,
)
from sqlalchemy.orm import scoped_session
from sqlalchemy.engine.base import Engine
from dynamicannotationdb import DynamicAnnotationInterface
import logging
import pytest


class TestDatabaseUtils:
    def test_get_sql_url_params(self, database_uri):
        url_mapping = get_sql_url_params(database_uri)

        assert url_mapping["user"] == "postgres"
        assert url_mapping["password"] == "postgres"
        assert url_mapping["dbname"] == "test_aligned_volume"
        assert url_mapping["host"] == "localhost"
        assert url_mapping["port"] == 5432

    def test_reflect_tables(self, database_uri):
        sql_base = database_uri.rpartition("/")[0]
        database_name = database_uri.rpartition("/")[-1]
        tables = reflect_tables(sql_base, database_name)
        logging.info(tables)
        assert set(tables) == set(
            [
                "spatial_ref_sys",
                "annotation_table_metadata",
                "segmentation_table_metadata",
                "combined_table_metadata",
                "analysisversion",
                "analysistables",
                "analysisviews",
                "geography_columns",
                "geometry_columns",
                "test_synapse_table",
                "test_synapse_table__test_pcg",
                "version_error"
            ]
        )


class TestCreateSession:
    @pytest.fixture(autouse=True)
    def setup_method(self, database_uri):
        self.session, self.engine = create_session(database_uri)

    def test_ping_connection(self):
        is_connected = ping_connection(self.session)
        assert is_connected == True

    def teardown_method(self):
        self.session.close()
        self.engine.dispose()


class TestSqlAlchemyCache:
    def test_get_session(self, test_app, aligned_volume_name):
        self.cached_session = sqlalchemy_cache.get(aligned_volume_name)
        assert isinstance(self.cached_session, scoped_session)

    def test_get_engine(self, test_app, aligned_volume_name):
        self.cached_engine = sqlalchemy_cache.get_engine(aligned_volume_name)
        assert isinstance(self.cached_engine, Engine)


class TestDynamicMaterializationCache:
    def test_get_mat_client(self, test_app, aligned_volume_name):
        self.mat_client = dynamic_annotation_cache.get_db(aligned_volume_name)
        assert isinstance(self.mat_client, DynamicAnnotationInterface)
