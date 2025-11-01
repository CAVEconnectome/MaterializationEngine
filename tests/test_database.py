import logging

import pytest
from dynamicannotationdb import DynamicAnnotationInterface
from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session

from materializationengine.database import (
    db_manager,
    dynamic_annotation_cache,
    get_sql_url_params,
    ping_connection,
    reflect_tables,
)


class TestDatabaseUtils:
    def test_get_sql_url_params(self, database_uri):
        url_mapping = get_sql_url_params(database_uri)

        assert url_mapping["user"] == "postgres"
        assert url_mapping["password"] == "postgres"
        assert url_mapping["dbname"] == "test_aligned_volume"
        assert url_mapping["host"] == "localhost"
        assert url_mapping["port"] == 5432

    def test_reflect_tables(self, database_uri, aligned_volume_name):
        sql_base = database_uri.rpartition("/")[0]
        tables = reflect_tables(sql_base, aligned_volume_name)
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


class TestDatabaseConnection:
    def test_ping_connection(self, database_uri, test_app):
        with db_manager.session_scope(database_uri.rpartition('/')[-1]) as session:
            is_connected = ping_connection(session)
            assert is_connected == True

class TestSqlAlchemyCache:
    def test_get_session(self, test_app, aligned_volume_name):
        with db_manager.session_scope(aligned_volume_name) as session:
            self.cached_session = session
            assert isinstance(self.cached_session, Session)

    def test_get_engine(self, test_app, aligned_volume_name):
        self.cached_engine = db_manager.get_engine(aligned_volume_name)
        assert isinstance(self.cached_engine, Engine)


class TestDynamicMaterializationCache:
    def test_get_mat_client(self, test_app, aligned_volume_name):
        self.mat_client = dynamic_annotation_cache.get_db(aligned_volume_name)
        assert isinstance(self.mat_client, DynamicAnnotationInterface)
