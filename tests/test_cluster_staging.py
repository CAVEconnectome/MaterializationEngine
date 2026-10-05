"""Clustering the staging tables by id before transfer_to_production means production
tables are loaded in id order, since pg_dump copies rows in physical order."""

import random
import shutil
import uuid
from unittest import mock

import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from materializationengine.blueprints.upload import tasks

pytestmark = pytest.mark.skipif(
    not (shutil.which("pg_dump") and shutil.which("psql")),
    reason="pg_dump and psql are needed to test the transfer",
)


@pytest.fixture
def source_and_target(mat_metadata):
    """Two empty scratch databases on the test server, dropped afterwards."""
    server_url = make_url(mat_metadata["sql_uri"])

    def url_for(database):
        return make_url(f"{server_url.drivername}://{server_url.username}:{server_url.password}"
                        f"@{server_url.host}:{server_url.port or 5432}/{database}")

    names = [f"cluster_test_{kind}_{uuid.uuid4().hex[:8]}" for kind in ("src", "dst")]
    admin = sa.create_engine(url_for("postgres"), isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        for name in names:
            conn.execute(f"CREATE DATABASE {name}")
    engines = [sa.create_engine(url_for(name)) for name in names]

    yield server_url, names, engines

    for engine in engines:
        engine.dispose()
    with admin.connect() as conn:
        for name in names:
            conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


def _create_table(engine, table_name):
    with engine.begin() as conn:
        conn.execute(f'CREATE TABLE "{table_name}" (id bigint PRIMARY KEY, v text)')
        conn.execute(f'CREATE INDEX "ix_{table_name}_v" ON "{table_name}" (v)')


def _load_shuffled(engine, table_name, n=20000):
    """Create the table and insert ids 1..n in shuffled order; return the ids."""
    ids = list(range(1, n + 1))
    random.Random(0).shuffle(ids)
    _create_table(engine, table_name)
    with engine.begin() as conn:
        conn.execute(
            sa.text(f'INSERT INTO "{table_name}" (id, v) SELECT x, md5(x::text) FROM unnest(:ids) AS x'),
            {"ids": ids},
        )
    assert _physical_ids(engine, table_name) != sorted(ids)
    return ids


def _physical_ids(engine, table_name):
    with engine.connect() as conn:
        return [row[0] for row in conn.execute(f'SELECT id FROM "{table_name}" ORDER BY ctid')]


def _is_clustered_on_pkey(engine, table_name):
    with engine.connect() as conn:
        return conn.execute(
            sa.text(
                "SELECT i.indisclustered FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid "
                "WHERE c.relname = :t AND i.indisprimary"
            ),
            {"t": table_name},
        ).scalar()


class TestClusterStagingTables:
    """CLUSTER on staging and the pg_dump transfer keep production tables in id order."""

    # A capitalised name checks the identifiers are quoted
    @pytest.mark.parametrize(
        "table_name",
        ["synapse_cluster_test", "Synapse_Cluster_Test"],
        ids=["lowercase", "mixed_case"],
    )
    def test_cluster_orders_rows_by_id(self, source_and_target, table_name):
        _, _, (source, _) = source_and_target
        ids = _load_shuffled(source, table_name)

        assert tasks.cluster_table_by_id(table_name, source) is True
        assert _physical_ids(source, table_name) == sorted(ids)

        assert tasks.mark_clustered_on_primary_key(table_name, source) is True
        assert _is_clustered_on_pkey(source, table_name) is True

    # transfer_table_using_pg_dump builds unquoted SQL, so it only handles lowercase names
    def test_clustered_staging_table_transfers_in_id_order(self, source_and_target):
        server_url, (source_db, target_db), (source, target) = source_and_target
        table_name = "synapse_cluster_test"
        ids = _load_shuffled(source, table_name)
        _create_table(target, table_name)

        assert tasks.cluster_table_by_id(table_name, source) is True

        rows = tasks.transfer_table_using_pg_dump(
            table_name=table_name,
            source_db=source_db,
            target_db=target_db,
            db_info={
                "host": server_url.host,
                "port": server_url.port or 5432,
                "user": server_url.username,
                "password": server_url.password,
            },
            drop_indices=False,
            rebuild_indices=False,
            engine=target,
            source_engine=source,
        )
        assert rows == len(ids)
        assert _physical_ids(target, table_name) == sorted(ids)

        assert tasks.mark_clustered_on_primary_key(table_name, target) is True
        assert _is_clustered_on_pkey(target, table_name) is True

    def test_cluster_skips_missing_table(self, source_and_target):
        _, _, (source, _) = source_and_target
        assert tasks.cluster_table_by_id("does_not_exist", source) is False

    # The staging segmentation table has no indexes when it is clustered
    @pytest.mark.parametrize(
        "table_name",
        ["synapse_cluster_test__minnie3_v1", "Synapse_Cluster_Test__minnie3_v1"],
        ids=["lowercase", "mixed_case"],
    )
    def test_cluster_orders_table_without_indexes_and_leaves_it_without(self, source_and_target, table_name):
        _, _, (source, _) = source_and_target
        ids = list(range(1, 20001))
        random.Random(0).shuffle(ids)
        with source.begin() as conn:
            conn.execute(f'CREATE TABLE "{table_name}" (id bigint NOT NULL, v text)')
            conn.execute(
                sa.text(f'INSERT INTO "{table_name}" (id, v) SELECT x, md5(x::text) FROM unnest(:ids) AS x'),
                {"ids": ids},
            )

        assert tasks.cluster_table_by_id(table_name, source) is True
        assert _physical_ids(source, table_name) == sorted(ids)
        with source.connect() as conn:
            assert conn.execute(
                sa.text("SELECT count(*) FROM pg_index WHERE indrelid = CAST(:t AS regclass)"),
                {"t": f'"{table_name}"'},
            ).scalar() == 0
        assert tasks.mark_clustered_on_primary_key(table_name, source) is False

    def test_cluster_staging_tables_clusters_both_tables_and_passes_result_through(self):
        monitor_result = {
            "datastack_info": {"segmentation_source": "graphene://https://example/segmentation/table/minnie3_v1"},
            "table_name": "synapse_cluster_test",
            "job_id_for_status": "job1",
        }
        with mock.patch.object(tasks, "get_config_param", return_value="staging"), mock.patch.object(
            tasks.db_manager, "get_engine"
        ), mock.patch.object(tasks, "update_job_status"), mock.patch.object(
            tasks, "is_upload_cancelled", return_value=False
        ), mock.patch.object(
            tasks, "cluster_table_by_id", side_effect=[True, RuntimeError("disk full")]
        ) as mock_cluster:
            result = tasks.cluster_staging_tables.run(monitor_result)

        assert result is monitor_result
        assert [call.args[0] for call in mock_cluster.call_args_list] == [
            "synapse_cluster_test",
            "synapse_cluster_test__minnie3_v1",
        ]
