"""An upload removes its staging tables once they are in production."""

import uuid
from unittest import mock

import pytest
import sqlalchemy as sa
from flask import Flask
from sqlalchemy.engine.url import make_url

from materializationengine.blueprints.upload import tasks

ANNO = "synapses_cleanup_test"
SEG = f"{ANNO}__minnie3_v1"


@pytest.fixture
def staging_engine(mat_metadata):
    """A scratch database with the metadata tables (and their foreign keys) staging has."""
    server_url = make_url(mat_metadata["sql_uri"])
    base = (f"{server_url.drivername}://{server_url.username}:{server_url.password}"
            f"@{server_url.host}:{server_url.port or 5432}")
    name = f"cleanup_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    with engine.begin() as conn:
        conn.execute("CREATE TABLE annotation_table_metadata (table_name varchar PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE segmentation_table_metadata (table_name varchar PRIMARY KEY,"
            " annotation_table varchar REFERENCES annotation_table_metadata(table_name))"
        )
        conn.execute(
            "CREATE TABLE combined_table_metadata (table_name varchar PRIMARY KEY,"
            " annotation_table varchar REFERENCES annotation_table_metadata(table_name),"
            " reference_table varchar REFERENCES annotation_table_metadata(table_name))"
        )
        for table in (ANNO, "other_upload"):
            conn.execute(f'CREATE TABLE "{table}" (id bigint PRIMARY KEY)')
            conn.execute(f'CREATE TABLE "{table}__minnie3_v1" (id bigint)')
            conn.execute(sa.text("INSERT INTO annotation_table_metadata VALUES (:t)"), {"t": table})
            conn.execute(
                sa.text("INSERT INTO segmentation_table_metadata VALUES (:s, :t)"),
                {"s": f"{table}__minnie3_v1", "t": table},
            )
    yield engine
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


def _state(engine):
    with engine.connect() as conn:
        tables = {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'")}
        anno = {r[0] for r in conn.execute("SELECT table_name FROM annotation_table_metadata")}
        seg = {r[0] for r in conn.execute("SELECT table_name FROM segmentation_table_metadata")}
    return tables, anno, seg


def _transfer_result(seg_success=True, status="success"):
    return {
        "status": status,
        "job_id_for_status": "job1",
        "tables_transferred": {
            "annotation_table": {"name": ANNO, "success": True, "rows_transferred": 10},
            "segmentation_table": {"name": SEG, "success": seg_success, "rows_transferred": 10},
        },
    }


@pytest.fixture
def run_cleanup(staging_engine):
    def run(result):
        with mock.patch.object(tasks, "get_config_param", return_value="staging"), \
                mock.patch.object(tasks.db_manager, "get_engine", return_value=staging_engine), \
                mock.patch.object(tasks, "update_job_status") as status:
            return tasks.cleanup_staging_tables.run(result), status
    return run


class TestCleanupStagingTables:
    def test_drops_the_uploads_tables_and_metadata_only(self, staging_engine, run_cleanup):
        result, status = run_cleanup(_transfer_result())

        assert result == {"status": "success", "dropped": [SEG, ANNO], "errors": {}}
        tables, anno, seg = _state(staging_engine)
        assert ANNO not in tables and SEG not in tables
        assert ANNO not in anno and SEG not in seg
        # another upload's staging tables are untouched
        assert {"other_upload", "other_upload__minnie3_v1"} <= tables
        assert "other_upload" in anno and "other_upload__minnie3_v1" in seg
        status.assert_called_once_with("job1", {"staging_cleanup": "done"})

    def test_rerun_is_harmless(self, staging_engine, run_cleanup):
        run_cleanup(_transfer_result())
        result, _ = run_cleanup(_transfer_result())
        assert result["status"] == "success" and not result["errors"]

    def test_unsuccessful_transfer_drops_nothing(self, staging_engine, run_cleanup):
        before = _state(staging_engine)
        result, status = run_cleanup(_transfer_result(status="error"))
        assert result["status"] == "skipped"
        assert _state(staging_engine) == before
        status.assert_not_called()

    def test_untransferred_table_keeps_everything(self, staging_engine, run_cleanup):
        # The segmentation table's data exists only in staging; dropping either table
        # would lose what is needed to redo the transfer
        before = _state(staging_engine)
        result, status = run_cleanup(_transfer_result(seg_success=False))
        assert result["status"] == "skipped"
        assert _state(staging_engine) == before
        status.assert_called_once_with("job1", {"staging_cleanup": f"skipped: {[SEG]} not transferred"})

    def test_other_pcg_versions_segmentation_table_blocks_the_annotation_drop(self, staging_engine, run_cleanup):
        other_version = f"{ANNO}__minnie3_v2"
        with staging_engine.begin() as conn:
            conn.execute(f'CREATE TABLE "{other_version}" (id bigint)')
            conn.execute(
                sa.text("INSERT INTO segmentation_table_metadata VALUES (:s, :t)"),
                {"s": other_version, "t": ANNO},
            )
        result, _ = run_cleanup(_transfer_result())
        tables, anno, seg = _state(staging_engine)
        assert ANNO in tables and ANNO in anno and other_version in seg
        assert result["dropped"] == [SEG] and ANNO in result["errors"]

    def test_table_used_as_a_reference_is_kept(self, staging_engine, run_cleanup):
        with staging_engine.begin() as conn:
            conn.execute(
                sa.text("INSERT INTO combined_table_metadata VALUES ('combo', 'other_upload', :t)"),
                {"t": ANNO},
            )
        result, _ = run_cleanup(_transfer_result())
        tables, anno, _ = _state(staging_engine)
        assert ANNO in tables and ANNO in anno
        assert SEG not in tables
        assert result["dropped"] == [SEG] and ANNO in result["errors"]


class TestCleanupIsTheLastStep:
    def test_upload_chain_ends_with_cleanup(self):
        app = Flask(__name__)
        with app.app_context(), mock.patch.object(tasks, "chain") as chain, \
                mock.patch.object(tasks, "is_upload_cancelled", return_value=False), \
                mock.patch.object(tasks, "update_job_status"):
            tasks.process_and_upload.run(
                "gs://bucket/file.csv",
                {"metadata": {"table_name": "t", "schema_type": "synapse"}, "column_mapping": {}},
                {"datastack": "minnie65_phase3_v1"},
                job_id="job1",
            )
        steps = [sig.task for sig in chain.call_args.args]
        assert steps[-2:] == ["workflow:transfer_to_production", "workflow:cleanup_staging_tables"]
