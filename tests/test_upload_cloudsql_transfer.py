"""The transfer to production through GCS (method "cloudsql"), and skipping CLUSTER for
a staging table that is already in id order.

Cloud SQL's exports and imports are simulated with COPY on the test PostGIS server: an
export writes the query's rows as CSV into a dict keyed by URI, an import COPYs that
CSV into the named columns of the target table, as Cloud SQL does.
"""

import io
import random
from unittest import mock

import pytest
import sqlalchemy as sa
from flask import Flask

from materializationengine.blueprints.upload import tasks
from materializationengine.cloudsql_admin import CloudSQLAdminError
from materializationengine.rayjobs.entrypoints import bulk_upload
from materializationengine.upload_state import store
from materializationengine.upload_state.status_sink import StoreSink, use_sink
from tests.test_cluster_staging import _physical_ids, source_and_target  # noqa: F401
from tests.test_upload_long_tasks import FakeRedis
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401

TABLE = "synapses_transfer_test"
SEG_TABLE = f"{TABLE}__minnie3_v1"


class FakeRedisWithDelete(FakeRedis):
    def delete(self, *keys):
        for key in keys:
            self.data.pop(key, None)


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(SQL_INSTANCE_NAME="instance", STAGING_DATABASE_NAME="staging")
    with app.app_context():
        yield app


@pytest.fixture
def redis(app):
    redis = FakeRedisWithDelete()
    with mock.patch.object(tasks, "REDIS_CLIENT", redis):
        yield redis


class TestRunCloudSQLOperation:
    """Each export or import runs once, is waited on after a restart, and is run again
    only when it failed; prepare (the truncate) never runs under a live import."""

    def run(self, **kwargs):
        self.prepared, self.started = [], []

        def start(on_started):
            self.started.append(True)
            on_started("op-new")

        return tasks._run_cloudsql_operation(
            JOB, "transfer_import:t", "IMPORT", "gs://b/f.csv", "prod", "t",
            start=start, prepare=lambda: self.prepared.append(True), **kwargs,
        )

    def test_a_fresh_operation_prepares_then_runs(self, redis):
        assert self.run() == "ran"
        assert self.prepared and self.started
        state = tasks.get_import_state(JOB, "transfer_import:t")
        assert (state["operation"], state["status"]) == ("op-new", "done")
        assert tasks.get_import_state(JOB) == {}  # the staging import's state is separate

    def test_a_finished_operation_is_not_run_again(self, redis):
        tasks.set_import_state(JOB, "transfer_import:t", operation="op-1", status="done")
        assert self.run() == "done"
        assert not self.prepared and not self.started

    def test_a_running_operation_is_waited_on(self, redis):
        tasks.set_import_state(JOB, "transfer_import:t", operation="op-1", status="running")
        with mock.patch.object(tasks, "wait_for_operation_name") as wait:
            assert self.run() == "resumed"
        assert wait.call_args.args[:2] == ("instance", "op-1")
        assert not self.prepared and not self.started
        assert tasks.get_import_state(JOB, "transfer_import:t")["status"] == "done"

    def test_a_requested_operation_is_adopted_from_its_database(self, redis):
        tasks.set_import_state(
            JOB, "transfer_import:t", status="requested", uri="gs://b/f.csv", database="prod", table="t"
        )
        with mock.patch.object(tasks, "find_operation", return_value={"name": "op-7"}) as find, \
                mock.patch.object(tasks, "wait_for_operation_name") as wait:
            assert self.run() == "resumed"
        # not the staging import of the same CSV
        assert find.call_args.args == ("instance", "IMPORT", "gs://b/f.csv")
        assert find.call_args.kwargs == {"database": "prod"}
        assert wait.call_args.args[1] == "op-7"
        assert not self.prepared and not self.started

    def test_a_requested_operation_that_never_started_is_run(self, redis):
        tasks.set_import_state(
            JOB, "transfer_import:t", status="requested", uri="gs://b/f.csv", database="prod", table="t"
        )
        with mock.patch.object(tasks, "find_operation", return_value=None):
            assert self.run() == "ran"
        assert self.prepared and self.started

    def test_a_failed_operation_is_run_again(self, redis):
        tasks.set_import_state(JOB, "transfer_import:t", operation="op-1", status="running")
        with mock.patch.object(
            tasks, "wait_for_operation_name", side_effect=CloudSQLAdminError("disk full")
        ):
            assert self.run() == "ran"
        assert self.prepared and self.started
        assert tasks.get_import_state(JOB, "transfer_import:t")["operation"] == "op-new"

    def test_the_store_keeps_each_kind_apart(self, engine, app):
        new_job(engine)
        with use_sink(StoreSink(engine, JOB)):
            tasks.set_import_state(JOB, status="requested", uri="gs://b/f.csv", database="staging", table="t")
            tasks.set_import_state(JOB, operation="op-staging", status="done")
            tasks.set_import_state(JOB, "transfer_import:t", operation="op-1", status="running")
            with mock.patch.object(
                tasks, "wait_for_operation_name", side_effect=CloudSQLAdminError("failed")
            ):
                assert self.run() == "ran"
            assert tasks.get_import_state(JOB)["operation"] == "op-staging"
        row = store.get_import(engine, JOB, "transfer_import:t")
        assert (row["database"], row["operation_name"], row["status"]) == ("prod", "op-new", "done")


@pytest.fixture
def cloudsql(source_and_target, app, redis):
    """export_csv and import_csv done with COPY on the test server."""
    _, (source_db, target_db), (source, target) = source_and_target
    engines = {source_db: source, target_db: target}
    files = {}
    calls = []

    def copy(engine, sql, buffer):
        raw = engine.raw_connection()
        try:
            raw.cursor().copy_expert(sql, buffer)
            raw.commit()
        finally:
            raw.close()

    def export_csv(instance, uri, database, select_query, on_started=None, **kwargs):
        calls.append(("export", uri, database))
        on_started("op-export")
        buffer = io.StringIO()
        copy(engines[database], f"COPY ({select_query}) TO STDOUT WITH (FORMAT csv)", buffer)
        files[uri] = buffer.getvalue()

    def import_csv(instance, uri, database, table, columns=None, on_started=None, **kwargs):
        calls.append(("import", uri, database))
        on_started("op-import")
        cols = ", ".join(f'"{c}"' for c in columns)
        copy(engines[database], f'COPY "{table}" ({cols}) FROM STDIN WITH (FORMAT csv)',
             io.StringIO(files[uri]))

    with mock.patch.object(tasks, "export_csv", side_effect=export_csv), \
            mock.patch.object(tasks, "import_csv", side_effect=import_csv), \
            mock.patch.object(tasks, "update_job_status"), \
            mock.patch.object(tasks, "_rebuild_production_indices") as rebuild, \
            mock.patch.object(tasks, "wait_for_backend_work") as wait_for_backend_work:
        yield {
            "source_db": source_db, "target_db": target_db, "source": source, "target": target,
            "files": files, "calls": calls, "rebuild": rebuild,
            "wait_for_backend_work": wait_for_backend_work,
        }


def transfer(env, csv_uri, export, table=SEG_TABLE):
    return tasks.transfer_table_using_cloudsql(
        table_name=table,
        source_db=env["source_db"],
        target_db=env["target_db"],
        engine=env["target"],
        source_engine=env["source"],
        model_creator=lambda: None,
        csv_uri=csv_uri,
        export=export,
        job_id=JOB,
        table_label="Segmentation Table",
    )


def load_segmentation(env, n=5000):
    """A staging segmentation table written in shuffled order, and its production table
    with the columns in another order (imports must go by column name)."""
    ids = list(range(1, n + 1))
    random.Random(1).shuffle(ids)
    with env["source"].begin() as conn:
        conn.execute(f'CREATE TABLE "{SEG_TABLE}" (id bigint NOT NULL, pt_supervoxel_id bigint, pt_root_id bigint)')
        conn.execute(
            sa.text(f'INSERT INTO "{SEG_TABLE}" SELECT x, x * 10, CASE WHEN x % 7 = 0 THEN NULL ELSE x * 100 END '
                    "FROM unnest(:ids) AS x"),
            {"ids": ids},
        )
    with env["target"].begin() as conn:
        conn.execute(f'CREATE TABLE "{SEG_TABLE}" (pt_root_id bigint, id bigint PRIMARY KEY, pt_supervoxel_id bigint)')
        conn.execute(f'INSERT INTO "{SEG_TABLE}" (id) VALUES (999999)')  # left by a failed transfer
    return ids


def rows(engine):
    with engine.connect() as conn:
        return {tuple(r) for r in conn.execute(f'SELECT id, pt_supervoxel_id, pt_root_id FROM "{SEG_TABLE}"')}


class TestTransferTableUsingCloudSQL:
    def test_exports_staging_and_imports_it_in_id_order(self, cloudsql):
        ids = load_segmentation(cloudsql)
        uri = "gs://bucket/transfer/job/seg.csv"

        assert transfer(cloudsql, uri, export=True) == len(ids)

        assert [c[0] for c in cloudsql["calls"]] == ["export", "import"]
        assert rows(cloudsql["target"]) == rows(cloudsql["source"])  # NULLs and all
        assert _physical_ids(cloudsql["target"], SEG_TABLE) == sorted(ids)
        cloudsql["rebuild"].assert_called_once()
        cloudsql["wait_for_backend_work"].assert_not_called()

    def test_imports_an_existing_csv_without_exporting(self, cloudsql):
        ids = load_segmentation(cloudsql)
        uri = "gs://bucket/processed.csv"
        cloudsql["files"][uri] = "".join(f"{i},{i * 10},{'' if i % 7 == 0 else i * 100}\n" for i in sorted(ids))

        assert transfer(cloudsql, uri, export=False) == len(ids)
        assert [c[0] for c in cloudsql["calls"]] == ["import"]
        assert rows(cloudsql["target"]) == rows(cloudsql["source"])

    def test_a_csv_that_does_not_match_staging_fails_the_count_check(self, cloudsql):
        ids = load_segmentation(cloudsql)
        uri = "gs://bucket/processed.csv"
        cloudsql["files"][uri] = "".join(f"{i},{i * 10},\n" for i in sorted(ids)[:-1])
        with pytest.raises(RuntimeError, match="Row count mismatch"):
            transfer(cloudsql, uri, export=False)
        cloudsql["rebuild"].assert_not_called()

    def test_a_rerun_after_the_import_goes_to_the_indexes(self, cloudsql):
        ids = load_segmentation(cloudsql)
        uri = "gs://bucket/transfer/job/seg.csv"
        transfer(cloudsql, uri, export=True)
        cloudsql["calls"].clear()

        assert transfer(cloudsql, uri, export=True) == len(ids)
        assert cloudsql["calls"] == []  # nothing exported, truncated or imported again
        cloudsql["wait_for_backend_work"].assert_called_once()  # a dead attempt's CREATE INDEX
        assert cloudsql["rebuild"].call_count == 2


class TestTransferToProductionMethod:
    def test_cloudsql_uses_the_processed_csv_when_it_is_there(self, app):
        monitor_result = {
            "datastack_info": {"aligned_volume": {"name": "prod"}, "segmentation_source": "graphene://x/minnie3_v1"},
            "table_name": TABLE,
            "materialization_time_stamp": "2026-10-27T13:33:51",
            "job_id_for_status": JOB,
            "annotation_csv_uri": "gs://bucket/processed.csv",
        }
        db_client = mock.MagicMock()
        db_client.database.get_table_metadata.return_value = {"schema_type": "synapse"}
        db_client.schema.is_segmentation_table_required.return_value = True
        with mock.patch.object(tasks.dynamic_annotation_cache, "get_db", return_value=db_client), \
                mock.patch.object(tasks.db_manager, "get_engine"), \
                mock.patch.object(tasks.db_manager, "session_scope"), \
                mock.patch.object(tasks, "get_db_connection_info"), \
                mock.patch.object(tasks, "create_missing_segmentation_table"), \
                mock.patch.object(tasks, "update_job_status"), \
                mock.patch.object(tasks, "is_upload_cancelled", return_value=False), \
                mock.patch.object(tasks, "_gcs_blob_exists", return_value=True), \
                mock.patch.object(tasks, "transfer_table_using_pg_dump") as pg_dump, \
                mock.patch.object(tasks, "transfer_table_using_cloudsql", return_value=7) as cloudsql:
            result = tasks.transfer_to_production.run(monitor_result, method="cloudsql")

        pg_dump.assert_not_called()
        annotation, segmentation = (c.kwargs for c in cloudsql.call_args_list)
        assert (annotation["table_name"], annotation["csv_uri"], annotation["export"]) == (
            TABLE, "gs://bucket/processed.csv", False,
        )
        assert (segmentation["table_name"], segmentation["export"]) == (SEG_TABLE, True)
        assert segmentation["csv_uri"] == f"gs://bucket/transfer/{JOB}/{SEG_TABLE}.csv"
        assert result["exported_csvs"] == [segmentation["csv_uri"]]


class TestClusterSkip:
    def test_correlation_tells_an_ordered_table_from_a_shuffled_one(self, source_and_target):
        _, _, (source, _) = source_and_target
        with source.begin() as conn:
            conn.execute('CREATE TABLE "ordered" (id bigint PRIMARY KEY, v text)')
            conn.execute('INSERT INTO "ordered" SELECT x, md5(x::text) FROM generate_series(1, 20000) x')
            conn.execute('CREATE TABLE "shuffled" (id bigint, v text)')
            conn.execute('INSERT INTO "shuffled" SELECT x, md5(x::text) FROM generate_series(1, 20000) x ORDER BY random()')
        assert tasks.id_order_correlation("ordered", source) > 0.999
        assert abs(tasks.id_order_correlation("shuffled", source)) < 0.1
        assert tasks.id_order_correlation("missing", source) is None

    def test_a_table_in_id_order_is_not_clustered(self, app):
        monitor_result = {
            "datastack_info": {"segmentation_source": "graphene://x/minnie3_v1"},
            "table_name": TABLE,
            "job_id_for_status": JOB,
        }
        with mock.patch.object(tasks, "get_config_param", return_value="staging"), \
                mock.patch.object(tasks.db_manager, "get_engine"), \
                mock.patch.object(tasks, "update_job_status"), \
                mock.patch.object(tasks, "is_upload_cancelled", return_value=False), \
                mock.patch.object(tasks, "id_order_correlation", side_effect=[0.9999, 0.02]), \
                mock.patch.object(tasks, "cluster_table_by_id", return_value=True) as cluster:
            tasks.cluster_staging_tables.run(monitor_result)
        assert [c.args[0] for c in cluster.call_args_list] == [SEG_TABLE]


class TestDriverTransfer:
    def test_the_transfer_stage_uses_cloudsql_with_the_processed_csv(self):
        ctx = mock.MagicMock()
        ctx.results = {"process_source": {"output_path": "bucket/processed.csv"}}
        with mock.patch.object(bulk_upload, "_monitor_result", return_value={"table_name": TABLE}), \
                mock.patch.object(tasks.transfer_to_production, "run", return_value={}) as run:
            bulk_upload.stage_transfer(ctx)
        monitor_result = run.call_args.args[0]
        assert monitor_result["annotation_csv_uri"] == "gs://bucket/processed.csv"
        assert run.call_args.kwargs["method"] == "cloudsql"

    def test_cleanup_deletes_the_exported_csvs(self):
        ctx = mock.MagicMock()
        ctx.table_name = TABLE
        ctx.results = {
            "process_source": {"output_path": "bucket/processed.csv"},
            "transfer": {"exported_csvs": ["gs://bucket/transfer/job/seg.csv"]},
        }
        with mock.patch.object(tasks, "_discard_staging", return_value={"status": "success"}), \
                mock.patch.object(tasks, "update_job_status"), \
                mock.patch("materializationengine.blueprints.upload.gcs_processor.GCSCsvProcessor") as gcs:
            bulk_upload.stage_cleanup(ctx)
        deleted = [c.args[0] for c in gcs.return_value.delete_blobs.call_args_list]
        assert deleted == [["processed.csv"], ["transfer/job/seg.csv"]]
