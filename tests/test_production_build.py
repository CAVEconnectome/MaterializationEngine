"""Uploads built in the production database under temporary names: publishing them by
rename, cleaning up only what the job built, and giving way to frozen materialization.

The publish tests build real synapse tables (PostGIS) in a scratch aligned volume
database made with dynamicannotationdb, so the metadata tables are the real ones.
"""

import datetime
import threading
import time
import uuid
from unittest import mock

import pytest
import numpy as np
import sqlalchemy as sa
from dynamicannotationdb import DynamicAnnotationInterface
from flask import Flask
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import NullPool

from materializationengine.blueprints.upload import production_build as pb
from materializationengine.blueprints.upload import tasks
from materializationengine.rayjobs import runner as runner_module
from materializationengine.rayjobs.entrypoints import bulk_upload
from materializationengine.upload_state import clone_gate, store
from tests.test_ray_bulk_upload import FakeStages, discarded, release  # noqa: F401
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401

PCG = "test_pcg"
TABLE = "synapses_pub"
META = {
    "schema_type": "synapse",
    "description": "test",
    "user_id": "1174",
    "voxel_resolution_x": 4.0,
    "voxel_resolution_y": 4.0,
    "voxel_resolution_z": 40.0,
    "reference_table": None,
    "flat_segmentation_source": None,
    "read_permission": "PUBLIC",
    "write_permission": "PRIVATE",
    "notice_text": None,
}
CREATED = datetime.datetime(2026, 10, 27, 13, 33, 51)
LAST_UPDATED = datetime.datetime(2026, 10, 27, 13, 33, 52)


@pytest.fixture
def production(mat_metadata):
    """A scratch aligned volume database with dynamicannotationdb's metadata tables."""
    server = make_url(mat_metadata["sql_uri"])
    name = f"prod_build_test_{uuid.uuid4().hex[:8]}"
    base = f"{server.drivername}://{server.username}:{server.password}@{server.host}:{server.port or 5432}"
    interface = DynamicAnnotationInterface(f"{base}/{name}", name)
    engine = sa.create_engine(f"{base}/{name}", poolclass=NullPool)
    with engine.begin() as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    # dynamicannotationdb's models share one MetaData, and an interface for a new
    # database creates every model in it: a name per test keeps earlier tests' tables
    # out of this one's database
    yield engine, interface, f"syn_{uuid.uuid4().hex[:8]}"
    interface.database.engine.dispose()
    engine.dispose()
    admin_engine = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT", poolclass=NullPool)
    with admin_engine.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")
    admin_engine.dispose()


def build(engine, table, job_id=JOB, rows=200):
    """Build an upload's temporary tables as the stages do, with rows in them."""
    names = pb.build_names(job_id, table, PCG)
    anno = pb.annotation_model(names["annotation"], META)
    pb.create_table(engine, anno, job_id, keep_primary_key=False)
    with engine.begin() as conn:
        conn.execute(sa.text(
            f'INSERT INTO "{names["annotation"]}" (id, created, valid, size, pre_pt_position, post_pt_position, ctr_pt_position)'
            " SELECT x, now(), true, x, ST_MakePoint(x, x, x), ST_MakePoint(x, x, x + 1), ST_MakePoint(x, x, x + 2)"
            " FROM generate_series(1, :n) x"
        ), {"n": rows})
    for command in pb.index_commands(engine, names["annotation"], anno):
        with engine.begin() as conn:
            conn.execute(sa.text(command))
    seg = pb.segmentation_model(names["annotation"], META, PCG)
    pb.create_table(engine, seg, job_id, keep_primary_key=True)
    with engine.begin() as conn:
        conn.execute(sa.text(f'INSERT INTO "{names["segmentation"]}" (id) SELECT generate_series(1, :n)'), {"n": rows})
    for command in pb.index_commands(engine, names["segmentation"], seg):
        with engine.begin() as conn:
            conn.execute(sa.text(command))
    return names


def publish(engine, names, job_id=JOB):
    return pb.publish(engine, job_id, names, META, PCG, created=CREATED, last_updated=LAST_UPDATED)


def tables(engine):
    with engine.connect() as conn:
        return {r[0] for r in conn.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")}


class TestNames:
    def test_temporary_names_are_per_job_and_derive_like_normal_ones(self):
        names = pb.build_names(JOB, TABLE, PCG)
        assert names["annotation"] == f"{pb.build_prefix(JOB)}{TABLE}"
        assert names["segmentation"] == f"{names['annotation']}__{PCG}"
        assert names["final_segmentation"] == f"{TABLE}__{PCG}"
        assert "__" not in pb.build_prefix(JOB)
        assert pb.build_prefix(JOB) != pb.build_prefix(JOB + "x")

    def test_names_postgres_would_truncate_are_found(self):
        assert pb.names_too_long(pb.build_names(JOB, TABLE, PCG), META, PCG) == []
        long_names = pb.build_names(JOB, "s" * 40, PCG)
        assert pb.names_too_long(long_names, META, PCG)


class TestPublish:
    def test_publish_renames_everything_and_registers_the_tables(self, production):
        engine, interface, TABLE = production
        names = build(engine, TABLE)
        assert TABLE not in interface.database.get_valid_table_names()

        assert publish(engine, names) == "published"

        assert {TABLE, names["final_segmentation"]} <= tables(engine)
        assert not {names["annotation"], names["segmentation"]} & tables(engine)
        # every index, constraint and the sequence has the name a normal table's has,
        # so index_manager finds nothing missing
        assert pb.index_commands(engine, TABLE, pb.annotation_model(TABLE, META)) == []
        assert pb.index_commands(
            engine, names["final_segmentation"], pb.segmentation_model(TABLE, META, PCG)
        ) == []
        with engine.connect() as conn:
            assert conn.execute(sa.text("SELECT pg_get_serial_sequence(:t, 'id')"), {"t": TABLE}).scalar() == f"public.{TABLE}_id_seq"
            assert conn.execute(sa.text(f"SELECT nextval('{TABLE}_id_seq')")).scalar() == 201
            leftovers = conn.execute(sa.text(
                "SELECT relname FROM pg_class WHERE relname LIKE :p"), {"p": f"{pb.build_prefix(JOB)}%"}
            ).fetchall()
        assert leftovers == []
        assert TABLE in interface.database.get_valid_table_names()
        with engine.connect() as conn:
            row = conn.execute(sa.text(
                "SELECT schema_type, voxel_resolution_z, created, valid FROM annotation_table_metadata"
                " WHERE table_name = :t"), {"t": TABLE}).first()
        assert tuple(row) == ("synapse", 40.0, CREATED, True)
        seg = interface.segmentation.get_segmentation_table_metadata(TABLE, PCG)
        assert seg["last_updated"] == LAST_UPDATED

    def test_publishing_again_is_a_no_op(self, production):
        engine, _, TABLE = production
        names = build(engine, TABLE)
        publish(engine, names)
        assert publish(engine, names) == "already published"

    def test_a_taken_final_name_changes_nothing(self, production):
        engine, _, TABLE = production
        names = build(engine, TABLE)
        with engine.begin() as conn:
            conn.execute(f'CREATE TABLE "{TABLE}" (id bigint)')
        with pytest.raises(pb.PublishConflict):
            publish(engine, names)
        assert {names["annotation"], names["segmentation"]} <= tables(engine)
        assert bulk_upload._keeps_built_tables(pb.PublishConflict("x"))

    def test_names_in_use_ignores_the_jobs_own_tables(self, production):
        engine, _, TABLE = production
        names = build(engine, TABLE)
        assert pb.names_in_use(engine, names, job_id=JOB) == []
        assert pb.names_in_use(engine, names) == sorted([names["annotation"], names["segmentation"]])
        publish(engine, names)
        assert TABLE in pb.names_in_use(engine, pb.build_names(JOB + "2", TABLE, PCG))


class TestDropBuildTables:
    def test_drops_only_this_jobs_unpublished_tables(self, production):
        engine, _, TABLE = production
        mine = build(engine, TABLE)
        other = build(engine, TABLE, job_id=JOB + "_other")
        assert pb.drop_build_tables(engine, JOB, mine) == [mine["segmentation"], mine["annotation"]]
        assert {other["annotation"], other["segmentation"]} <= tables(engine)
        # names it was given but did not build, or that were published, are left alone
        assert pb.drop_build_tables(engine, JOB, other) == []
        publish(engine, other, job_id=JOB + "_other")
        assert pb.drop_build_tables(engine, JOB + "_other", other) == []
        assert TABLE in tables(engine)


class TestCloneGate:
    def test_waits_for_a_holder_then_releases(self, engine, app):
        new_job(engine)
        store.update_job(engine, JOB, {"status": "processing"})
        store.set_job_fields(engine, JOB, driver_heartbeat_at=datetime.datetime.now(datetime.timezone.utc))
        store.set_holding(engine, JOB, "prod", True)
        assert store.jobs_holding_production(engine, "prod") == [JOB]

        def driver_lets_go():
            while not store.clone_requested(engine, "prod"):
                time.sleep(0.05)
            store.set_holding(engine, JOB, "prod", False)

        thread = threading.Thread(target=driver_lets_go)
        thread.start()
        with mock.patch.object(clone_gate, "_enabled", return_value=True), \
                mock.patch("materializationengine.upload_state.db.get_engine", return_value=engine):
            with clone_gate.clone_gate("prod", wait_seconds=10, poll_seconds=0.05):
                assert store.jobs_holding_production(engine, "prod") == []
                assert store.clone_requested(engine, "prod")
        thread.join()
        assert not store.clone_requested(engine, "prod")

    def test_a_dead_driver_holds_nothing(self, engine, app):
        new_job(engine)
        store.update_job(engine, JOB, {"status": "processing"})
        store.set_holding(engine, JOB, "prod", True)  # but it never heartbeated
        assert store.jobs_holding_production(engine, "prod") == []

    def test_gives_up_waiting_and_copies_anyway(self, engine, app):
        new_job(engine)
        store.update_job(engine, JOB, {"status": "processing"})
        store.set_job_fields(engine, JOB, driver_heartbeat_at=datetime.datetime.now(datetime.timezone.utc))
        store.set_holding(engine, JOB, "prod", True)
        with mock.patch.object(clone_gate, "_enabled", return_value=True), \
                mock.patch("materializationengine.upload_state.db.get_engine", return_value=engine):
            with clone_gate.clone_gate("prod", wait_seconds=0.2, poll_seconds=0.05):
                pass
        assert not store.clone_requested(engine, "prod")

    def test_without_the_state_database_it_just_copies(self):
        with mock.patch.object(clone_gate, "_enabled", return_value=False):
            with clone_gate.clone_gate("prod"):
                pass


@pytest.fixture
def app():
    app = Flask(__name__)
    with app.app_context():
        yield app


class TestDriverYields:
    def test_the_gate_releases_the_database_until_the_copy_is_done(self, engine, app):
        new_job(engine)
        ctx = bulk_upload.Context(engine=engine, job_id=JOB, epoch=1, params={}, runner=None)
        gate = bulk_upload.ProductionGate(ctx, "prod", poll_seconds=0.05)
        gate.start()
        request = store.request_clone(engine, "prod")
        seen = []

        def materialization():
            while True:
                with engine.connect() as conn:
                    holding = conn.execute(sa.text("SELECT holding FROM production_holds")).scalar()
                if not holding:
                    seen.append("released")
                    store.release_clone(engine, request)
                    return
                time.sleep(0.05)

        thread = threading.Thread(target=materialization)
        thread.start()
        with mock.patch("materializationengine.database.db_manager.cleanup") as cleanup:
            gate.wait_turn()
        thread.join()
        assert seen == ["released"]
        cleanup.assert_called()  # its connections were closed
        with engine.connect() as conn:
            assert conn.execute(sa.text("SELECT holding FROM production_holds")).scalar() is True

    def test_chunk_dispatch_drains_before_pausing(self):
        runner = runner_module.InlineRunner()
        events = []
        pause = {"on": False}

        def on_done(key, result):
            events.append(("done", key))
            if key == 1:
                pause["on"] = True

        def on_paused():
            events.append(("paused",))
            pause["on"] = False

        with mock.patch.dict("materializationengine.rayjobs.upload_tasks.TASKS", {"echo": lambda x: x}):
            runner_module.run_bounded(
                runner, "echo", ((i, (i,)) for i in range(4)), 1, on_done,
                should_pause=lambda: pause["on"], on_paused=on_paused, poll_seconds=0.01,
            )
        assert events == [("done", 0), ("done", 1), ("paused",), ("done", 2), ("done", 3)]


PRODUCTION_PARAMS = {
    "file_path": "gs://b/f.csv",
    "file_metadata": {"metadata": {"table_name": TABLE, "schema_type": "synapse"}, "column_mapping": {}},
    "datastack_info": {"datastack": "minnie65_phase3_v1", "aligned_volume": {"name": "prod"},
                       "segmentation_source": f"graphene://x/{PCG}"},
    "sql_instance_name": "instance",
    "created_ts": "2026-10-27T13:33:51+00:00",
    "materialization_time_stamp": "2026-10-27T13:33:51",
    "build_in_production": True,
    "build_tables": pb.build_names(JOB, TABLE, PCG),
    "table_meta": META,
}


@pytest.fixture
def production_job(engine, app):
    new_job(engine)
    store.set_job_fields(engine, JOB, params=PRODUCTION_PARAMS)
    return engine


@pytest.fixture
def production_stages():
    fake = FakeStages()
    with mock.patch.dict(
        bulk_upload.PRODUCTION_STAGES, {n: fake(n) for n in bulk_upload.PRODUCTION_STAGE_ORDER}
    ):
        yield fake


@pytest.fixture
def dropped():
    calls = []
    with mock.patch.object(pb, "drop_build_tables", side_effect=lambda e, j, n: calls.append(j) or []), \
            mock.patch("materializationengine.database.db_manager.get_engine"):
        yield calls


def run(engine):
    return bulk_upload.run_job(engine, JOB, runner_module.InlineRunner, host="head", lease_wait_seconds=0)


class TestProductionDriver:
    def test_runs_the_production_stages_holding_the_database(self, production_job, production_stages, dropped):
        assert run(production_job) == 0
        assert production_stages.runs == bulk_upload.PRODUCTION_STAGE_ORDER
        with production_job.connect() as conn:
            assert conn.execute(sa.text("SELECT aligned_volume, holding FROM production_holds")).first() == ("prod", False)
        assert dropped == []

    def test_a_failure_drops_only_the_built_tables(self, production_job, production_stages, dropped, discarded):
        production_stages.fail["spatial_chunks"] = ValueError("bad")
        assert run(production_job) == 1
        assert dropped == [JOB] and discarded == []  # never the staging discard

    def test_a_publish_conflict_keeps_the_built_tables(self, production_job, production_stages, dropped):
        production_stages.fail["publish"] = pb.PublishConflict("taken")
        assert run(production_job) == 1
        record = store.get_job_record(production_job, JOB)
        assert record["status"] == "error" and record["staging_cleanup"].startswith("kept")
        assert dropped == []

    def test_a_cancel_is_refused_once_publishing(self, production_job, production_stages, dropped):
        def publish_stage(ctx):
            production_stages.runs.append("publish")
            store.request_cancel(production_job, JOB, "1174")
            return {}

        with mock.patch.dict(bulk_upload.PRODUCTION_STAGES, {"publish": publish_stage}):
            assert run(production_job) == 0
        assert "cleanup" in production_stages.runs


class TestProductionStages:
    """The stage bodies, against a scratch aligned volume database: the Cloud SQL import
    and the spatial chunks are stood in for by inserts."""

    def test_load_lookup_finish_and_publish(self, production, engine, app):
        from materializationengine.upload_state.status_sink import StoreSink, use_sink

        prod, interface, table = production
        app.config.update(
            MERGE_TABLES=True, THROTTLE_QUEUES=False, QUEUE_LENGTH_LIMIT=10000,
            MATERIALIZATION_ROW_CHUNK_SIZE=500, SPATIAL_LOOKUP_TARGET_ROWS_PER_CHUNK=1000,
        )
        names = pb.build_names(JOB, table, PCG)
        params = {
            **PRODUCTION_PARAMS,
            "file_metadata": {"metadata": {"table_name": table, "schema_type": "synapse"}, "column_mapping": {}},
            "build_tables": names,
        }
        new_job(engine)
        store.set_job_fields(engine, JOB, params=params)
        ctx = bulk_upload.Context(engine=engine, job_id=JOB, epoch=1, params=params, runner=None)
        ctx.results["process_source"] = {"output_path": "bucket/processed.csv"}

        def fake_import(instance, uri, database, table, columns=None, on_started=None, **kwargs):
            on_started("op-1")
            assert columns[0] == "id" and "pre_pt_position" in columns
            with prod.begin() as conn:
                conn.execute(sa.text(
                    f'INSERT INTO "{table}" (id, created, valid, size, pre_pt_position, post_pt_position, ctr_pt_position)'
                    " SELECT x, now(), true, x, ST_MakePoint(x * 10, x * 10, x), ST_MakePoint(x * 10, x * 10, x + 1),"
                    " ST_MakePoint(x * 10, x * 10, x + 2) FROM generate_series(1, 500) x"
                ))

        with use_sink(StoreSink(engine, JOB, epoch=1)), \
                mock.patch("materializationengine.database.db_manager.get_engine", return_value=prod), \
                mock.patch("materializationengine.database.dynamic_annotation_cache.get_db", return_value=interface), \
                mock.patch.object(tasks, "import_csv", side_effect=fake_import), \
                mock.patch.object(tasks, "get_config_param", return_value=None):
            loaded = bulk_upload.stage_load_annotations(ctx)
            assert loaded["indexes"] >= 5  # pkey, created, deleted and the three GIST indexes
            assert TABLE not in interface.database.get_valid_table_names()

            ctx.results["load_annotations"] = loaded
            bulk_upload.stage_spatial_init(ctx)
            spatial = store.get_spatial(engine, JOB)
            info = spatial["mat_metadata"]
            assert (spatial["database_name"], info["annotation_table_name"]) == ("prod", names["annotation"])
            assert info["segmentation_table_name"] == names["segmentation"]
            # the bounds and a chunk's points come from the table, not a metadata row
            assert spatial["total_chunks"] > 0
            lo, hi = spatial["min_enclosing_bbox"]
            assert lo != hi
            from materializationengine.workflows.spatial_lookup import get_pts_from_bbox

            pts = get_pts_from_bbox("prod", np.array(lo), np.array(hi) + 1, dict(info))
            assert pts is not None and len(pts) >= 500
            # the chunks, written out of id order
            with prod.begin() as conn:
                conn.execute(sa.text(
                    f'INSERT INTO "{names["segmentation"]}" (id) SELECT x FROM generate_series(1, 500) x ORDER BY random()'
                ))

            finished = bulk_upload.stage_finish_tables(ctx)
            assert finished["clustered"] is True
            published = bulk_upload.stage_publish(ctx)

        assert published["publish"] == "published"
        assert table in interface.database.get_valid_table_names()
        assert pb.index_commands(prod, table, pb.annotation_model(table, META)) == []
        assert pb.index_commands(
            prod, names["final_segmentation"], pb.segmentation_model(table, META, PCG)
        ) == []
        with prod.connect() as conn:
            ids = [r[0] for r in conn.execute(f'SELECT id FROM "{names["final_segmentation"]}" ORDER BY ctid')]
        assert ids == list(range(1, 501))
        assert store.get_import(engine, JOB)["database"] == "prod"
