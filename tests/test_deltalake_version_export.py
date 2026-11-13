"""Exporting to Delta Lake: config, spec overrides, table listing, the version's status
record, and the export endpoints (Ray replaced by fakes). The export itself is covered by
tests/test_lake_export_*.py."""

import json
import os
import uuid
from contextlib import contextmanager
from unittest import mock

import pytest
import redis
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import sessionmaker

from materializationengine.workflows import deltalake_export as dx
from materializationengine.workflows import deltalake_version_export as dve

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))


class TestConfig:
    def test_defaults_and_table_entries_merge(self):
        config = dve.load_export_config(json.dumps({
            "defaults": {"max_row_group_size": 10000},
            "tables": {"synapses": {"n_partitions": 512, "max_row_group_size": 20000},
                       "custom": {"output_specs": [{"name": "id"}]},
                       "scratch": {"skip": True}}}))
        assert dve.resolve_table_config(config, "synapses") == (False, None, {"max_row_group_size": 20000, "n_partitions": 512})
        assert dve.resolve_table_config(config, "custom") == (False, [{"name": "id"}], {"max_row_group_size": 10000})
        assert dve.resolve_table_config(config, "scratch")[0] is True
        assert dve.resolve_table_config(config, "other") == (False, None, {"max_row_group_size": 10000})
        assert dve.resolve_table_config({}, "other") == (False, None, {})

    @pytest.mark.parametrize("raw,match", [
        ('{"default": {}}', "unknown keys"),
        ('{"defaults": {"row_groups": 5}}', "unknown output spec fields"),
        ('{"tables": {"t": {"partitions": 5}}}', "unknown output spec fields"),
        ('{"tables": {"t": 5}}', "must be an object"),
        ('[1]', "JSON object"),
    ])
    def test_rejects_mistakes(self, raw, match):
        with pytest.raises(ValueError, match=match):
            dve.load_export_config(raw)

    def test_empty(self):
        assert dve.load_export_config(None) == {} and dve.load_export_config("") == {}


class TestSpecOverrides:
    def test_fields_and_name_suffix(self):
        specs = [dx.DeltaLakeOutputSpec(name="id"), dx.DeltaLakeOutputSpec(name="post_pt_root_id", n_partitions=8)]
        dx.apply_spec_overrides(specs, {"max_row_group_size": 10000, "name_suffix": "__s4", "n_partitions": 512})
        assert [s.name for s in specs] == ["id__s4", "post_pt_root_id__s4"]
        assert all(s.max_row_group_size == 10000 and s.n_partitions == 512 for s in specs)

    def test_unknown_and_invalid(self):
        with pytest.raises(ValueError, match="unknown output spec override"):
            dx.apply_spec_overrides([dx.DeltaLakeOutputSpec(name="id")], {"row_groups": 1})
        with pytest.raises(ValueError, match="positive integer"):
            dx.apply_spec_overrides([dx.DeltaLakeOutputSpec(name="id")], {"max_row_group_size": 0})


@pytest.fixture
def frozen_db(mat_metadata):
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    name = f"dve_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    with engine.begin() as conn:
        conn.execute("CREATE TABLE materializedmetadata (id serial PRIMARY KEY, schema varchar, table_name varchar, "
                     "row_count bigint, materialized_timestamp timestamp)")
        conn.execute("INSERT INTO materializedmetadata (schema, table_name, row_count, materialized_timestamp) VALUES "
                     "('synapse', 'small', 10, now()), ('synapse', 'big', 1000, now()), ('cell_type', 'mid', 100, now())")
        conn.execute("CREATE TABLE big (id bigint)")
        conn.execute("CREATE VIEW connections AS SELECT id FROM big")
    Session = sessionmaker(bind=engine)

    @contextmanager
    def scope(database):
        session = Session()
        try:
            yield session
        finally:
            session.close()

    from materializationengine.database import db_manager
    with mock.patch.object(db_manager, "session_scope", side_effect=scope):
        yield name
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


class TestListTables:
    def test_tables_biggest_first(self, frozen_db):
        assert [(t["table"], t["rows"]) for t in dve.list_version_tables("ds", 1)] == [("big", 1000), ("mid", 100), ("small", 10)]

    def test_views_after_tables(self, frozen_db):
        tables = dve.list_version_tables("ds", 1, include_views=True)
        assert [t["table"] for t in tables] == ["big", "mid", "small", "connections"]
        assert tables[-1] == {"table": "connections", "rows": None, "kind": "view"}


@pytest.fixture
def test_redis():
    client = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=12)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    client.flushdb()
    with mock.patch.object(dx, "_get_redis_client", return_value=client):
        yield client
    client.flushdb()


class TestVersionStatus:
    def test_running_tables_carry_their_progress(self, test_redis):
        dve.set_version_status("ds", 7, "j1", state="running", tables={
            "big": {"state": "running"}, "small": {"state": "done"}})
        dx.set_deltalake_export_status("ds", 7, "big", status="exporting", phase="streaming", total_rows=100,
                                       rows_processed=40, job_id=dve.table_job_id("j1", "big"))
        status = dve.get_version_status("ds", 7, "j1")
        assert status["state"] == "running"
        assert status["tables"]["big"]["progress"] == {"phase": "streaming", "rows_processed": 40, "total_rows": 100,
                                                      "percent_complete": 40.0}
        assert "progress" not in status["tables"]["small"]
        assert dve.get_version_status("ds", 7, "nope") is None


class TestEndpoint:
    @pytest.fixture
    def client(self, monkeypatch):
        from flask import Flask
        from flask_restx import Api

        import middle_auth_client.decorators as auth
        from materializationengine.blueprints.materialize import api

        monkeypatch.setattr(auth, "AUTH_DISABLED", True)
        app = Flask(__name__)
        Api(app).add_namespace(api.mat_bp, path="/api/v2")
        with app.test_client() as client:
            yield client

    URL = "/api/v2/materialize/run/write_deltalake_version/datastack/minnie65_phase3_v1/version/2253/"

    def test_submits_one_rayjob_for_the_version(self, client):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob", return_value="mat-deltalake-v2253-x") as submit, \
                mock.patch.object(dve, "set_version_status") as record:
            r = client.post(self.URL + "?workers=8", json={
                "tables": ["synapses_pni_2", "nucleus_detection_v0"], "max_streams": 3, "name_suffix": "__s5",
                "views": True, "config": {"defaults": {"max_row_group_size": 10000}}})
        assert r.status_code == 200, r.get_data()
        entry = submit.call_args.kwargs["entrypoint"]
        assert "entrypoints.lake_export --datastack minnie65_phase3_v1 --version 2253" in entry
        assert "--tables synapses_pni_2 nucleus_detection_v0" in entry and "--version-job" in entry
        assert "--views" in entry and "--max-streams 3" in entry
        assert "--name-suffix __s5" in entry and "max_row_group_size" in entry
        assert submit.call_args.kwargs["num_workers"] == 8
        assert submit.call_args.kwargs["worker_cpus"] == 2 and submit.call_args.kwargs["worker_memory"] == "11Gi"
        assert record.call_args.kwargs["state"] == "submitted" and r.json["rayjob_name"] == "mat-deltalake-v2253-x"

    def test_workers_follow_the_streams(self, client):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob", return_value="x") as submit, \
                mock.patch.object(dve, "set_version_status"):
            client.post(self.URL, json={"max_streams": 16})
        entry = submit.call_args.kwargs["entrypoint"]
        assert "--views" not in entry and "--tables" not in entry
        assert submit.call_args.kwargs["num_workers"] == 8  # 16 streams on 2-CPU workers

    def test_a_job_with_views_may_grow_to_the_stages_workers(self, client):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob", return_value="x") as submit, \
                mock.patch.object(dve, "set_version_status"):
            client.post(self.URL, json={"views": True})
            assert submit.call_args.kwargs["num_workers"] == 8  # views_workers, over 4 streams' 2
            client.post(self.URL, json={})
            assert submit.call_args.kwargs["num_workers"] == 2

    @pytest.mark.parametrize("body", [
        {"tables": "synapses"}, {"max_streams": 0}, {"name_suffix": "a b; rm"},
        {"config": {"defaults": {"bogus": 1}}}, {"include_views": True}, {"views": "yes"},
    ])
    def test_bad_requests(self, client, body):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob") as submit:
            assert client.post(self.URL, json=body).status_code == 400
        submit.assert_not_called()

    def test_needs_ray(self, client):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=False):
            assert client.post(self.URL, json={}).status_code == 501

    def test_status(self, client):
        with mock.patch.object(dve, "get_version_status", return_value={"state": "running"}):
            assert client.get(self.URL + "?job_id=j1").json == {"state": "running"}
        with mock.patch.object(dve, "get_version_status", return_value=None):
            assert client.get(self.URL + "?job_id=j1").status_code == 404
        assert client.get(self.URL).status_code == 400


class TestTableEndpoint:
    client = TestEndpoint.client
    URL = ("/api/v2/materialize/run/write_deltalake/datastack/minnie65_phase3_v1/version/2253/"
           "table_name/synapses_pni_2/")

    def test_submits_the_table(self, client):
        specs = [{"name": "id", "partition_by": "id", "partition_strategy": "percentile_range"}]
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob", return_value="mat-deltalake-x") as submit:
            r = client.post(self.URL, json={"output_specs": specs})
        assert r.status_code == 200, r.get_data()
        assert r.json["backend"] == "ray" and r.json["rayjob_name"] == "mat-deltalake-x" and r.json["job_id"]
        entry = submit.call_args.kwargs["entrypoint"]
        assert "entrypoints.lake_export" in entry and "--tables synapses_pni_2" in entry
        assert "--version-job" not in entry and "percentile_range" in entry
        assert f"--job-id {r.json['job_id']}" in entry
        assert submit.call_args.kwargs["name_prefix"] == "mat-deltalake-synapses-pni-2"

    @pytest.mark.parametrize("query,body,status", [
        ("?backend=celery", {}, 400),
        ("", {"output_specs": {"name": "id"}}, 400),
        ("", {"output_specs": [{"name": ""}]}, 400),
    ])
    def test_bad_requests(self, client, query, body, status):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob") as submit:
            assert client.post(self.URL + query, json=body).status_code == status
        submit.assert_not_called()

    def test_needs_ray(self, client):
        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=False):
            assert client.post(self.URL, json={}).status_code == 501
