"""Admin page: listings it adds (databases, repack jobs, queues) and the superadmin-only nav link.

Needs redis on localhost:6390 (`docker run -d --rm -p 6390:6379 redis:7`) for the redis
tests, or they are skipped.
"""

import inspect
import json
import os
import time
import uuid
from unittest import mock

import pytest
import redis
import sqlalchemy as sa
from flask import Flask, g, render_template_string
from sqlalchemy.engine.url import make_url

from materializationengine.blueprints.admin import api as admin_api
from materializationengine.workflows import table_maintenance as tm

REDIS_PORT = int(os.environ.get("TEST_REDIS_PORT", "6390"))


@pytest.fixture
def scratch_dbs(mat_metadata):
    """A live database with analysisversion rows and two frozen copies (one unknown)."""
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    tag = uuid.uuid4().hex[:6]
    live, frozen, stray = f"adm{tag}", f"adm{tag}_ds__mat7", f"adm{tag}_ds__mat8"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        for name in (live, frozen, stray):
            conn.execute(f"CREATE DATABASE {name}")
    setup = sa.create_engine(f"{base}/{live}")
    with setup.begin() as conn:
        conn.execute("CREATE TABLE analysisversion (datastack varchar, version int, valid bool,"
                     " expires_on timestamp, status varchar, time_stamp timestamp)")
        conn.execute(f"INSERT INTO analysisversion VALUES ('adm{tag}_ds', 7, true, '2026-11-06', 'AVAILABLE', '2026-10-07')")
    setup.dispose()
    engines = {}

    def get_engine(name):
        if name not in engines:
            engines[name] = sa.create_engine(f"{base}/{name}")
        return engines[name]

    with mock.patch.object(tm.db_manager, "get_engine", side_effect=get_engine):
        yield live, frozen, stray
    for e in engines.values():
        e.dispose()
    with admin.connect() as conn:
        for name in (live, frozen, stray):
            conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


@pytest.fixture
def real_redis():
    client = redis.StrictRedis(host="localhost", port=REDIS_PORT, db=12)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip(f"no redis on localhost:{REDIS_PORT}")
    client.flushdb()
    with mock.patch.object(tm, "REDIS_CLIENT", client), mock.patch.object(admin_api, "get_redis_client", return_value=client):
        yield client
    client.flushdb()


class TestListDatabases:
    def test_live_and_frozen_with_version_details(self, scratch_dbs):
        live, frozen, stray = scratch_dbs
        dbs = {d["name"]: d for d in tm.list_databases()}
        assert dbs[live]["kind"] == "live" and dbs[live]["size_gb"] is not None
        assert dbs[frozen]["kind"] == "frozen" and dbs[frozen]["version"] == 7
        assert dbs[frozen]["valid"] is True and dbs[frozen]["expires_on"].startswith("2026-11-06")
        assert dbs[frozen]["live_database"] == live
        # a frozen database with no analysisversion row still lists, with what its name says
        assert dbs[stray]["kind"] == "frozen" and dbs[stray]["version"] == 8 and "valid" not in dbs[stray]
        assert "postgres" not in dbs and "template1" not in dbs

    def test_sizes_can_be_skipped(self, scratch_dbs):
        live, _, _ = scratch_dbs
        assert {d["name"]: d for d in tm.list_databases(with_sizes=False)}[live]["size_gb"] is None

    def test_frozen_versions_of_one_datastack_newest_first(self, scratch_dbs):
        _, frozen, stray = scratch_dbs
        versions = tm.list_frozen_versions(frozen.rsplit("__mat", 1)[0])
        assert [(v["name"], v["version"]) for v in versions] == [(stray, 8), (frozen, 7)]
        assert versions[1]["valid"] is True and "size_gb" not in versions[1]
        assert tm.list_frozen_versions("no_such_datastack") == []

    def test_relations_are_tables_and_views_without_metadata_or_extension_objects(self, scratch_dbs):
        _, frozen, _ = scratch_dbs
        with tm.db_manager.get_engine(frozen).begin() as conn:
            conn.execute("CREATE EXTENSION postgis")  # spatial_ref_sys, geometry_columns, ...
            conn.execute("CREATE TABLE materializedmetadata (id serial PRIMARY KEY, table_name varchar)")
            conn.execute("CREATE TABLE synapses (id bigint PRIMARY KEY)")
            conn.execute("INSERT INTO synapses SELECT generate_series(1, 1000)")
            conn.execute("CREATE VIEW synapses_view AS SELECT id FROM synapses")
            conn.execute("CREATE MATERIALIZED VIEW synapses_mv AS SELECT id FROM synapses")
            conn.execute("ANALYZE synapses")
        relations = {r["name"]: r for r in tm.list_relations(frozen)}
        assert set(relations) == {"synapses", "synapses_view", "synapses_mv"}
        assert relations["synapses"]["kind"] == "table" and relations["synapses"]["rows"] == 1000
        assert relations["synapses_view"]["kind"] == "view" and relations["synapses_view"]["rows"] is None
        assert relations["synapses_mv"]["kind"] == "materialized view"


class TestListings:
    def test_repack_jobs_newest_first(self, real_redis):
        for i, state in enumerate(("done", "running", "refused")):
            real_redis.set(f"{tm.STATUS_KEY_PREFIX}job{i}", json.dumps({"job_id": f"job{i}", "state": state, "updated_at": f"2026-10-07T0{i}:00:00"}))
        assert [j["job_id"] for j in tm.list_repack_jobs()] == ["job2", "job1", "job0"]
        assert len(tm.list_repack_jobs(limit=2)) == 2

    def test_queue_status_counts_waiting_and_claimed(self, real_redis):
        real_redis.rpush("spatial", "a", "b")
        message = lambda task: json.dumps([{"headers": {"task": task}, "body": ""}, "", "spatial"])
        real_redis.hset("spatial_unacked", mapping={"t1": message("spatial:process_chunk"), "t2": message("spatial:process_chunk")})
        real_redis.zadd("spatial_unacked_index", {"t1": time.time() - 600, "t2": time.time()})
        status = admin_api.queue_status()
        assert status["queues"]["spatial"] == 2 and status["queues"]["process"] == 0
        claimed = status["claimed"]["spatial_unacked"]
        assert claimed["count"] == 2 and claimed["tasks"] == {"spatial:process_chunk": 2}
        assert 590 <= claimed["oldest_claim_seconds"] <= 610


class TestAdminRoutes:
    @pytest.fixture
    def client(self):
        app = Flask(__name__, template_folder=os.path.join(os.path.dirname(__file__), "..", "templates"))
        app.register_blueprint(admin_api.admin_bp)
        with app.test_client() as client:
            yield client

    def test_listing_routes(self, client):
        with mock.patch.object(tm, "list_databases", return_value=[{"name": "db"}]) as dbs, \
                mock.patch.object(tm, "list_repack_jobs", return_value=[]), \
                mock.patch.object(admin_api, "queue_status", return_value={"queues": {}, "claimed": {}}):
            assert client.get("/materialize/admin/api/databases?sizes=false").json == {"databases": [{"name": "db"}]}
            dbs.assert_called_once_with(with_sizes=False)
            assert client.get("/materialize/admin/api/repack/jobs").json == {"jobs": []}
            assert client.get("/materialize/admin/api/queues").json == {"queues": {}, "claimed": {}}

    def test_datastacks_route(self, client):
        with mock.patch("materializationengine.utils.get_config_param", return_value=["minnie65_phase3_v1", "zheng_ca3"]):
            assert client.get("/materialize/admin/api/datastacks").json == {"datastacks": ["minnie65_phase3_v1", "zheng_ca3"]}


DATASTACKS = ["minnie65_phase3_v1", "zheng_ca3"]
DATASETS = {"minnie65_phase3_v1": "minnie65", "zheng_ca3": "zheng-mouse-hc"}


@pytest.fixture
def permission_app():
    """A request context with the configured datastacks and their auth datasets."""
    app = Flask(__name__)
    admin_api._dataset_for.cache_clear()
    with mock.patch("materializationengine.utils.get_config_param", return_value=DATASTACKS), \
            mock.patch.object(admin_api, "dataset_from_table_id_from_request", side_effect=DATASETS.__getitem__):
        yield app
    admin_api._dataset_for.cache_clear()


def _as(app, user):
    ctx = app.test_request_context("/materialize/admin/")
    ctx.push()
    g.auth_user = user
    return ctx


class TestCapabilities:
    def test_superadmin_gets_every_datastack(self, permission_app):
        ctx = _as(permission_app, {"admin": True})
        try:
            caps = admin_api.capabilities()
            assert caps["superadmin"] is True
            assert [(d["name"], d["admin"], d["edit"]) for d in caps["datastacks"]] == [
                ("minnie65_phase3_v1", True, True), ("zheng_ca3", True, True)]
            assert admin_api.can_see_admin_page()
        finally:
            ctx.pop()

    def test_dataset_admin_gets_only_their_datastacks(self, permission_app):
        ctx = _as(permission_app, {"admin": False, "datasets_admin": ["minnie65"], "permissions_v2": {"minnie65": ["view", "edit"]}})
        try:
            caps = admin_api.capabilities()
            assert caps["superadmin"] is False
            assert [(d["name"], d["dataset"], d["admin"], d["edit"]) for d in caps["datastacks"]] == [
                ("minnie65_phase3_v1", "minnie65", True, True)]
            assert admin_api.can_see_admin_page()
        finally:
            ctx.pop()

    def test_edit_only_user_sees_edit_actions_but_not_the_page(self, permission_app):
        ctx = _as(permission_app, {"admin": False, "datasets_admin": [], "permissions_v2": {"zheng-mouse-hc": ["view", "edit"]}})
        try:
            caps = admin_api.capabilities()
            assert [(d["name"], d["admin"], d["edit"]) for d in caps["datastacks"]] == [("zheng_ca3", False, True)]
            assert not admin_api.can_see_admin_page()
        finally:
            ctx.pop()

    def test_view_only_user_has_nothing(self, permission_app):
        ctx = _as(permission_app, {"admin": False, "datasets_admin": [], "permissions_v2": {"minnie65": ["view"]}})
        try:
            assert admin_api.capabilities()["datastacks"] == []
            assert not admin_api.can_see_admin_page()
            # the view itself (the auth decorators replace the user when AUTH_DISABLED is set)
            with pytest.raises(Exception) as refused:
                inspect.unwrap(admin_api.admin_page)()
            assert getattr(refused.value, "code", None) == 403
        finally:
            ctx.pop()

    def test_dataset_lookup_failure_hides_the_datastack_from_non_superadmins(self):
        app = Flask(__name__)
        admin_api._dataset_for.cache_clear()
        with mock.patch("materializationengine.utils.get_config_param", return_value=DATASTACKS), \
                mock.patch.object(admin_api, "dataset_from_table_id_from_request", side_effect=RuntimeError("auth down")):
            ctx = _as(app, {"admin": False, "datasets_admin": ["minnie65"], "permissions_v2": {}})
            try:
                assert admin_api.capabilities()["datastacks"] == []
            finally:
                ctx.pop()
        admin_api._dataset_for.cache_clear()

    def test_no_user_means_no_link(self):
        app = Flask(__name__)
        with app.test_request_context("/"):
            assert not admin_api.can_see_admin_page()


class TestDumpChoices:
    """The versions and tables routes that fill the CSV dump form: datastack admins only."""

    @pytest.fixture
    def app(self, permission_app):
        permission_app.register_blueprint(admin_api.admin_bp)
        return permission_app

    def call(self, app, user, view, **kwargs):
        ctx = _as(app, user)
        try:
            return inspect.unwrap(view)(**kwargs)
        finally:
            ctx.pop()

    def test_dataset_admin_gets_versions_and_tables_of_their_datastack(self, app):
        user = {"admin": False, "datasets_admin": ["minnie65"], "permissions_v2": {}}
        with mock.patch.object(tm, "list_frozen_versions", return_value=[{"version": 3}]) as versions, \
                mock.patch.object(tm, "list_relations", return_value=[{"name": "synapses"}]) as relations:
            assert self.call(app, user, admin_api.frozen_versions, datastack="minnie65_phase3_v1").json == {"versions": [{"version": 3}]}
            versions.assert_called_once_with("minnie65_phase3_v1")
            r = self.call(app, user, admin_api.version_tables, datastack="minnie65_phase3_v1", version=3)
            assert r.json == {"database": "minnie65_phase3_v1__mat3", "tables": [{"name": "synapses"}]}
            relations.assert_called_once_with("minnie65_phase3_v1__mat3")

    @pytest.mark.parametrize("datastack", ["zheng_ca3", "not_configured"])
    def test_other_datastacks_are_refused(self, app, datastack):
        user = {"admin": False, "datasets_admin": ["minnie65"], "permissions_v2": {"zheng-mouse-hc": ["edit"]}}
        for view, kwargs in ((admin_api.frozen_versions, {}), (admin_api.version_tables, {"version": 3})):
            with pytest.raises(Exception) as refused:
                self.call(app, user, view, datastack=datastack, **kwargs)
            assert getattr(refused.value, "code", None) == 403

    def test_missing_version_is_not_found(self, app):
        with mock.patch.object(tm, "list_relations", side_effect=tm.RepackRefused("database does not exist")):
            with pytest.raises(Exception) as missing:
                self.call(app, {"admin": True}, admin_api.version_tables, datastack="minnie65_phase3_v1", version=99)
        assert getattr(missing.value, "code", None) == 404


class TestDatabasesFilter:
    def test_filters_to_the_datastacks_live_and_frozen_databases(self):
        app = Flask(__name__)
        app.register_blueprint(admin_api.admin_bp)
        dbs = [{"name": "minnie65_phase3", "kind": "live"}, {"name": "zheng_ca3", "kind": "live"},
               {"name": "minnie65_phase3_v1__mat1935", "kind": "frozen", "datastack": "minnie65_phase3_v1"},
               {"name": "zheng_ca3__mat9", "kind": "frozen", "datastack": "zheng_ca3"}]
        with mock.patch.object(tm, "list_databases", return_value=dbs), \
                mock.patch("materializationengine.info_client.get_datastack_info", return_value={"aligned_volume": {"name": "minnie65_phase3"}}):
            r = app.test_client().get("/materialize/admin/api/databases?datastack=minnie65_phase3_v1")
        assert [d["name"] for d in r.json["databases"]] == ["minnie65_phase3", "minnie65_phase3_v1__mat1935"]


class TestAdminPageRenders:
    @pytest.mark.parametrize("show_link", [True, False])
    def test_page_renders_with_the_nav(self, show_link):
        from materializationengine.blueprints.deltalake.api import deltalake_bp
        from materializationengine.blueprints.upload.api import upload_bp
        from materializationengine.views import views_bp

        root = os.path.join(os.path.dirname(__file__), "..")
        app = Flask(__name__, template_folder=os.path.join(root, "templates"), static_folder=os.path.join(root, "static"))
        for bp in (views_bp, upload_bp, deltalake_bp, admin_api.admin_bp):
            app.register_blueprint(bp)

        @app.context_processor
        def inject():
            return {"show_admin_link": show_link}

        with app.test_request_context("/materialize/admin/"), \
                mock.patch.object(admin_api, "can_see_admin_page", return_value=True):
            g.auth_user = {"admin": True, "name": "Test User"}
            html = admin_api.admin_page()
        for marker in ("tab-datastack", "tab-deployment", 'data-needs="superadmin"', 'data-needs="admin"', 'data-needs="edit"', "js/admin.js"):
            assert marker in html, marker
        assert ('href="/materialize/admin/"' in html) is show_link

    def test_base_template_shows_the_link_from_the_context_flag(self):
        base = open(os.path.join(os.path.dirname(__file__), "..", "templates", "base.html")).read()
        assert "{% if show_admin_link %}" in base and "mat_admin.admin_page" in base
