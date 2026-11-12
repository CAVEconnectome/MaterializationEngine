"""The view pilot's three engines build the same lake for minnie's views (local Postgres,
local Delta lakes, Daft's local runner)."""

import json

import pytest
from sqlalchemy.engine.url import make_url

from materializationengine.workflows import view_pilot as vp
from tests.test_lake_views import PCG, TARGETS, frozen  # noqa: F401  (frozen is a fixture)


@pytest.fixture(scope="module")
def lakes(frozen, tmp_path_factory):  # noqa: F811
    from deltalake import write_deltalake

    _, tables = frozen
    root = tmp_path_factory.mktemp("lakes")
    out = {}
    for table, data in tables.items():
        write_deltalake(str(root / table), data)
        out[table] = {"table_name": table, "uri": str(root / table)}
    return out


class TestViewPilot:
    def test_reads_the_view_definitions_back(self, frozen):  # noqa: F811
        engine, _ = frozen
        defs, cols = vp.view_definitions(engine)
        assert set(TARGETS) <= set(defs)
        assert {"name": "pt_position", "type": "geometry(PointZ)"} in cols["single_neurons"]

    @pytest.mark.parametrize("view", TARGETS)
    def test_every_engine_builds_the_same_lake(self, frozen, lakes, view, tmp_path):  # noqa: F811
        pytest.importorskip("daft")
        engine, _ = frozen
        defs, cols = vp.view_definitions(engine)
        plan = vp.plan_view(view, defs, cols, lakes, PCG)
        url = make_url(str(engine.url))
        conn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"
        results = [vp.run_one(e, view, str(tmp_path / e), plan=plan, lakes=lakes, connection_string=conn,
                              columns=cols[view]) for e in vp.ENGINES]
        assert [r.get("error") for r in results] == [None, None, None]
        assert results[0]["rows"] > 0 and results[0]["bytes"] > 0
        assert vp.compare(results) == {view: {"agree": True, "engines": sorted(vp.ENGINES), "failed": []}}

    def test_a_failing_engine_is_recorded_not_raised(self, tmp_path):
        r = vp.run_one("duckdb", "v", str(tmp_path / "x"), plan=vp.lv.LakeView("SELECT nonsense", {"t"}, []),
                       lakes={"t": {"uri": str(tmp_path / "missing")}})
        assert "error" in r and "seconds" not in r
        assert vp.compare([r]) == {"v": {"agree": False, "engines": [], "failed": ["duckdb"]}}


class TestEndpoint:
    @pytest.fixture
    def client(self, monkeypatch):
        from flask import Flask
        from flask_restx import Api

        import middle_auth_client.decorators as auth
        from materializationengine.blueprints.materialize import api

        monkeypatch.setattr(auth, "AUTH_DISABLED", True)
        app = Flask(__name__)
        app.config["DELTALAKE_OUTPUT_BUCKET"] = "gs://bucket/deltalake"
        Api(app).add_namespace(api.mat_bp, path="/api/v2")
        with app.test_client() as client:
            yield client

    URL = "/api/v2/materialize/run/view_pilot/datastack/minnie65_phase3_v1/version/2253/"

    def test_submits_one_rayjob_on_the_pilot_image(self, client):
        from unittest import mock

        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob", return_value="mat-view-pilot-v2253-x") as submit:
            r = client.post(self.URL + "?workers=6", json={
                "views": ["synapses_pni_2_in_out_degree", "connections_with_nuclei"], "engines": ["duckdb", "daft"],
                "rounds": 2, "duckdb_memory": "24GB", "image_tag": "vDeltaVersion12-viewpilot",
                "task_resources": {"num_cpus": 8, "memory": 32000000000}})
        assert r.status_code == 200, r.get_data()
        kw = submit.call_args.kwargs
        assert "view_pilot --datastack minnie65_phase3_v1 --version 2253 --run-id " in kw["entrypoint"]
        assert "--views synapses_pni_2_in_out_degree connections_with_nuclei --engines duckdb daft" in kw["entrypoint"]
        assert "--rounds 2" in kw["entrypoint"] and "--duckdb-memory 24GB" in kw["entrypoint"]
        assert kw["image_tag"] == "vDeltaVersion12-viewpilot" and kw["num_workers"] == 6
        assert json.loads(kw["env"]["RAY_VIEW_PILOT_RESOURCES"]) == {"num_cpus": 8, "memory": 32000000000}
        assert r.json["results"] == f"gs://bucket/deltalake/minnie65_phase3_v1/v2253/_pilot/{r.json['run_id']}/results.json"

    @pytest.mark.parametrize("body", [
        {}, {"views": "connections"}, {"views": ["a; rm -rf"]}, {"views": ["v"], "engines": ["spark"]},
        {"views": ["v"], "rounds": 0}, {"views": ["v"], "duckdb_memory": "lots"}, {"views": ["v"], "image_tag": "a b"},
        {"views": ["v"], "task_resources": {"num_gpus": 1}}, {"views": ["v"], "task_resources": "big"},
    ])
    def test_bad_requests(self, client, body):
        from unittest import mock

        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob") as submit:
            assert client.post(self.URL, json=body).status_code == 400
        submit.assert_not_called()
