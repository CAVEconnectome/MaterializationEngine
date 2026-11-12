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
        engines = vp.ENGINES + vp.EXTRA_ENGINES
        results = [vp.run_one(e, view, str(tmp_path / e), plan=plan, lakes=lakes, connection_string=conn,
                              columns=cols[view]) for e in engines]
        assert [r.get("error") for r in results] == [None] * len(engines)
        assert results[0]["rows"] > 0 and results[0]["bytes"] > 0
        assert vp.compare(results) == {view: {"agree": True, "engines": sorted(engines), "failed": []}}
        by_engine = {r["engine"]: r for r in results}
        assert by_engine["duckdb"]["operator_seconds"] and by_engine["duckdb_arrow"]["operator_seconds"]

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
                "task_resources": {"num_cpus": 8, "memory": 32000000000}, "worker": {"cpus": 3, "memory": "11Gi"}})
        assert r.status_code == 200, r.get_data()
        kw = submit.call_args.kwargs
        assert "view_pilot --datastack minnie65_phase3_v1 --version 2253 --run-id " in kw["entrypoint"]
        assert "--views synapses_pni_2_in_out_degree connections_with_nuclei --engines duckdb daft" in kw["entrypoint"]
        assert "--rounds 2" in kw["entrypoint"] and "--duckdb-memory 24GB" in kw["entrypoint"]
        assert kw["image_tag"] == "vDeltaVersion12-viewpilot" and kw["num_workers"] == 6
        assert json.loads(kw["env"]["RAY_VIEW_PILOT_RESOURCES"]) == {"num_cpus": 8, "memory": 32000000000}
        assert (kw["worker_cpus"], kw["worker_memory"]) == (3, "11Gi")
        assert r.json["results"] == f"gs://bucket/deltalake/minnie65_phase3_v1/v2253/_pilot/{r.json['run_id']}/results.json"

    @pytest.mark.parametrize("body", [
        {}, {"views": "connections"}, {"views": ["a; rm -rf"]}, {"views": ["v"], "engines": ["spark"]},
        {"views": ["v"], "rounds": 0}, {"views": ["v"], "duckdb_memory": "lots"}, {"views": ["v"], "image_tag": "a b"},
        {"views": ["v"], "task_resources": {"num_gpus": 1}}, {"views": ["v"], "task_resources": "big"},
        {"views": ["v"], "worker": {"cpus": 0}}, {"views": ["v"], "worker": "big"}, {"views": ["v"], "worker": {"memory": "lots"}},
    ])
    def test_bad_requests(self, client, body):
        from unittest import mock

        with mock.patch("materializationengine.rayjobs.ray_enabled", return_value=True), \
                mock.patch("materializationengine.rayjobs.submit_rayjob") as submit:
            assert client.post(self.URL, json=body).status_code == 400
        submit.assert_not_called()


class TestToDelta:
    def test_commits_what_convert_to_deltalake_would(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from deltalake import DeltaTable, convert_to_deltalake

        for name in ("ours", "theirs"):
            (tmp_path / name).mkdir()
            for i in range(5):
                pq.write_table(pa.table({"id": pa.array(range(i * 100, i * 100 + 100 + i), pa.int64()),
                                         "size": pa.array([float(i)] * (100 + i))}),
                               tmp_path / name / f"part-{i}.parquet", compression="snappy")
        assert vp._to_delta(str(tmp_path / "ours")) >= 0
        convert_to_deltalake(str(tmp_path / "theirs"))

        def facts(uri):
            adds = pa.table(DeltaTable(uri).get_add_actions(flatten=True)).to_pylist()
            return sorted((a["path"], a["size_bytes"], a["num_records"], a["min.id"], a["max.id"]) for a in adds)

        assert facts(str(tmp_path / "ours")) == facts(str(tmp_path / "theirs"))
        assert DeltaTable(str(tmp_path / "ours")).to_pyarrow_table().num_rows == 510

    def test_refuses_an_empty_directory(self, tmp_path):
        with pytest.raises(ValueError, match="no Parquet files"):
            vp._to_delta(str(tmp_path))

    def test_commits_only_the_files_the_writer_reported(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq
        from deltalake import DeltaTable

        for i in range(3):  # part-2 stands for a lost task's orphan
            pq.write_table(pa.table({"id": pa.array(range(10), pa.int64())}), tmp_path / f"part-{i}.parquet")
        vp._to_delta(str(tmp_path), paths=[str(tmp_path / "part-0.parquet"), f"file://{tmp_path}/part-1.parquet"])
        assert DeltaTable(str(tmp_path)).to_pyarrow_table().num_rows == 20

    def test_a_reported_file_that_is_not_there_is_refused(self, tmp_path):
        import pyarrow as pa
        import pyarrow.parquet as pq

        pq.write_table(pa.table({"id": pa.array([1], pa.int64())}), tmp_path / "part-0.parquet")
        with pytest.raises(ValueError, match="not under"):
            vp._to_delta(str(tmp_path), paths=[str(tmp_path / "part-0.parquet"), str(tmp_path / "gone.parquet")])
