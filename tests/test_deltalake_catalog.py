"""The Delta Lake catalog: rows per lake through writing -> complete/failed, checked
against real local lakes, and the endpoint that lists them."""

import uuid
from unittest import mock

import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.engine.url import make_url

from materializationengine import deltalake_catalog as cat


@pytest.fixture
def aligned_volume(mat_metadata):
    url = make_url(mat_metadata["sql_uri"])
    base = f"{url.drivername}://{url.username}:{url.password}@{url.host}:{url.port or 5432}"
    name = f"catalog_test_{uuid.uuid4().hex[:8]}"
    admin = sa.create_engine(f"{base}/postgres", isolation_level="AUTOCOMMIT")
    with admin.connect() as conn:
        conn.execute(f"CREATE DATABASE {name}")
    engine = sa.create_engine(f"{base}/{name}")
    from materializationengine.database import db_manager
    with mock.patch.object(db_manager, "get_engine", return_value=engine):
        yield name, engine
    cat._ensured.discard(str(engine.url))
    engine.dispose()
    with admin.connect() as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {name}")
    admin.dispose()


def lake(tmp_path, name, rows):
    from deltalake import write_deltalake

    uri = str(tmp_path / name)
    write_deltalake(uri, pa.table({"id": pa.array(range(rows), pa.int64()),
                                   "post_pt_root_id": pa.array([7] * rows, pa.int64()),
                                   "ctr_pt_position_x": pa.array([1] * rows, pa.int32())}))
    return {"spec_name": name, "uri": uri, "partition_by": "post_pt_root_id",
            "spec": {"name": name, "partition_by": "post_pt_root_id", "n_partitions": "auto"}}


def rows(engine):
    with engine.connect() as conn:
        return {r["spec_name"]: dict(r) for r in conn.execute("SELECT * FROM deltalake_exports")}


class TestCatalog:
    def test_writing_then_complete(self, aligned_volume, tmp_path):
        name, engine = aligned_volume
        lakes = [lake(tmp_path, "id", 3), lake(tmp_path, "post_pt_root_id", 3)]
        cat.record_writing(name, "ds", 7, "synapses", lakes, job_id="j1")  # creates the table on first use
        assert {r["status"] for r in rows(engine).values()} == {"writing"}

        lakes[1]["spec"] = {**lakes[1]["spec"], "n_partitions": 4}  # resolved during the export
        assert cat.record_finished(name, "ds", 7, "synapses", lakes, expected_rows=3) == []
        r = rows(engine)["post_pt_root_id"]
        assert r["status"] == "complete" and r["row_count"] == 3 and r["files"] == 1 and r["bytes"] > 0
        assert r["completed_at"] is not None and r["job_id"] == "j1"
        assert {c["name"] for c in r["columns"]} == {"id", "post_pt_root_id", "ctr_pt_position_x"}
        assert r["spec"]["n_partitions"] == 4

        listed = cat.list_exports(name, "ds", 7)
        assert [(x["table_name"], x["spec_name"]) for x in listed] == [("synapses", "id"), ("synapses", "post_pt_root_id")]
        assert isinstance(listed[0]["completed_at"], str)  # JSON-ready
        assert "id" not in listed[0]  # the catalog's own primary key is not exposed

    def test_row_count_mismatch_and_unreadable_lake_fail(self, aligned_volume, tmp_path):
        name, engine = aligned_volume
        short = lake(tmp_path, "id", 2)
        missing = {"spec_name": "pre_pt_root_id", "uri": str(tmp_path / "nope"), "partition_by": "pre_pt_root_id"}
        cat.record_writing(name, "ds", 7, "synapses", [short, missing])
        assert sorted(cat.record_finished(name, "ds", 7, "synapses", [short, missing], expected_rows=3)) == [
            "id", "pre_pt_root_id"]
        r = rows(engine)
        assert r["id"]["status"] == "failed" and "2 rows, expected 3" in r["id"]["error"]
        assert r["pre_pt_root_id"]["status"] == "failed" and "could not read" in r["pre_pt_root_id"]["error"]
        assert cat.list_exports(name, "ds", 7) == []
        assert len(cat.list_exports(name, "ds", 7, statuses=None)) == 2

    def test_a_view_of_unknown_size_accepts_any_count(self, aligned_volume, tmp_path):
        name, engine = aligned_volume
        v = lake(tmp_path, "flat", 5)
        cat.record_writing(name, "ds", 7, "connections", [v])
        assert cat.record_finished(name, "ds", 7, "connections", [v], expected_rows=None) == []
        assert rows(engine)["flat"]["status"] == "complete"

    def test_reexport_resets_the_row(self, aligned_volume, tmp_path):
        name, engine = aligned_volume
        x = lake(tmp_path, "id", 3)
        cat.record_writing(name, "ds", 7, "synapses", [x])
        cat.record_finished(name, "ds", 7, "synapses", [x], expected_rows=3)
        cat.record_writing(name, "ds", 7, "synapses", [x], job_id="j2")
        r = rows(engine)
        assert len(r) == 1 and r["id"]["status"] == "writing" and r["id"]["row_count"] is None
        assert r["id"]["job_id"] == "j2" and r["id"]["completed_at"] is None

    def test_failed_and_forget(self, aligned_volume, tmp_path):
        name, engine = aligned_volume
        lakes = [lake(tmp_path, "id", 1), lake(tmp_path, "post_pt_root_id", 1)]
        cat.record_writing(name, "ds", 7, "small", lakes)
        cat.forget(name, "ds", 7, "small", ["post_pt_root_id"])
        cat.record_failed(name, "ds", 7, "small", ["id"], "RuntimeError: boom")
        r = rows(engine)
        assert set(r) == {"id"} and r["id"]["status"] == "failed" and r["id"]["error"] == "RuntimeError: boom"

    def test_safely_never_raises(self):
        def broken():
            raise RuntimeError("database down")
        assert cat.safely(broken) is None


class TestEndpoint:
    @pytest.fixture
    def client(self, monkeypatch):
        from flask import Flask
        from flask_restx import Api

        import middle_auth_client.decorators as auth
        from materializationengine.blueprints.client import api2

        monkeypatch.setattr(auth, "AUTH_DISABLED", True)
        monkeypatch.setattr(api2, "get_relevant_datastack_info", lambda ds: ("minnie65_phase3", "pcg"))
        app = Flask(__name__)
        Api(app).add_namespace(api2.client_bp, path="/api/v3")
        with app.test_client() as client:
            yield client

    def test_lists_complete_lakes_with_https_urls(self, client):
        lakes = [{"table_name": "synapses_pni_2", "spec_name": "post_pt_root_id",
                  "uri": "gs://mat_dbs/public/deltalake/minnie65_phase3_v1/v2253/synapses_pni_2/post_pt_root_id"}]
        with mock.patch.object(cat, "list_exports", return_value=lakes) as listed:
            r = client.get("/api/v3/datastack/minnie65_phase3_v1/version/2253/deltalake")
        assert r.status_code == 200, r.get_data()
        assert listed.call_args.args == ("minnie65_phase3", "minnie65_phase3_v1", 2253)
        assert listed.call_args.kwargs["statuses"] == ("complete",)
        assert r.json[0]["https_url"] == ("https://storage.googleapis.com/mat_dbs/public/deltalake/"
                                          "minnie65_phase3_v1/v2253/synapses_pni_2/post_pt_root_id")

    def test_all_statuses(self, client):
        with mock.patch.object(cat, "list_exports", return_value=[]) as listed:
            client.get("/api/v3/datastack/minnie65_phase3_v1/version/2253/deltalake?all_statuses=true")
        assert listed.call_args.kwargs["statuses"] is None
