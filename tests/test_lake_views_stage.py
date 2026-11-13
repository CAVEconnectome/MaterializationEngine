"""The views stage: minnie's real view definitions, built by Daft from table lakes recorded
in the catalog, must match Postgres, land as sorted copies recorded like tables, resume,
and survive a lost attempt."""

from types import SimpleNamespace

import pytest

from materializationengine.workflows import lake_views_stage as stage
from materializationengine.workflows import view_pilot as vp
from tests.test_deltalake_catalog import aligned_volume  # noqa: F401  (fixture)
from tests.test_lake_views import COLS, DEFS, PCG, TARGETS, frozen  # noqa: F401  (fixture)

EXPECTED_COPIES = {
    "nucleus_detection_lookup_v1": ["pt_root_id", "orig_root_id"],
    "synapses_pni_2_in_out_degree": ["root_id"],
    "connections_with_nuclei": ["pre_pt_root_id", "post_pt_root_id"],
}


@pytest.fixture
def version(frozen, aligned_volume, tmp_path, monkeypatch):  # noqa: F811
    """A version whose tables have complete lakes in the catalog, and a views-stage context;
    the live database's view definitions are minnie's (the frozen fixture's database)."""
    from deltalake import write_deltalake

    from materializationengine import deltalake_catalog as cat

    pytest.importorskip("daft")
    engine, tables = frozen
    av, _ = aligned_volume
    bucket = tmp_path / "bucket"
    for table, data in tables.items():
        uri = str(bucket / "ds" / "v7" / table / "id")
        write_deltalake(uri, data)
        lake = {"spec_name": "id", "uri": uri, "partition_by": "id", "spec": {"name": "id", "partition_by": "id"}}
        cat.record_writing(av, "ds", 7, table, [lake], job_id="tables")
        assert cat.record_finished(av, "ds", 7, table, [lake], expected_rows=data.num_rows) == []
    monkeypatch.setattr(stage, "view_definitions", lambda _engine: vp.view_definitions(engine))
    monkeypatch.setattr(stage, "registered_views", lambda _engine, _ds: list(TARGETS))
    monkeypatch.setattr(stage, "_wait_for_cluster", lambda cpus, wait: None)

    def ctx(job_id="job1"):
        return SimpleNamespace(datastack_info={"datastack": "ds", "segmentation_source": f"graphene://x/{PCG}"},
                               datastack="ds", version=7, aligned_volume=av, job_id=job_id,
                               settings=SimpleNamespace(output_bucket=str(bucket)), tables={},
                               lakes=cat.list_exports(av, "ds", 7))

    return SimpleNamespace(engine=engine, ctx=ctx, bucket=bucket, av=av)


def _postgres_summary(engine, view, tmp_path):
    from sqlalchemy.engine.url import make_url

    url = make_url(str(engine.url))
    conn = f"postgresql://{url.username}:{url.password}@{url.host}:{url.port or 5432}/{url.database}"
    out = str(tmp_path / f"pg_{view}")
    vp.build_postgres(conn, view, COLS[view], out)
    return vp.summarize(out)


class TestViewsStage:
    def test_builds_every_view_as_sorted_copies_that_match_postgres(self, version, tmp_path):
        import pyarrow.parquet as pq
        from deltalake import DeltaTable

        from materializationengine import deltalake_catalog as cat

        result = stage.build_views(version.ctx())
        assert result["state"] == "done", result
        assert set(result["detail"]["built"]) == set(TARGETS) and not result["detail"]["failed"]
        for view, keys in EXPECTED_COPIES.items():
            recorded = {r["spec_name"]: r for r in cat.list_exports(version.av, "ds", 7, table_name=view)}
            assert {s: (r["status"], r["partition_by"], r["job_id"]) for s, r in recorded.items()} == \
                {k: ("complete", k, f"job1.{view}") for k in keys}
            expected = _postgres_summary(version.engine, view, tmp_path)
            assert expected["rows"] > 0
            for key in keys:
                uri = str(version.bucket / "ds" / "v7" / view / key)
                got = vp.summarize(uri)
                assert (got["rows"], got["checksum"]) == (expected["rows"], expected["checksum"]), (view, key)
                for f in DeltaTable(uri).file_uris():  # sorted by the copy's key, row group by row group
                    md = pq.ParquetFile(f).metadata
                    col = md.schema.to_arrow_schema().get_field_index(key)
                    mins = [md.row_group(i).column(col).statistics.min for i in range(md.num_row_groups)]
                    assert mins == sorted(mins)
                assert recorded[key]["row_count"] == expected["rows"] and recorded[key]["spec"]["view"]

    def test_a_restarted_driver_skips_the_views_it_finished(self, version):
        assert stage.build_views(version.ctx())["state"] == "done"
        again = stage.build_views(version.ctx())
        assert again["state"] == "done" and not again["detail"]["built"]
        assert set(again["detail"]["skipped"]) == set(TARGETS)

    def test_a_lake_another_export_wrote_is_refused(self, version):
        assert stage.build_views(version.ctx("job1"))["state"] == "done"
        other = stage.build_views(version.ctx("job2"))
        assert other["state"] == "failed" and "another export wrote" in other["detail"]["failed"]["connections_with_nuclei"]

    def test_a_view_reading_a_table_without_a_lake_is_skipped(self, version, monkeypatch):
        defs = {**DEFS, "needs_more": "SELECT id FROM some_other_table"}
        monkeypatch.setattr(stage, "view_definitions", lambda _engine: (defs, COLS))
        monkeypatch.setattr(stage, "registered_views", lambda _engine, _ds: ["needs_more"])
        result = stage.build_views(version.ctx())
        assert result["state"] == "done" and "no lake" in result["detail"]["skipped"]["needs_more"]

    def test_a_lost_attempt_is_built_again_and_its_files_removed(self, version, monkeypatch):
        import pyarrow as pa
        import pyarrow.parquet as pq

        real = stage.build_view
        calls = []

        def flaky(plan, lakes, copies, row_group_rows=10000):
            calls.append(1)
            if len(calls) == 1:  # a worker was lost after writing a file into the first copy
                first = copies[0][2]
                import os

                os.makedirs(first, exist_ok=True)
                pq.write_table(pa.table({"x": [1]}), f"{first}/lost-attempt.parquet")
                raise RuntimeError("ObjectReconstructionFailedError")
            return real(plan, lakes, copies, row_group_rows)

        monkeypatch.setattr(stage, "build_view", flaky)
        monkeypatch.setattr(stage, "registered_views", lambda _engine, _ds: ["synapses_pni_2_in_out_degree"])
        result = stage.build_views(version.ctx())
        built = result["detail"]["built"]["synapses_pni_2_in_out_degree"]
        assert result["state"] == "done" and len(calls) == 2
        assert [a["attempt"] for a in built["failed_attempts"]] == [1] and built["orphans_removed"] == 1
        uri = str(version.bucket / "ds" / "v7" / "synapses_pni_2_in_out_degree" / "root_id")
        assert vp.summarize(uri)["rows"] == built["rows"]
        import os

        assert "lost-attempt.parquet" not in os.listdir(uri)

    def test_gives_up_after_its_attempts_and_fails_the_stage(self, version, monkeypatch):
        from materializationengine import deltalake_catalog as cat

        monkeypatch.setattr(stage, "build_view", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("down")))
        monkeypatch.setattr(stage, "registered_views", lambda _engine, _ds: ["synapses_pni_2_in_out_degree"])
        result = stage.build_views(version.ctx())
        assert result["state"] == "failed" and "down" in result["detail"]["failed"]["synapses_pni_2_in_out_degree"]
        rows = cat.list_exports(version.av, "ds", 7, table_name="synapses_pni_2_in_out_degree", statuses=None)
        assert [r["status"] for r in rows] == ["failed"]


class TestPieces:
    def test_copy_keys(self):
        assert stage.copy_keys(["pre_pt_root_id", "n_syn", "post_pt_root_id"]) == ["pre_pt_root_id", "post_pt_root_id"]
        assert stage.copy_keys(["id", "volume"]) == [None]

    def test_one_lake_per_table_the_smallest(self):
        rows = [{"table_name": "t", "spec_name": "a", "bytes": 9, "status": "complete"},
                {"table_name": "t", "spec_name": "b", "bytes": 3, "status": "complete"},
                {"table_name": "t", "spec_name": "c", "bytes": 1, "status": "writing"}]
        assert stage.table_lakes(rows)["t"]["spec_name"] == "b"

    def test_views_to_build_from_config(self, monkeypatch):
        monkeypatch.setattr("materializationengine.utils.get_config_param",
                            lambda key, default=None: '["a", "b", "a"]' if key == "DELTALAKE_EXPORT_VIEWS" else default)
        assert stage._views_to_build("ds", ["x"]) == ["a", "b"]


class TestThroughTheExportHook:
    def test_the_config_string_runs_the_stage_through_the_version_job(self, version):
        from materializationengine.workflows.lake_export import version as lev

        hook = lev.resolve_stage("materializationengine.workflows.lake_views_stage:build_views")
        result = lev.run_views_stage(version.ctx(), hook)
        assert isinstance(result, lev.StageResult) and result.state == "done" and result.error is None
        assert set(result.detail["built"]) == set(TARGETS)


class TestRegisteredViews:
    """registered_views against a real database (every other test stubs it out, which is how
    an Inspector.has_table call, SQLAlchemy 1.4+, reached ltv7 under the locked 1.3)."""

    def test_no_analysisviews_table_means_no_views(self, frozen):  # noqa: F811
        engine, _ = frozen
        with engine.begin() as conn:
            conn.execute("DROP TABLE IF EXISTS analysisviews")
        assert stage.registered_views(engine, "ds") == []

    def test_the_datastacks_views_in_registration_order(self, frozen):  # noqa: F811
        engine, _ = frozen
        with engine.begin() as conn:
            conn.execute("CREATE TABLE analysisviews (id serial PRIMARY KEY, table_name varchar, datastack_name varchar)")
            conn.execute("INSERT INTO analysisviews (table_name, datastack_name) VALUES "
                         "('second_built_first', 'ds'), ('other', 'elsewhere'), ('then_this', 'ds')")
        try:
            assert stage.registered_views(engine, "ds") == ["second_built_first", "then_this"]
        finally:
            with engine.begin() as conn:
                conn.execute("DROP TABLE analysisviews")
