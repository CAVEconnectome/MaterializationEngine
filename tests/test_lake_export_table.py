"""lake_export end to end: tables of a throwaway frozen-shaped PostGIS database exported to
local lakes through the real scheduler -- units on threads (ThreadExecutor), and once on
a local Ray cluster (the RayJob entrypoint's executor). Planning, streaming, publishing,
optimizing, finalizing, the catalog, the status record, and resuming a failed job."""

import json
import threading

import pytest
import sqlalchemy as sa

from materializationengine import deltalake_catalog as cat
from materializationengine.workflows.lake_export import plan as planning
from materializationengine.workflows.lake_export import publish as pub
from materializationengine.workflows.lake_export import status as status_mod
from materializationengine.workflows.lake_export import storage
from materializationengine.workflows.lake_export import table as table_mod
from materializationengine.workflows.lake_export.job import TableRequest, run_tables
from materializationengine.workflows.lake_export.scheduler import ThreadExecutor
from materializationengine.workflows.lake_export.settings import ExportSettings
from materializationengine.workflows.lake_export.sizing import SizingConfig
from tests.test_lake_export_ranges import SEG, TABLE, frozen  # noqa: F401

DATASTACK, VERSION, ALIGNED = "ds", 7, "av"


@pytest.fixture
def catalog(frozen, monkeypatch):
    """The catalog table in the throwaway database, as the aligned volume's."""
    from materializationengine.database import db_manager

    monkeypatch.setattr(db_manager, "get_engine", lambda name: frozen["engine"])
    yield frozen["engine"]
    with frozen["engine"].begin() as conn:
        conn.execute("DROP TABLE IF EXISTS deltalake_exports")
    cat._ensured.discard(str(frozen["engine"].url))


@pytest.fixture
def statuses(monkeypatch):
    """Every status record and log line, by key, instead of Redis."""
    records, logs = {}, {}
    monkeypatch.setattr(status_mod, "_redis_writer", lambda key, payload: records.setdefault(key, []).append(payload))
    monkeypatch.setattr(status_mod, "_redis_logger",
                        lambda ds, v, t, job, msg: logs.setdefault(job, []).append(msg))
    return records, logs


def settings_for(tmp_path, split=True, **kw):
    # tiny ranges and partitions, so a 20k-row table is split and partitioned like a big one
    sizing = SizingConfig(range_bytes=600_000, split_min_bytes=1_000_000, optimize_group_bytes=100_000,
                          whole_lake_max_bytes=0) if split else SizingConfig()
    return ExportSettings(output_bucket=str(tmp_path), sizing=sizing, max_streams=kw.pop("max_streams", 3),
                          max_row_group_size=1000, target_partition_size_mb=1,
                          small_table_threshold_mb=0 if split else 200, **kw)


def run(frozen, tmp_path, settings, job="job1", table=TABLE, executor=None, row_count=None, **request):
    def inputs(info, version, name):
        rows = frozen["engine"].execute(f'SELECT count(*) FROM "{name}"').scalar()
        return {"connection_string": frozen["conn_str"], "engine": frozen["engine"],
                "segmentation_table": SEG if name == TABLE else None,
                "row_count": rows if row_count is None else row_count}

    info = {"datastack": DATASTACK, "aligned_volume": {"name": ALIGNED}, "segmentation_source": "graphene://x/pcg"}
    executor = executor or ThreadExecutor(4)
    return run_tables(info, VERSION, [TableRequest(table, **request)], job_id_for=lambda t: job,
                      settings=settings, executor=executor, inputs=inputs)[table]


def base(tmp_path, table=TABLE):
    return f"{tmp_path}/{DATASTACK}/v{VERSION}/{table}"


def lakes(engine):
    with engine.connect() as conn:
        return {r["spec_name"]: dict(r) for r in conn.execute("SELECT * FROM deltalake_exports")}


class TestExportTable:
    def test_small_table_is_one_range_and_one_lake(self, frozen, catalog, statuses, tmp_path):
        from deltalake import DeltaTable

        summary = run(frozen, tmp_path, settings_for(tmp_path, split=False))
        assert summary["state"] == "complete", summary
        assert summary["ranges"] == 1 and summary["lakes"] == 1
        (row,) = lakes(catalog).values()
        assert row["status"] == "complete" and row["row_count"] == frozen["rows"]
        uri = row["uri"]
        assert DeltaTable(uri).to_pyarrow_table(columns=["id"]).num_rows == frozen["rows"]
        assert storage.list_paths(uri, "_cave_index")  # the planning index
        # once complete, the plan is replaced by the job's done marker
        assert storage.list_paths(base(tmp_path), planning.PLAN_DIR) == [planning.done_path("job1")]
        records, logs = statuses
        final = records[f"deltalake_export:{DATASTACK}:v{VERSION}:{TABLE}:job1"][-1]
        assert final["status"] == "complete" and final["percent_complete"] == 100.0
        assert [r["phase"] for r in records[f"deltalake_export:{DATASTACK}:v{VERSION}:{TABLE}:job1"]
                if r["phase"] != "streaming"][:1] == ["planning"]
        assert logs["job1"][-1] == "Export complete."

    def test_split_table_every_lake_complete(self, frozen, catalog, statuses, tmp_path):
        from deltalake import DeltaTable

        summary = run(frozen, tmp_path, settings_for(tmp_path))
        assert summary["state"] == "complete", summary
        assert summary["ranges"] > 3 and summary["lakes"] == 4
        rows = lakes(catalog)
        assert {r["status"] for r in rows.values()} == {"complete"}
        for name, row in rows.items():
            assert row["row_count"] == frozen["rows"]
            commits = [h for h in DeltaTable(row["uri"]).history() if h.get(pub.RANGE_KEY) is not None]
            assert len(commits) == summary["ranges"]
            # optimized: no staged file left, one file per partition
            assert not [p for p in storage.list_paths(row["uri"], "staging") if p.endswith(".parquet")]
            spec = json.loads(row["spec"]) if isinstance(row["spec"], str) else row["spec"]
            assert spec["bounds"] is not None and spec["n_partitions"] > 1

    def test_streams_stay_within_the_budget(self, frozen, catalog, statuses, tmp_path):
        running, peak, lock = [0], [0], threading.Lock()
        original = table_mod.run_range

        def counted(plan, index):
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
            try:
                return original(plan, index)
            finally:
                with lock:
                    running[0] -= 1

        executor = ThreadExecutor(8)
        submit = executor.submit
        executor.submit = lambda unit: submit(unit if unit.kind != "range" else
                                              table_mod.Unit(unit.table, unit.kind, unit.key, counted, unit.args))
        summary = run(frozen, tmp_path, settings_for(tmp_path, max_streams=2), executor=executor)
        assert summary["state"] == "complete" and peak[0] == 2

    def test_resume_after_a_failed_range(self, frozen, catalog, statuses, tmp_path):
        from deltalake import DeltaTable

        settings = settings_for(tmp_path, max_streams=1)
        original = table_mod.run_range
        calls = []

        def failing(plan, index):
            if index == 2:
                raise ConnectionError("worker lost")
            return original(plan, index)

        executor = ThreadExecutor(2)
        submit = executor.submit
        executor.submit = lambda unit: submit(unit if unit.kind != "range" else
                                              table_mod.Unit(unit.table, unit.kind, unit.key, failing, unit.args))
        first = run(frozen, tmp_path, settings, executor=executor)
        assert first["state"] == "failed" and "worker lost" in first["error"]
        assert {r["status"] for r in lakes(catalog).values()} == {"failed"}
        assert storage.list_paths(base(tmp_path), planning.PLAN_DIR)  # the plan stays for a restart

        def counting(plan, index):
            calls.append(index)
            return original(plan, index)

        executor = ThreadExecutor(2)
        submit2 = executor.submit
        executor.submit = lambda unit: submit2(unit if unit.kind != "range" else
                                               table_mod.Unit(unit.table, unit.kind, unit.key, counting, unit.args))
        second = run(frozen, tmp_path, settings, executor=executor)
        assert second["state"] == "complete", second
        assert 0 not in calls and 2 in calls and len(calls) < second["ranges"]
        for row in lakes(catalog).values():
            assert row["status"] == "complete" and row["row_count"] == frozen["rows"]
            ranges = [h[pub.RANGE_KEY] for h in DeltaTable(row["uri"]).history() if h.get(pub.RANGE_KEY)]
            assert sorted(ranges, key=int) == [str(i) for i in range(second["ranges"])]  # each range once

    def test_a_lake_another_export_wrote_is_refused(self, frozen, catalog, statuses, tmp_path):
        settings = settings_for(tmp_path, split=False)
        assert run(frozen, tmp_path, settings, job="first")["state"] == "complete"
        again = run(frozen, tmp_path, settings, job="second")
        assert again["state"] == "failed" and "name_suffix" in again["error"]
        assert lakes(catalog)["id"]["status"] == "complete"  # the first export's row is untouched
        assert planning.plan_path("second") not in storage.list_paths(base(tmp_path), planning.PLAN_DIR)
        beside = run(frozen, tmp_path, settings, job="third", spec_overrides={"name_suffix": "_b"})
        assert beside["state"] == "complete"
        assert set(lakes(catalog)) == {"id", "id_b"}

    def test_a_restart_skips_the_tables_it_completed(self, frozen, catalog, statuses, tmp_path):
        """KubeRay restarting the driver (e.g. after a failed views stage) must not plan a
        completed table again: new sampled bounds would not line up with its published ranges."""
        from deltalake import DeltaTable

        settings = settings_for(tmp_path)
        first = run(frozen, tmp_path, settings)
        assert first["state"] == "complete"
        versions = {name: DeltaTable(row["uri"]).version() for name, row in lakes(catalog).items()}

        submitted = []
        executor = ThreadExecutor(2)
        submit = executor.submit
        executor.submit = lambda unit: submitted.append(unit) or submit(unit)
        again = run(frozen, tmp_path, settings, executor=executor)
        assert again["state"] == "complete" and again["resumed"] and again["ranges"] == first["ranges"]
        assert submitted == []  # nothing streamed or optimized again
        assert {name: DeltaTable(row["uri"]).version() for name, row in lakes(catalog).items()} == versions
        records, logs = statuses
        assert records[f"deltalake_export:{DATASTACK}:v{VERSION}:{TABLE}:job1"][-1]["status"] == "complete"
        assert "skipped" in logs["job1"][-2]

    def test_row_count_mismatch_fails(self, frozen, catalog, statuses, tmp_path):
        summary = run(frozen, tmp_path, settings_for(tmp_path, split=False), row_count=frozen["rows"] + 1)
        assert summary["state"] == "failed" and "do not match" in summary["error"]
        assert {r["status"] for r in lakes(catalog).values()} == {"failed"}

    def test_empty_table(self, frozen, catalog, statuses, tmp_path):
        with frozen["engine"].begin() as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS empty_table (id bigint PRIMARY KEY, size int)")
        summary = run(frozen, tmp_path, settings_for(tmp_path, split=False), table="empty_table")
        assert summary["state"] == "complete", summary
        assert lakes(catalog) == {}

    def test_view_is_refused(self, frozen, catalog, statuses, tmp_path):
        with frozen["engine"].begin() as conn:
            conn.execute(f"CREATE OR REPLACE VIEW a_view AS SELECT id FROM {TABLE}")
        summary = run(frozen, tmp_path, settings_for(tmp_path, split=False), table="a_view")
        assert summary["state"] == "failed" and "views are not exported" in summary["error"]


class TestOnRay:
    def test_export_through_ray_tasks(self, frozen, catalog, statuses, tmp_path):
        ray = pytest.importorskip("ray")
        from materializationengine.rayjobs.entrypoints.lake_export import RayExecutor

        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True)
        try:
            summary = run(frozen, tmp_path, settings_for(tmp_path, max_streams=2), executor=RayExecutor(1))
        finally:
            ray.shutdown()
        assert summary["state"] == "complete", summary
        assert {r["status"] for r in lakes(catalog).values()} == {"complete"}
