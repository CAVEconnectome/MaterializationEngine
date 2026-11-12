"""lake_export.version: a whole version's tables in one job, then the views stage -- on
the throwaway frozen-shaped database of test_lake_export_ranges, units on threads."""

import pytest

from materializationengine.workflows.lake_export import version as version_mod
from materializationengine.workflows.lake_export.job import TableRequest
from materializationengine.workflows.lake_export.scheduler import ThreadExecutor
from materializationengine.workflows.lake_export.version import StageResult, VersionStatus, run_version
from tests.test_lake_export_ranges import SEG, TABLE, frozen  # noqa: F401
from tests.test_lake_export_table import ALIGNED, DATASTACK, VERSION, catalog, settings_for, statuses  # noqa: F401

CELLS = "cells"


@pytest.fixture
def cells(frozen):
    """A second, small table of the version, with no segmentation table."""
    with frozen["engine"].begin() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {CELLS}")
        conn.execute(f"CREATE TABLE {CELLS} (id bigint PRIMARY KEY, cell_type text, volume float)")
        conn.execute(f"INSERT INTO {CELLS} SELECT g, 'type_' || mod(g, 5), g * 1.5 FROM generate_series(1, 500) g")
    yield CELLS
    with frozen["engine"].begin() as conn:
        conn.execute(f"DROP TABLE IF EXISTS {CELLS}")


class Recorder:
    def __init__(self):
        self.updates = []

    def __call__(self, datastack, version, job_id, **fields):
        self.updates.append(fields)

    def last(self, key):
        return next(u[key] for u in reversed(self.updates) if key in u)


def export_version(frozen, tmp_path, tables, **kw):
    def inputs(info, version, name):
        rows = frozen["engine"].execute(f'SELECT count(*) FROM "{name}"').scalar()
        return {"connection_string": frozen["conn_str"], "engine": frozen["engine"],
                "segmentation_table": SEG if name == TABLE else None, "row_count": rows}

    recorder = Recorder()
    info = {"datastack": DATASTACK, "aligned_volume": {"name": ALIGNED}, "segmentation_source": "graphene://x/pcg"}
    settings = kw.pop("settings", None) or settings_for(tmp_path)
    result = run_version(info, VERSION, "vjob", [TableRequest(t) for t in tables], settings,
                         kw.pop("executor", None) or ThreadExecutor(4), inputs=inputs,
                         status=VersionStatus(DATASTACK, VERSION, "vjob", write=recorder), **kw)
    return result, recorder


class TestVersion:
    def test_tables_then_views_stage(self, frozen, cells, catalog, statuses, tmp_path):
        seen = []

        def stage(ctx):
            seen.append(ctx)
            return {"state": "done", "detail": {"views": 0}}

        result, status = export_version(frozen, tmp_path, [TABLE, CELLS], views=True, views_stage=stage)
        assert result["state"] == "done", result
        assert {t: s["state"] for t, s in result["tables"].items()} == {TABLE: "complete", CELLS: "complete"}
        (ctx,) = seen
        assert ctx.version == VERSION and ctx.job_id == "vjob" and ctx.aligned_volume == ALIGNED
        assert {lake["table_name"] for lake in ctx.lakes} == {TABLE, CELLS}
        assert {lake["status"] for lake in ctx.lakes} == {"complete"}
        assert result["views"] == {"state": "done", "error": None, "detail": {"views": 0}}

        assert status.last("state") == "done"
        assert status.last("stages")["tables"] == {"state": "done", "failed": []}
        assert status.last("stages")["views"]["state"] == "done"
        assert {t: s["state"] for t, s in status.last("tables").items()} == {TABLE: "done", CELLS: "done"}
        # a table was reported running along the way (get_version_status folds its progress in)
        assert any(s["state"] == "running" for u in status.updates if "tables" in u for s in u["tables"].values())

    def test_views_not_asked_for_are_skipped(self, frozen, cells, catalog, statuses, tmp_path):
        called = []
        result, status = export_version(frozen, tmp_path, [CELLS], views=False, views_stage=called.append)
        assert result["state"] == "done" and result["views"]["state"] == "skipped" and not called
        assert status.updates[0]["stages"]["views"] == {"state": "skipped"}

    def test_no_stage_configured(self, frozen, cells, catalog, statuses, tmp_path):
        result, _ = export_version(frozen, tmp_path, [CELLS], views=True, views_stage=None)
        assert result["state"] == "done" and result["views"]["state"] == "not_implemented"

    def test_a_failed_table_skips_the_views(self, frozen, cells, catalog, statuses, tmp_path):
        called = []
        result, status = export_version(frozen, tmp_path, [CELLS, "no_such_table"], views=True,
                                        views_stage=called.append)
        assert result["state"] == "failed" and not called
        assert result["tables"][CELLS]["state"] == "complete"
        assert result["tables"]["no_such_table"]["state"] == "failed"
        assert result["views"]["state"] == "skipped" and "no_such_table" in result["views"]["error"]
        assert status.last("failed_tables") == ["no_such_table"]

    def test_a_failing_stage_fails_the_job(self, frozen, cells, catalog, statuses, tmp_path):
        def stage(ctx):
            raise RuntimeError("daft fell over")

        result, status = export_version(frozen, tmp_path, [CELLS], views=True, views_stage=stage)
        assert result["state"] == "failed" and status.last("state") == "failed"
        assert result["tables"][CELLS]["state"] == "complete"
        assert result["views"]["state"] == "failed" and "daft fell over" in result["views"]["error"]

    def test_tables_share_one_stream_budget(self, frozen, cells, catalog, statuses, tmp_path):
        import threading

        from materializationengine.workflows.lake_export import table as table_mod

        running, peak, tables, lock = [0], [0], set(), threading.Lock()
        original = table_mod.run_range

        def counted(plan, index):
            with lock:
                running[0] += 1
                peak[0] = max(peak[0], running[0])
                tables.add(plan.table)
            try:
                return original(plan, index)
            finally:
                with lock:
                    running[0] -= 1

        executor = ThreadExecutor(8)
        submit = executor.submit
        executor.submit = lambda unit: submit(unit if unit.kind != "range" else
                                              table_mod.Unit(unit.table, unit.kind, unit.key, counted, unit.args))
        result, _ = export_version(frozen, tmp_path, [TABLE, CELLS], executor=executor,
                                   settings=settings_for(tmp_path, max_streams=2))
        assert result["state"] == "done" and peak[0] == 2 and tables == {TABLE, CELLS}


class TestStageHook:
    def test_resolve_stage(self):
        assert version_mod.resolve_stage(None) is None
        assert version_mod.resolve_stage("json:dumps").__name__ == "dumps"
        with pytest.raises(ValueError, match="module:function"):
            version_mod.resolve_stage("json.dumps")

    def test_stage_results(self):
        ctx = object()
        assert version_mod.run_views_stage(ctx, None).state == "not_implemented"
        assert version_mod.run_views_stage(ctx, lambda c: None).state == "done"
        assert version_mod.run_views_stage(ctx, lambda c: StageResult("done", detail={"n": 1})).detail == {"n": 1}


class TestEntrypointArguments:
    @pytest.mark.parametrize("argv", [
        ["--datastack", "ds", "--version", "1", "--job-id", "j"],  # no tables, not a version job
        ["--datastack", "ds", "--version", "1", "--job-id", "j", "--tables", "t", "--views"],
        ["--datastack", "ds", "--version", "1", "--job-id", "j", "--tables", "a", "b", "--output-specs", "[]"],
        ["--datastack", "ds", "--version", "1", "--job-id", "j", "--version-job", "--output-specs", "[]"],
    ])
    def test_refused(self, argv):
        from materializationengine.rayjobs.entrypoints import lake_export

        with pytest.raises(SystemExit):
            lake_export.main(argv)
