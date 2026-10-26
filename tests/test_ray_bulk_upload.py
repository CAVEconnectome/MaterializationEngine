"""The Ray bulk upload driver: stages run once, resume after a crash, fail for good,
cancel, and keep to the lease. Runs against the test PostGIS server (as the store
tests) with InlineRunner and fake stage bodies; one test uses a local Ray cluster.
"""

from unittest import mock

import pytest
from flask import Flask

from materializationengine.blueprints.upload import tasks
from materializationengine.rayjobs import runner as runner_module
from materializationengine.rayjobs.entrypoints import bulk_upload
from materializationengine.upload_state import store
from materializationengine.upload_state.status_sink import StoreSink, use_sink
from tests.test_upload_state_store import JOB, admin, engine, new_job  # noqa: F401

PARAMS = {
    "file_path": "gs://b/f.csv",
    "file_metadata": {"metadata": {"table_name": "synapses_test", "schema_type": "synapse"}, "column_mapping": {}},
    "datastack_info": {"datastack": "minnie65_phase3_v1", "segmentation_source": "graphene://x/minnie3_v1"},
    "sql_instance_name": "instance",
    "created_ts": "2026-10-26T08:00:00+00:00",
    "materialization_time_stamp": "2026-10-26T08:00:00",
}


@pytest.fixture
def app():
    app = Flask(__name__)
    app.config.update(STAGING_DATABASE_NAME="staging")
    with app.app_context():
        yield app


@pytest.fixture
def job(engine, app):
    new_job(engine)
    store.set_job_fields(engine, JOB, params=PARAMS)
    return engine


@pytest.fixture
def discarded():
    calls = []

    def fake_discard(table_name, job_id, reason):
        calls.append(reason)
        return {"status": "success", "dropped": []}

    with mock.patch.object(tasks, "_discard_staging", side_effect=fake_discard):
        yield calls


class FakeStages:
    """Stage bodies that record their runs and can be told to fail."""

    def __init__(self):
        self.runs = []
        self.fail = {}  # stage -> exception, or list of them (one per run)

    def __call__(self, name):
        def stage(ctx):
            self.runs.append(name)
            failure = self.fail.get(name)
            if isinstance(failure, list):
                failure = failure.pop(0) if failure else None
            if failure is not None:
                raise failure
            tasks.update_job_status(ctx.job_id, {"status": "processing", "phase": name})
            if name == "cleanup":
                tasks.update_job_status(ctx.job_id, {"status": "done", "phase": "Transfer Complete"})
            return {"stage": name, "saw": sorted(ctx.results)}

        return stage


@pytest.fixture
def stages():
    fake = FakeStages()
    with mock.patch.dict(bulk_upload.STAGES, {name: fake(name) for name in bulk_upload.STAGE_ORDER}):
        yield fake


def run(engine, **kwargs):
    kwargs.setdefault("lease_wait_seconds", 0)
    return bulk_upload.run_job(engine, JOB, runner_module.InlineRunner, host="head", **kwargs)


def release(engine):
    """Make the lease look abandoned, as after the head pod died."""
    store.set_job_fields(engine, JOB, driver_heartbeat_at=None)


class TestDriver:
    def test_runs_every_stage_once(self, job, stages, discarded):
        assert run(job) == 0
        assert stages.runs == bulk_upload.STAGE_ORDER
        record = store.get_job_record(job, JOB)
        assert (record["status"], record["phase"]) == ("done", "Transfer Complete")
        assert record["user_id"] == "1174"  # merged into the record the API created
        done = store.get_stages(job, JOB)
        assert all(done[s]["status"] == "done" for s in bulk_upload.STAGE_ORDER)
        assert set(done["transfer"]["result"]["saw"]) == set(bulk_upload.STAGE_ORDER[:6])
        assert discarded == []
        # a resubmitted driver finds nothing to do
        assert run(job) == 0
        assert len(stages.runs) == len(bulk_upload.STAGE_ORDER)

    def test_a_crash_resumes_at_the_failed_stage(self, job, stages, discarded):
        stages.fail["spatial_chunks"] = [RuntimeError("worker node lost")]
        assert run(job) == 1
        job_row = store.get_job(job, JOB)
        assert job_row["status"] == "processing" and job_row["retryable"]
        assert "Retrying" in job_row["record"]["phase"]
        assert store.get_stages(job, JOB)["spatial_chunks"]["status"] == "failed"
        assert discarded == []  # staging is kept for the retry

        assert run(job) == 0
        assert stages.runs == bulk_upload.STAGE_ORDER[:4] + bulk_upload.STAGE_ORDER[3:]
        # the resumed driver has the earlier stages' results
        assert set(store.get_stages(job, JOB)["cluster"]["result"]["saw"]) == set(bulk_upload.STAGE_ORDER[:5])
        assert store.get_job(job, JOB)["driver_epoch"] == 2

    def test_a_permanent_failure_ends_the_job(self, job, stages, discarded):
        stages.fail["upload_to_db"] = ValueError("Table name is missing")
        assert run(job) == 1
        job_row = store.get_job(job, JOB)
        assert (job_row["status"], job_row["retryable"]) == ("error", False)
        assert "Table name is missing" in job_row["record"]["error"]
        assert job_row["finished_at"] is not None
        assert discarded == ["failed"]
        # a resubmitted driver does not try again
        assert run(job) == 1
        assert stages.runs == ["process_source", "upload_to_db"]

    def test_a_stage_that_keeps_failing_ends_the_job(self, job, stages, discarded):
        stages.fail["cluster"] = [RuntimeError("disk full")] * bulk_upload.MAX_STAGE_ATTEMPTS
        for _ in range(bulk_upload.MAX_STAGE_ATTEMPTS - 1):
            assert run(job) == 1
            assert store.get_job(job, JOB)["retryable"]
        assert run(job) == 1
        assert store.get_job(job, JOB)["status"] == "error"
        assert discarded == ["failed"]

    def test_a_cancel_stops_before_the_next_stage(self, job, stages, discarded):
        def cancel_during(ctx):
            stages.runs.append("process_source")
            store.request_cancel(ctx.engine, JOB, by="1174")
            ctx.cancel.set()  # what the heartbeat notices
            return {}

        with mock.patch.dict(bulk_upload.STAGES, {"process_source": cancel_during}):
            assert run(job) == 0
        assert stages.runs == ["process_source"]
        assert store.get_job(job, JOB)["status"] == "cancelled"
        assert discarded == ["cancelled"]

    def test_a_cancel_after_the_transfer_started_is_refused(self, job, stages, discarded):
        accepted = []

        def transfer(ctx):
            stages.runs.append("transfer")
            accepted.append(store.request_cancel(ctx.engine, JOB)["cancel_requested_at"])
            return {}

        with mock.patch.dict(bulk_upload.STAGES, {"transfer": transfer}):
            assert run(job) == 0
        assert accepted == [None]
        assert store.get_job(job, JOB)["status"] == "done"
        assert discarded == []

    def test_a_cancel_just_before_the_transfer_wins(self, job, stages, discarded):
        def cluster(ctx):
            stages.runs.append("cluster")
            store.request_cancel(ctx.engine, JOB)  # the heartbeat has not seen it yet
            return {}

        with mock.patch.dict(bulk_upload.STAGES, {"cluster": cluster}):
            assert run(job) == 0
        assert "transfer" not in stages.runs
        assert store.get_job(job, JOB)["status"] == "cancelled"

    def test_a_live_driver_keeps_the_job(self, job, stages):
        assert store.acquire_lease(job, JOB, "other-head") == 1
        assert run(job) == 1
        assert stages.runs == []

    def test_a_driver_that_lost_its_lease_stops_without_writing(self, job, stages, discarded):
        def taken_over(ctx):
            stages.runs.append("spatial_init")
            release(ctx.engine)
            store.acquire_lease(ctx.engine, JOB, "new-head")  # epoch 2
            tasks.update_job_status(ctx.job_id, {"phase": "from the old driver"})
            return {}

        with mock.patch.dict(bulk_upload.STAGES, {"spatial_init": taken_over}):
            assert run(job) == 1
        job_row = store.get_job(job, JOB)
        assert job_row["record"]["phase"] != "from the old driver"
        assert store.get_stages(job, JOB)["spatial_init"]["status"] == "running"
        assert job_row["status"] == "processing"
        assert discarded == []


def part_signatures(n):
    return [
        tasks.process_csv_part.si(
            bucket_name="b", source_blob_name="f.csv", start=i * 10, end=(i + 1) * 10,
            header="h\n", destination_blob_name=f"processed_parts/{JOB}/part-{i:05d}.csv",
            schema_type="synapse", column_mapping={}, timestamp=PARAMS["created_ts"],
            id_counter_start=i * 100, job_id_for_status=JOB,
        )
        for i in range(n)
    ]


def context(engine, runner, epoch, **kwargs):
    return bulk_upload.Context(engine=engine, job_id=JOB, epoch=epoch, params=PARAMS, runner=runner, **kwargs)


class TestPartRunner:
    def test_only_unfinished_parts_run_again(self, job, app):
        seen = []

        def flaky(kwargs):
            seen.append(kwargs["start"])
            assert kwargs["job_id_for_status"] == JOB  # the worker task drops it
            if kwargs["start"] == 20:
                raise RuntimeError("worker lost")
            return {"rows_read": 10, "rows_written": 10, "dropped_rows": 1, "lines": 10}

        epoch = store.acquire_lease(job, JOB, "head")
        ctx = context(job, runner_module.InlineRunner({"csv_part": flaky}), epoch, max_in_flight=1)
        with use_sink(StoreSink(job, JOB, epoch=epoch)), pytest.raises(RuntimeError):
            bulk_upload.make_part_runner(ctx)(part_signatures(4), JOB, "Processing CSV Data")
        assert seen == [0, 10, 20]
        assert [p["status"] for p in store.get_parts(job, JOB)] == ["done", "done", "pending", "pending"]

        seen.clear()
        ok = lambda kwargs: seen.append(kwargs["start"]) or {  # noqa: E731
            "rows_read": 10, "rows_written": 9, "dropped_rows": 0,
        }
        ctx.runner = runner_module.InlineRunner({"csv_part": ok})
        with use_sink(StoreSink(job, JOB, epoch=epoch)):
            results = bulk_upload.make_part_runner(ctx)(part_signatures(4), JOB, "Processing CSV Data")
        assert seen == [20, 30]
        assert [r["rows_read"] for r in results] == [10, 10, 10, 10]
        assert [r["dropped_rows"] for r in results] == [1, 1, 0, 0]
        assert store.get_job_record(job, JOB)["current_chunk_num"] == 4

    def test_a_different_plan_is_refused(self, job, app):
        epoch = store.acquire_lease(job, JOB, "head")
        ok = runner_module.InlineRunner({"csv_part": lambda kw: {"rows_read": 1, "rows_written": 1, "dropped_rows": 0}})
        ctx = context(job, ok, epoch)
        with use_sink(StoreSink(job, JOB, epoch=epoch)):
            bulk_upload.make_part_runner(ctx)(part_signatures(2), JOB, "p")
            changed = part_signatures(2)
            changed[1].kwargs["end"] = 999
            with pytest.raises(bulk_upload.PermanentUploadError):
                bulk_upload.make_part_runner(ctx)(changed, JOB, "p")

    def test_counting_parts_are_not_recorded(self, job, app):
        epoch = store.acquire_lease(job, JOB, "head")
        counts = runner_module.InlineRunner({"count_csv_part": lambda b, f, s, e: e - s})
        ctx = context(job, counts, epoch)
        sigs = [tasks.count_csv_part.si("b", "f.csv", s, s + 7) for s in (0, 7)]
        with use_sink(StoreSink(job, JOB, epoch=epoch)):
            assert bulk_upload.make_part_runner(ctx)(sigs, JOB, "counting") == [7, 7]
        assert store.get_parts(job, JOB) == []


def init_spatial(engine, total):
    store.init_spatial(
        engine, JOB, database_name="staging", workflow_name="synapses_test",
        datastack_name="minnie65_phase3_v1", status="processing_chunks", total_chunks=total,
        chunking_parameters={"strategy_name": "grid"}, mat_metadata={"segmentation_source": "x"},
        total_point_estimate=2 * total,
    )


@pytest.fixture
def bounds():
    with mock.patch(
        "materializationengine.workflows.chunking.reconstruct_chunk_bounds",
        side_effect=lambda i, params: ([i, 0, 0], [i + 1, 1, 1]),
    ):
        yield


class TestSpatialChunks:
    def test_retries_then_resumes_without_redoing_chunks(self, job, app, bounds):
        init_spatial(job, 8)
        epoch = store.acquire_lease(job, JOB, "head")
        failures = {5: 1}
        done = []

        def chunk(database, info, min_corner, max_corner, sub_batch):
            i = min_corner[0]
            if failures.get(i):
                failures[i] -= 1
                raise ConnectionError(f"bigtable blip on {i}")
            done.append(i)
            return {"points": 2, "rows": 1, "sub_batches": 1}

        ctx = context(job, runner_module.InlineRunner({"spatial_chunk": chunk}), epoch, max_in_flight=2)
        real_on_tick = runner_module.run_bounded

        def stop_after_four(*args, **kwargs):
            kwargs["should_stop"] = lambda: len(done) >= 4
            return real_on_tick(*args, **kwargs)

        with use_sink(StoreSink(job, JOB, epoch=epoch)), \
                mock.patch.object(runner_module, "run_bounded", side_effect=stop_after_four), \
                pytest.raises(runner_module.Stopped):
            bulk_upload.stage_spatial_chunks(ctx)
        first = set(done)
        completed, _, _ = store.chunk_state(job, JOB)
        assert completed == first and len(first) == 4  # flushed though stopped

        done.clear()
        with use_sink(StoreSink(job, JOB, epoch=epoch)):
            result = bulk_upload.stage_spatial_chunks(ctx)
        assert set(done) == set(range(8)) - first
        assert result == {"completed_chunks": 8, "failed_chunks": 0, "rows_processed": 16}
        rows = {r["chunk_index"]: r for r in store.chunk_rows(job, JOB)}
        assert rows[5]["attempt_count"] == 2 and rows[5]["status"] == "COMPLETED"
        spatial = store.get_spatial(job, JOB)
        assert (spatial["completed_chunks"], spatial["rows_processed"]) == (8, 16)
        assert spatial["processing_rate"] or spatial["rate_samples"]
        assert store.get_job_record(job, JOB)["phase"] == "Spatial Lookup: processing_chunks (8/8 chunks)"

    def test_a_chunk_that_keeps_failing_fails_the_job(self, job, app, bounds):
        init_spatial(job, 3)
        epoch = store.acquire_lease(job, JOB, "head")

        def chunk(database, info, min_corner, max_corner, sub_batch):
            if min_corner[0] == 1:
                raise ValueError("bad points")
            return {"points": 2, "rows": 1}

        ctx = context(job, runner_module.InlineRunner({"spatial_chunk": chunk}), epoch)
        with use_sink(StoreSink(job, JOB, epoch=epoch)), pytest.raises(bulk_upload.PermanentUploadError):
            bulk_upload.stage_spatial_chunks(ctx)
        rows = {r["chunk_index"]: r for r in store.chunk_rows(job, JOB)}
        assert rows[1]["status"] == "FAILED_PERMANENT"
        assert rows[1]["attempt_count"] == bulk_upload.MAX_CHUNK_ATTEMPTS
        assert rows[1]["error_type"] == "ValueError"
        assert store.get_spatial(job, JOB)["status"] == "failed"


class TestRunBounded:
    def test_never_more_than_max_in_flight(self):
        class Slow(runner_module.InlineRunner):
            def __init__(self):
                super().__init__({"t": lambda x: x})
                self.live, self.peak = 0, 0

            def submit(self, name, *args):
                self.live += 1
                self.peak = max(self.peak, self.live)
                return super().submit(name, *args)

            def wait(self, handles, timeout):
                self.live -= 1
                return handles[:1]

        slow = Slow()
        out = {}
        runner_module.run_bounded(slow, "t", ((i, (i,)) for i in range(20)), 3, out.__setitem__)
        assert out == {i: i for i in range(20)} and slow.peak == 3


def _ray_task(name, x):
    if name == "boom":
        raise KeyError(x)
    return x * 2


class TestRayRunner:
    def test_runs_on_a_local_ray_cluster(self):
        ray = pytest.importorskip("ray")
        ray.init(num_cpus=2, include_dashboard=False, ignore_reinit_error=True, log_to_driver=False)
        try:
            ray_runner = runner_module.RayRunner(fn=_ray_task)
            out = {}
            runner_module.run_bounded(ray_runner, "ok", ((i, (i,)) for i in range(6)), 2, out.__setitem__)
            assert out == {i: 2 * i for i in range(6)}
            errors = []
            runner_module.run_bounded(
                ray_runner, "boom", [(0, (7,))], 1, out.__setitem__,
                on_error=lambda key, e: errors.append(e) and False,
            )
            assert isinstance(errors[0], KeyError)  # the worker's own exception
        finally:
            ray.shutdown()
