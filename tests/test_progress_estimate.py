"""The spatial lookup's estimated completion follows points, not rows, at the current rate."""

import datetime
from unittest import mock

import pytest

from materializationengine.blueprints.upload import checkpoint_manager as cm
from materializationengine.workflows import spatial_lookup

START = datetime.datetime(2026, 10, 6, 22, 42, tzinfo=datetime.timezone.utc)
ROWS = 1_020_000
POINTS = 2 * ROWS  # synapse table: pre and post points


def _at(minutes):
    return (START + datetime.timedelta(minutes=minutes)).isoformat()


def _workflow(**overrides):
    data = cm.WorkflowData(table_name="t", task_id="x", start_time=START.isoformat(),
                           total_row_estimate=ROWS, total_point_estimate=POINTS)
    for k, v in overrides.items():
        setattr(data, k, v)
    return data


def _run(minutes, points_per_minute, sample_every_s=20, workflow=None):
    """Feed completions at a steady rate; return the last estimate and the workflow."""
    workflow = workflow or _workflow()
    result = {}
    for step in range(int(minutes * 60 / sample_every_s) + 1):
        t = step * sample_every_s / 60
        result = cm._progress_estimate(workflow, int(points_per_minute * t), _at(t))
        workflow.rate_samples = result["rate_samples"]
    return result, workflow


class TestProgressEstimate:
    def test_sixty_percent_done_is_not_done_now(self):
        # final_test5 read "done now" from the halfway point: rows_processed (points)
        # had passed total_row_estimate (rows)
        rate = POINTS / 16  # points per minute, finishing in 16 minutes
        result, _ = _run(minutes=0.6 * 16, points_per_minute=rate)
        eta = datetime.datetime.fromisoformat(result["estimated_completion"])
        assert eta > START + datetime.timedelta(minutes=0.6 * 16 + 5)

    def test_steady_rate_predicts_the_finish(self):
        rate = POINTS / 16
        result, _ = _run(minutes=4, points_per_minute=rate)
        eta = datetime.datetime.fromisoformat(result["estimated_completion"])
        assert abs((eta - (START + datetime.timedelta(minutes=16))).total_seconds()) < 30

    def test_rate_follows_recent_throughput_not_the_slow_start(self):
        # 10 minutes of scale-up at a tenth of the rate, then full speed for 6
        slow, fast = POINTS / 400, POINTS / 40
        _, workflow = _run(minutes=10, points_per_minute=slow)
        done_at_10 = int(slow * 10)
        result = {}
        for step in range(1, 6 * 3 + 1):  # every 20s for 6 minutes
            t = 10 + step / 3
            result = cm._progress_estimate(workflow, done_at_10 + int(fast * (t - 10)), _at(t))
            workflow.rate_samples = result["rate_samples"]
        assert result["processing_rate"] == f"{fast:.2f} rows/minute"

    def test_samples_are_bounded_to_the_window(self):
        _, workflow = _run(minutes=30, points_per_minute=1000, sample_every_s=5)
        newest = datetime.datetime.fromisoformat(workflow.rate_samples[-1][0])
        oldest = datetime.datetime.fromisoformat(workflow.rate_samples[0][0])
        assert (newest - oldest).total_seconds() <= cm.RATE_WINDOW_SECONDS
        assert len(workflow.rate_samples) <= cm.RATE_WINDOW_SECONDS / cm.RATE_SAMPLE_SPACING_SECONDS + 1

    def test_all_points_done_is_now(self):
        result = cm._progress_estimate(_workflow(), POINTS, _at(16))
        assert result["estimated_completion"] == _at(16)

    def test_no_point_estimate_gives_no_eta(self):
        result = cm._progress_estimate(_workflow(total_point_estimate=None), 1000, _at(5))
        assert "estimated_completion" not in result


class TestTotalPointEstimate:
    def test_rows_times_looked_up_point_columns(self):
        with mock.patch.object(spatial_lookup, "spatial_lookup_point_columns",
                               return_value=(None, ["pre_pt_position", "post_pt_position"])):
            assert spatial_lookup._total_point_estimate(ROWS, {"database": "staging"}) == POINTS

    @pytest.mark.parametrize("rows", [0, None])
    def test_unknown_rows(self, rows):
        assert spatial_lookup._total_point_estimate(rows, {}) is None
