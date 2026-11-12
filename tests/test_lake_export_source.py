"""lake_export.source: where a table export reads from, and how the plan records it (pure)."""

import json

import pytest

from materializationengine.workflows.lake_export.plan import TablePlan
from materializationengine.workflows.lake_export.source import ExportSource, FrozenSource, source_from_dict


def a_plan(**overrides):
    fields = dict(datastack="ds", version=7, table="t", job_id="j", connection_string="postgresql://x/ds__mat7",
                  output_uri_base="/tmp/ds/v7/t", annotation_table="t", segmentation_table="t__pcg",
                  columns=["id"], specs=[{"name": "id"}], geometry_columns=[], row_count=10, bytes_per_row=100,
                  sizing={"estimated_bytes": 1000, "ranges": 1, "flush_bytes": 64}, ranges=[(0, 11)])
    fields.update(overrides)
    return TablePlan(**fields)


class TestSource:
    def test_frozen_reads_everything(self):
        source = FrozenSource()
        assert source.kind == "frozen" and source.join == "left" and source.as_of is None
        assert source.row_filter("t") is None
        assert source.database("ds", 7) == "ds__mat7"

    def test_round_trip(self):
        assert source_from_dict(FrozenSource().to_dict()) == FrozenSource()
        assert source_from_dict(None) == FrozenSource()  # a plan saved before sources existed

    def test_unknown_or_unbuilt_kinds_are_refused(self):
        with pytest.raises(NotImplementedError, match="live"):
            source_from_dict({"kind": "live", "as_of": "2026-11-12T00:00:00", "join": "inner"})
        with pytest.raises(ValueError, match="join"):
            ExportSource(join="outer")


class TestPlanRecordsItsSource:
    def test_default_is_frozen(self):
        plan = a_plan()
        assert plan.export_source == FrozenSource() and plan.row_filter is None
        assert plan.table_source.segmentation_table == "t__pcg"

    def test_saved_and_loaded(self):
        plan = TablePlan.from_json(a_plan().to_json())
        assert plan.source == FrozenSource().to_dict() and plan.ranges == [(0, 11)]

    def test_a_plan_saved_before_sources_loads_as_frozen(self):
        old = json.loads(a_plan().to_json())
        del old["source"]
        plan = TablePlan.from_json(json.dumps(old))
        assert plan.export_source == FrozenSource()
