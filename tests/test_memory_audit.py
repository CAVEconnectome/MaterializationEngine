"""Tests for materializationengine.memory_audit.

The production path is Linux-only (it reads /proc/self/status), so the parsing is exercised
against a fixture file rather than the host's real /proc -- otherwise the only code path CI on
macOS would cover is the graceful-degradation branch.
"""

import json
import logging

import pytest
from flask import Flask

from materializationengine import memory_audit
from materializationengine.memory_audit import (
    _mem_kb,
    _summarize_filters,
    init_memory_audit,
)

PROC_STATUS = """\
Name:\tuwsgi
Umask:\t0022
State:\tS
Tgid:\t42
VmPeak:\t  1234567 kB
VmSize:\t  1200000 kB
VmLck:\t        0 kB
VmHWM:\t   414720 kB
VmRSS:\t   261120 kB
RssAnon:\t  200000 kB
Threads:\t2
"""


@pytest.fixture
def captured():
    """Collect the JSON payloads the audit logger emits."""
    records = []

    class Handler(logging.Handler):
        def emit(self, record):
            records.append(json.loads(record.getMessage()))

    logger = logging.getLogger("materializationengine.memory_audit")
    handler = Handler()
    logger.addHandler(handler)
    prev_level, prev_prop = logger.level, logger.propagate
    logger.setLevel(logging.INFO)
    logger.propagate = False
    yield records
    logger.removeHandler(handler)
    logger.setLevel(prev_level)
    logger.propagate = prev_prop


def make_app(**config):
    app = Flask(__name__)
    app.config.update({"MEMORY_AUDIT_ENABLED": True, **config})
    init_memory_audit(app)

    @app.route("/materialize/api/v3/datastack/d/query", methods=["POST"])
    def query():
        return {"ok": 1}

    @app.route("/health")
    def health():
        return {"ok": 1}

    @app.route("/boom")
    def boom():
        raise ValueError("kaboom")

    return app


class TestMemReading:
    def test_parses_proc_status(self, tmp_path, monkeypatch):
        status = tmp_path / "status"
        status.write_text(PROC_STATUS)
        monkeypatch.setattr(memory_audit, "_STATUS", str(status))
        rss, hwm = _mem_kb()
        assert rss == 261120
        # VmHWM, not VmPeak: VmPeak is virtual address space, which for numpy/pandas runs far
        # above resident memory and would not correlate with what the kubelet evicts on.
        assert hwm == 414720

    def test_missing_proc_degrades_quietly(self, tmp_path, monkeypatch):
        monkeypatch.setattr(memory_audit, "_STATUS", str(tmp_path / "nope"))
        assert _mem_kb() == (None, None)

    def test_hooks_survive_unreadable_proc(self, tmp_path, monkeypatch, captured):
        """A request must never fail because memory accounting could not read /proc."""
        monkeypatch.setattr(memory_audit, "_STATUS", str(tmp_path / "nope"))
        app = make_app()
        resp = app.test_client().post(
            "/materialize/api/v3/datastack/d/query", json={"table": "t"}
        )
        assert resp.status_code == 200
        assert [r["phase"] for r in captured] == ["start", "end"]
        assert captured[0]["rss_mb"] is None


class TestFilterSummary:
    def test_records_shapes_never_values(self):
        rid = 864691136928006474
        body = {
            "table": "synapses_pni_2",
            "limit": 200000,
            "filter_in_dict": {"synapses_pni_2": {"pre_pt_root_id": [rid] * 50000}},
            "joins": [["a", "b"], ["c", "d"]],
            "select_columns": ["x", "y", "z"],
        }
        summary = _summarize_filters(body)
        # The whole point: a filter can hold hundreds of thousands of root ids, so logging
        # values would reproduce the memory problem being diagnosed.
        assert str(rid) not in json.dumps(summary)
        assert summary["filter_in_dict"]["synapses_pni_2"]["pre_pt_root_id"] == 50000
        assert summary["table"] == "synapses_pni_2"
        assert summary["limit"] == 200000
        assert summary["n_joins"] == 2
        assert summary["n_select_columns"] == 3

    def test_tolerates_unexpected_shapes(self):
        for body in (
            {"filter_in_dict": "not-a-dict"},
            {"filter_in_dict": {"t": "not-a-dict-either"}},
            {"joins": None},
            {},
        ):
            assert isinstance(_summarize_filters(body), dict)


class TestRequestHooks:
    def test_start_and_end_are_paired(self, captured):
        app = make_app()
        app.test_client().post(
            "/materialize/api/v3/datastack/d/query", json={"table": "synapses_pni_2"}
        )
        assert [r["phase"] for r in captured] == ["start", "end"]
        start, end = captured
        # Pairing by rid is what makes a killed request detectable: a start with no end.
        assert start["rid"] == end["rid"]
        assert start["table"] == "synapses_pni_2"
        assert start["method"] == "POST"
        assert "dur_s" in end

    def test_end_record_survives_an_exception(self, captured):
        app = make_app()
        # Flask turns the unhandled exception into a 500 rather than re-raising it here; what
        # matters is that teardown_request still ran and recorded which exception ended the
        # request, so a failing query is distinguishable from one that was killed outright.
        assert app.test_client().get("/boom").status_code == 500
        assert [r["phase"] for r in captured] == ["start", "end"]
        assert captured[1]["exc"] == "ValueError"

    def test_fast_health_logs_start_only(self, captured):
        """A healthy probe is noise; a slow or hungry one is the leading failure indicator."""
        app = make_app()
        app.test_client().get("/health")
        assert [r["phase"] for r in captured] == ["start"]

    def test_slow_health_still_reports(self, tmp_path, monkeypatch, captured):
        status = tmp_path / "status"
        status.write_text(PROC_STATUS)
        monkeypatch.setattr(memory_audit, "_STATUS", str(status))
        app = make_app()
        clock = iter([100.0, 130.0])
        monkeypatch.setattr(memory_audit.time, "monotonic", lambda: next(clock))
        app.test_client().get("/health")
        assert [r["phase"] for r in captured] == ["start", "end"]
        assert captured[1]["dur_s"] == 30.0

    def test_large_body_is_not_introspected(self, captured):
        app = make_app(MEMORY_AUDIT_MAX_BODY_BYTES=10)
        app.test_client().post(
            "/materialize/api/v3/datastack/d/query", json={"table": "synapses_pni_2"}
        )
        start = captured[0]
        # Parsing caches raw bytes plus the parsed object, so the biggest payloads -- the ones
        # closest to the eviction threshold -- are deliberately left unparsed.
        assert start["body_introspected"] is False
        assert "table" not in start
        assert start["body_bytes"] > 10

    def test_disabled_registers_no_hooks(self, captured):
        app = Flask(__name__)
        app.config["MEMORY_AUDIT_ENABLED"] = False
        init_memory_audit(app)

        @app.route("/x")
        def x():
            return {}

        app.test_client().get("/x")
        assert captured == []

    def test_high_delta_escalates_to_warning(self, tmp_path, monkeypatch):
        """A memory-hungry request must stand out at WARNING, not hide in the INFO stream."""
        status = tmp_path / "status"
        status.write_text(PROC_STATUS)
        monkeypatch.setattr(memory_audit, "_STATUS", str(status))

        levels = []

        class Handler(logging.Handler):
            def emit(self, record):
                levels.append((record.levelno, json.loads(record.getMessage())["phase"]))

        logger = logging.getLogger("materializationengine.memory_audit")
        handler = Handler()
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        try:
            app = make_app(MEMORY_AUDIT_WARN_DELTA_MB=50)
            # Grow RSS by 200Mi between the two /proc reads.
            reads = iter([(261120, 414720), (261120 + 200 * 1024, 414720)])
            monkeypatch.setattr(memory_audit, "_mem_kb", lambda: next(reads))
            app.test_client().post(
                "/materialize/api/v3/datastack/d/query", json={"table": "t"}
            )
        finally:
            logger.removeHandler(handler)

        end_level = [lvl for lvl, phase in levels if phase == "end"][0]
        assert end_level == logging.WARNING
