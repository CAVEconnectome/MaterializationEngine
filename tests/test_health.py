"""Tests for the liveness/readiness split.

The invariant that matters most: a BUSY pod must be drained but never restarted, and a pod whose
database pool is broken must be restarted. Conflating those is what caused 41 liveness restarts
of merely-busy pods on minniev7 on 2026-08-18.
"""

import pytest
from flask import Flask

from materializationengine import health
from materializationengine.health import health_bp


@pytest.fixture
def app(monkeypatch):
    monkeypatch.setattr(health, "_check_db", lambda: True)
    health.reset_state_for_tests()
    app = Flask(__name__)
    app.config.update(
        HEALTH_READY_MAX_BUSY_FRACTION=0.9,
        HEALTH_LIVE_DB_FAILURES=3,
        HEALTH_LIVE_DB_STALE_SECONDS=120,
    )
    app.register_blueprint(health_bp)
    yield app
    health.reset_state_for_tests()


def set_workers(monkeypatch, busy, total):
    monkeypatch.setattr(health, "_worker_saturation", lambda: (busy, total))


class TestReadiness:
    def test_ready_when_healthy_and_idle(self, app, monkeypatch):
        set_workers(monkeypatch, 0, 8)
        r = app.test_client().get("/health/ready")
        assert r.status_code == 200
        assert r.get_json()["database"] == "ok"

    def test_drains_when_saturated(self, app, monkeypatch):
        set_workers(monkeypatch, 8, 8)
        r = app.test_client().get("/health/ready")
        assert r.status_code == 503
        assert "saturated" in r.get_json()["reason"]

    def test_does_not_drain_while_cheaper_can_still_scale_up(self, app, monkeypatch):
        """2 of 2 spawned workers busy is only 25% of 8 slots -- uwsgi will spawn more.

        Using spawned workers as the denominator would drain a pod nowhere near its ceiling.
        """
        set_workers(monkeypatch, 2, 8)
        assert app.test_client().get("/health/ready").status_code == 200

    def test_drains_when_database_unreachable(self, app, monkeypatch):
        set_workers(monkeypatch, 0, 8)
        monkeypatch.setattr(health, "_check_db", lambda: False)
        r = app.test_client().get("/health/ready")
        assert r.status_code == 503
        assert r.get_json()["reason"] == "database unreachable"

    def test_saturation_draining_can_be_disabled(self, app, monkeypatch):
        app.config["HEALTH_READY_MAX_BUSY_FRACTION"] = 0
        set_workers(monkeypatch, 8, 8)
        assert app.test_client().get("/health/ready").status_code == 200

    def test_works_outside_uwsgi(self, app, monkeypatch):
        """Off uwsgi there are no worker stats; readiness degrades to a database check."""
        monkeypatch.setattr(health, "_worker_saturation", lambda: None)
        assert app.test_client().get("/health/ready").status_code == 200


class TestLiveness:
    def test_alive_when_healthy(self, app):
        assert app.test_client().get("/health/live").status_code == 200

    def test_alive_when_saturated(self, app, monkeypatch):
        """THE key invariant: a fully busy pod is drained, not restarted."""
        set_workers(monkeypatch, 8, 8)
        c = app.test_client()
        assert c.get("/health/ready").status_code == 503
        assert c.get("/health/live").status_code == 200

    def test_alive_when_idle_and_never_checked(self, app):
        """A worker that has run no database check must not be reported dead."""
        health.reset_state_for_tests()
        assert app.test_client().get("/health/live").status_code == 200

    def test_alive_after_a_brief_database_blip(self, app, monkeypatch):
        set_workers(monkeypatch, 0, 8)
        monkeypatch.setattr(health, "_check_db", lambda: False)
        c = app.test_client()
        for _ in range(2):  # below HEALTH_LIVE_DB_FAILURES
            c.get("/health/ready")
        assert c.get("/health/live").status_code == 200

    def test_dead_after_sustained_database_failure(self, app, monkeypatch):
        """The corrupted-pool signature: repeated failures AND no recent success."""
        set_workers(monkeypatch, 0, 8)
        monkeypatch.setattr(health, "_check_db", lambda: False)
        c = app.test_client()
        for _ in range(4):
            c.get("/health/ready")
        r = c.get("/health/live")
        assert r.status_code == 503
        assert "restart required" in r.get_json()["reason"]

    def test_recovery_clears_the_failure_state(self, app, monkeypatch):
        set_workers(monkeypatch, 0, 8)
        monkeypatch.setattr(health, "_check_db", lambda: False)
        c = app.test_client()
        for _ in range(4):
            c.get("/health/ready")
        assert c.get("/health/live").status_code == 503
        monkeypatch.setattr(health, "_check_db", lambda: True)
        c.get("/health/ready")
        assert c.get("/health/live").status_code == 200

    def test_liveness_does_no_io(self, app, monkeypatch):
        """Liveness must answer even when the database is unreachable and workers are full.

        It reports from state gathered by readiness, so it cannot itself block on the pool.
        """
        calls = []
        monkeypatch.setattr(health, "_check_db", lambda: calls.append(1) or True)
        set_workers(monkeypatch, 8, 8)
        app.test_client().get("/health/live")
        assert calls == []
