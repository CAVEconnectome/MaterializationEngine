"""Separate liveness and readiness checks, with distinct meanings.

The two probes answer different questions, and conflating them causes both of the failures
seen on minniev7 on 2026-08-18:

* **Readiness** -- "should the Service send me traffic?" A saturated pod, or one that cannot
  reach the database, should be drained. Draining is cheap and reversible: the pod keeps
  running and rejoins when it recovers.
* **Liveness** -- "am I broken in a way only a restart fixes?" A corrupted SQLAlchemy pool is
  the real example: the process will never recover on its own. Being *busy* is emphatically
  not such a case.

Both probes previously pointed at ``/health``, which does a database query and is served by the
same uwsgi worker pool as real traffic, with identical thresholds. So:

* a merely-busy pod failed liveness and was restarted, dumping its in-flight requests --
  41 "failed liveness probe, will be restarted" events in 30 minutes, and
* a pod whose pool was corrupted would fail readiness forever and never be restarted, because
  nothing distinguished it from a busy one.

``/health`` is deliberately left untouched for anything already pointing at it.

Why readiness can report saturation but liveness must not
--------------------------------------------------------
Every endpoint here is served by a uwsgi worker, so when workers are full even a trivial
endpoint is unreachable. That is *acceptable for readiness* (unreachable and "too busy" both
mean stop sending traffic) but *fatal for liveness*, so ``/health/live`` does no I/O at all and
answers from state already gathered by readiness. Combined with a generous liveness
failureThreshold, transient worker starvation cannot restart a healthy pod, while a genuinely
DB-broken one still gets restarted.
"""

import logging
import time

from flask import Blueprint, current_app, jsonify

logger = logging.getLogger("materializationengine.health")

health_bp = Blueprint("health", __name__)

# Per-worker-process state. The SQLAlchemy engine and its pool are per process, so this is the
# correct scope: if this process cannot reach the database, this process needs restarting.
#
# Seeded as "never failed" rather than "never succeeded" on purpose -- an idle worker that has
# run no checks must not be reported dead. Only observed failures move it.
_db_last_ok = None
_db_consecutive_failures = 0


def _record_db(ok):
    global _db_last_ok, _db_consecutive_failures
    if ok:
        _db_last_ok = time.monotonic()
        _db_consecutive_failures = 0
    else:
        _db_consecutive_failures += 1


def _check_db():
    """Cheapest possible round trip through the pool. True/False, never raises.

    SELECT 1 rather than the counting query ``/health`` uses: the point is to exercise the
    connection pool, not the data, and a probe that runs every few seconds on every pod should
    not put real query load on Cloud SQL.
    """
    from materializationengine.database import db_manager

    try:
        aligned_volume = current_app.config.get("TEST_DB_NAME", "annotation")
        with db_manager.session_scope(aligned_volume) as session:
            session.execute("SELECT 1")
        return True
    except Exception as exc:  # noqa: BLE001 - a probe must never propagate
        logger.warning("health: database check failed: %s", exc)
        return False


def _worker_saturation():
    """(busy, total) uwsgi workers, or None when not running under uwsgi.

    ``total`` is the number of worker SLOTS (i.e. ``processes``), not the number currently
    spawned. That distinction matters: with the busyness cheaper algorithm a pod may have 2 of 8
    workers spawned and both busy, which is 100% of spawned but only 25% of capacity -- uwsgi
    will spawn more. Using spawned as the denominator would drain a pod that is nowhere near its
    ceiling.
    """
    try:
        import uwsgi  # noqa: PLC0415 - only present inside a uwsgi worker
    except ImportError:
        return None
    try:
        workers = uwsgi.workers()
    except Exception:  # noqa: BLE001
        return None
    if not workers:
        return None
    busy = sum(1 for w in workers if w.get("status") == "busy")
    return busy, len(workers)


@health_bp.route("/health/ready")
def ready():
    """Readiness: drain this pod when it is saturated or cannot reach the database."""
    detail = {}
    ready_ok = True

    max_busy = current_app.config.get("HEALTH_READY_MAX_BUSY_FRACTION", 0.9)
    sat = _worker_saturation()
    if sat is not None:
        busy, total = sat
        detail["busy_workers"] = busy
        detail["total_workers"] = total
        detail["busy_fraction"] = round(busy / total, 3) if total else None
        # A max_busy of 0 (or None) disables saturation-based draining entirely, leaving
        # readiness purely a database check.
        if max_busy and total and (busy / total) >= max_busy:
            ready_ok = False
            detail["reason"] = "worker pool saturated"

    db_ok = _check_db()
    _record_db(db_ok)
    detail["database"] = "ok" if db_ok else "unreachable"
    if not db_ok:
        ready_ok = False
        detail["reason"] = "database unreachable"

    return jsonify(detail), (200 if ready_ok else 503)


@health_bp.route("/health/live")
def live():
    """Liveness: fail only for something a restart would actually fix.

    Does no I/O -- no database call, no uwsgi introspection -- so that a slow or contended pod
    still answers immediately. It reports unhealthy only once *this process* has seen repeated
    database failures AND has not succeeded for a while, which is the corrupted-pool signature.
    """
    failures = current_app.config.get("HEALTH_LIVE_DB_FAILURES", 3)
    stale_after = current_app.config.get("HEALTH_LIVE_DB_STALE_SECONDS", 120)

    stale_for = None
    if _db_last_ok is not None:
        stale_for = round(time.monotonic() - _db_last_ok, 1)

    # Both conditions are required. Consecutive failures alone could be a brief Cloud SQL
    # blip, and staleness alone happens whenever a worker is simply idle.
    broken = _db_consecutive_failures >= failures and (
        _db_last_ok is None or stale_for > stale_after
    )

    detail = {
        "db_consecutive_failures": _db_consecutive_failures,
        "db_seconds_since_ok": stale_for,
    }
    if broken:
        detail["reason"] = "database unreachable from this worker; restart required"
        logger.error("health: reporting NOT ALIVE -- %s", detail)
        return jsonify(detail), 503
    return jsonify(detail), 200


def reset_state_for_tests():
    global _db_last_ok, _db_consecutive_failures
    _db_last_ok = None
    _db_consecutive_failures = 0
