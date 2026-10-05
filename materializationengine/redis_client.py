"""One Redis client, and so one connection pool, per process per database.

Modules used to each build their own client at import time (and a few built a new one
on every call), so every worker process held a separate pool, and connection, per
module: on ltv7 a consumer pod with --concurrency=2 held ~22 connections. They now
share one client per database. redis-py pools notice a fork and reopen their
connections, so a client created before the prefork pool forks is safe to reuse.

The settings are read on first use rather than at import, so they come from the app
config when there is one (import-time reads only ever saw environment variables).
"""

import threading
from typing import Dict

import redis

from materializationengine.utils import get_config_param

# Replies never take this long; a server that stops answering (ltv7-mat-redis hung for
# ~20 minutes on 2026-10-05) should fail fast instead of hanging the caller forever. None
# of these clients run blocking commands (BLPOP, pub/sub), which this would cut short.
SOCKET_TIMEOUT_SECONDS = 20
SOCKET_CONNECT_TIMEOUT_SECONDS = 10
# PING a connection that has been idle this long before reusing it, so connections
# left dead by a Redis restart or failover are replaced instead of failing a request.
HEALTH_CHECK_INTERVAL_SECONDS = 30

_clients: Dict[int, redis.StrictRedis] = {}
_lock = threading.Lock()


def get_redis_client(db: int = 0) -> redis.StrictRedis:
    """The shared client for Redis database `db`, created on first use."""
    client = _clients.get(db)
    if client is None:
        with _lock:
            client = _clients.get(db)
            if client is None:
                client = redis.StrictRedis(
                    host=get_config_param("REDIS_HOST"),
                    port=get_config_param("REDIS_PORT"),
                    password=get_config_param("REDIS_PASSWORD") or None,
                    db=db,
                    socket_timeout=SOCKET_TIMEOUT_SECONDS,
                    socket_connect_timeout=SOCKET_CONNECT_TIMEOUT_SECONDS,
                    socket_keepalive=True,
                    health_check_interval=HEALTH_CHECK_INTERVAL_SECONDS,
                )
                _clients[db] = client
    return client


class SharedRedis:
    """Stands in for a module-level `redis.StrictRedis(...)`, forwarding to the shared
    client for its database. Keeps existing `REDIS_CLIENT.get(...)` call sites, and
    tests that patch a module's REDIS_CLIENT, unchanged."""

    def __init__(self, db: int = 0):
        self._db = db

    def __getattr__(self, name):
        return getattr(get_redis_client(self._db), name)


def reset_redis_clients() -> None:
    """Forget the shared clients (tests, or after the Redis settings change)."""
    with _lock:
        _clients.clear()
