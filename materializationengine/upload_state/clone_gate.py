"""Frozen materialization's side of sharing the live database with uploads built in it.

create_analysis_database copies the live (aligned volume) database with CREATE DATABASE
... WITH TEMPLATE, which first terminates every connection to it and fails if one comes
back before the copy starts. A Ray upload builds its tables in that database for hours
(index builds, the spatial lookup), so it would be cut off once per version, and a
killed index build starts again from nothing.

clone_gate records a request, waits until no upload holds the database (each one stops
starting work there, lets what is running finish and closes its connections), yields
for the terminate-and-copy, and then releases the request so the uploads carry on. The
wait is bounded (UPLOAD_CLONE_WAIT_SECONDS, 2 h by default: an upload finishes at most
one statement, the longest a GIST index build of ~1.6 h on 337M rows); after it the copy
goes ahead anyway, and an upload whose connection is terminated resumes its stage.
"""

import contextlib
import logging
import socket
import time

from materializationengine.utils import get_config_param

logger = logging.getLogger(__name__)

DEFAULT_CLONE_WAIT_SECONDS = 7200
POLL_SECONDS = 15


def _enabled() -> bool:
    """Whether any upload could be building in a production database: the upload state
    database exists. Checked without creating it, and without the Ray settings, which
    the celery workers that run create_analysis_database do not have."""
    from materializationengine.upload_state import db

    try:
        return db.database_exists()
    except Exception as e:
        logger.warning(f"could not check for the upload state database: {e}")
        return False


@contextlib.contextmanager
def clone_gate(aligned_volume: str, wait_seconds: float = None, poll_seconds: float = POLL_SECONDS):
    """Hold uploads building in aligned_volume off it while the block runs."""
    if not _enabled():
        yield
        return
    from materializationengine.upload_state import db, store

    try:
        engine = db.get_engine()
        request_id = store.request_clone(engine, aligned_volume, requested_by=socket.gethostname())
    except Exception as e:
        # the upload state database is an optional extra; materialization goes on
        logger.warning(f"clone gate unavailable for {aligned_volume}, copying without it: {e}")
        yield
        return
    try:
        if wait_seconds is None:
            wait_seconds = float(
                get_config_param("UPLOAD_CLONE_WAIT_SECONDS") or DEFAULT_CLONE_WAIT_SECONDS
            )
        deadline = time.monotonic() + wait_seconds
        while True:
            holding = store.jobs_holding_production(engine, aligned_volume)
            if not holding:
                break
            if time.monotonic() > deadline:
                logger.warning(
                    f"uploads {holding} still hold {aligned_volume} after {wait_seconds:.0f}s; "
                    "copying anyway (their stages resume after the terminate)"
                )
                break
            logger.info(f"waiting for uploads {holding} to release {aligned_volume}")
            time.sleep(poll_seconds)
        yield
    finally:
        try:
            store.release_clone(engine, request_id)
        except Exception as e:
            logger.warning(f"could not release clone request {request_id}: {e}")
