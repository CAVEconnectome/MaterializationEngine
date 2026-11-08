"""Open an API worker's database connections and fill its caches before it serves a request.

uwsgi builds the app once in the master and forks the workers from it, and the cheaper algorithm
keeps spawning fresh workers under load. Each new worker used to pay for its first connections,
its datastack info lookup and its table metadata on its first user request. register_prewarm runs
after_fork in every worker right after the fork: it first drops the database clients inherited
from the master (a socket shared by two processes interleaves their queries), then prewarms.
"""
import logging
import time

from flask import g

logger = logging.getLogger(__name__)


def _recent_versions(aligned_volume: str, datastack: str, n: int) -> list:
    from dynamicannotationdb.models import AnalysisVersion

    from materializationengine.database import db_manager

    with db_manager.session_scope(aligned_volume) as session:
        rows = (
            session.query(AnalysisVersion.version)
            .filter(AnalysisVersion.datastack == datastack)
            .filter(AnalysisVersion.valid == True)  # noqa: E712 - SQL expression
            .order_by(AnalysisVersion.time_stamp.desc())
            .limit(n)
            .all()
        )
    return [r[0] for r in rows]


def prewarm_worker(app) -> None:
    from materializationengine.blueprints.client import query as query_module
    from materializationengine.database import (
        KEEP_DB_CONNECTIONS_FLAG,
        db_manager,
        dynamic_annotation_cache,
    )
    from materializationengine.info_client import get_relevant_datastack_info

    t0 = time.perf_counter()
    n_versions = int(app.config.get("WEB_PREWARM_VERSIONS", 2))
    warmed = []
    with app.app_context():
        # this is not a request, so without the flag the app-context teardown disposes every pool
        setattr(g, KEEP_DB_CONNECTIONS_FLAG, True)
        for datastack in app.config.get("DATASTACKS", []):
            try:
                aligned_volume, _ = get_relevant_datastack_info(datastack)
                with db_manager.session_scope(aligned_volume) as session:
                    session.execute("SELECT 1")
                dynamic_annotation_cache.get_db(aligned_volume)
                warmed.append(aligned_volume)
                for version in _recent_versions(aligned_volume, datastack, n_versions):
                    mat_db = f"{datastack}__mat{version}"
                    db = dynamic_annotation_cache.get_db(mat_db)
                    if query_module.adbc_postgresql is not None:
                        query_module._adbc_connection(db.database.engine)
                    warmed.append(mat_db)
            except Exception as e:  # never keep a worker from starting
                logger.warning(f"prewarm of {datastack} failed: {e}")
    logger.info(f"prewarmed {warmed} in {time.perf_counter() - t0:.2f}s")


def after_fork(app) -> None:
    """Run in each uwsgi worker right after it is forked from the master.

    The master built the app, and create_app opens database clients (the staging client, at
    least). Forget them without closing them first: their sockets are shared with the master and
    the sibling workers, and using them from two processes interleaves the protocol. Then prewarm.
    """
    from materializationengine.database import reset_database_connections

    reset_database_connections(close_connections=False)
    if app.config.get("WEB_PREWARM", True):
        prewarm_worker(app)


def register_prewarm(app) -> None:
    """Run after_fork after every uwsgi fork. A no-op outside uwsgi (celery, tests, flask run)."""
    try:
        from uwsgidecorators import postfork  # only importable inside uwsgi
    except ImportError:
        return

    @postfork
    def _after_fork():
        after_fork(app)
