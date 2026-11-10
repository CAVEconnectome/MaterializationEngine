"""Open an API worker's database connections and fill its caches before it serves a request.

uwsgi builds the app once in the master and forks the workers from it, and the cheaper algorithm
keeps spawning fresh workers under load. Each new worker used to pay for its first connections,
its datastack info lookup and its table metadata on its first user request. register_prewarm runs
after_fork in every worker right after the fork: it first drops the database clients inherited
from the master (a socket shared by two processes interleaves their queries), then prewarms.
"""
import logging
import os
import time

from flask import g

from materializationengine.config import as_bool

logger = logging.getLogger(__name__)


def _setting(app, name: str, default):
    """app.config first, then the environment. This runs right after the fork, before any app
    context, so get_config_param (which reads current_app) cannot be used."""
    value = app.config.get(name)
    return os.environ.get(name, default) if value is None else value


def _recent_version_databases(aligned_volume: str, datastack: str, n: int) -> list:
    """Database names of the datastack's n most recent valid versions. A virtual version (one
    with a parent_version) reads its parent's database, as validate_datastack resolves it."""
    from dynamicannotationdb.models import AnalysisVersion

    from materializationengine.database import db_manager

    with db_manager.session_scope(aligned_volume) as session:
        rows = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.datastack == datastack)
            .filter(AnalysisVersion.valid == True)  # noqa: E712 - SQL expression
            .order_by(AnalysisVersion.time_stamp.desc())
            .limit(n)
            .all()
        )
        names = []
        for v in rows:
            if v.parent_version is not None:
                parent = session.query(AnalysisVersion).filter(AnalysisVersion.id == v.parent_version).first()
                if parent is None:
                    continue
                names.append(f"{parent.datastack}__mat{parent.version}")
            else:
                names.append(f"{datastack}__mat{v.version}")
    return list(dict.fromkeys(names))


def prewarm_worker(app) -> None:
    from materializationengine.blueprints.client import query as query_module
    from materializationengine.database import (
        KEEP_DB_CONNECTIONS_FLAG,
        db_manager,
        dynamic_annotation_cache,
    )
    from materializationengine.info_client import get_relevant_datastack_info

    t0 = time.perf_counter()
    n_versions = int(_setting(app, "WEB_PREWARM_VERSIONS", 2))
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
                mat_dbs = _recent_version_databases(aligned_volume, datastack, n_versions)
            except Exception as e:  # never keep a worker from starting
                logger.warning(f"prewarm of {datastack} failed: {e}")
                continue
            for mat_db in mat_dbs:
                try:
                    db = dynamic_annotation_cache.get_db(mat_db)
                    if query_module.adbc_postgresql is not None:
                        query_module._adbc_connection(db.database.engine)
                    warmed.append(mat_db)
                except Exception as e:  # one missing database does not stop the others
                    logger.warning(f"prewarm of {mat_db} failed: {e}")
            if as_bool(_setting(app, "QUERY_LAKE_PREWARM", True), default=True, name="QUERY_LAKE_PREWARM"):
                warmed += _prewarm_lakes(datastack, mat_dbs)
    logger.info(f"prewarmed {warmed} in {time.perf_counter() - t0:.2f}s")


def _prewarm_lakes(datastack: str, mat_dbs: list) -> list:
    """Planning indexes of the lakes this worker's first lake queries are likely to read: the
    recent versions' copies of the most queried tables, and the most queried lakes, by the
    tallies the workers keep in redis (lake_index). A virtual version's database is its
    parent's, and so is its catalog."""
    from materializationengine.blueprints.client import lake_query

    versions = {}
    for name in mat_dbs:
        m = lake_query._FROZEN_DB.match(name)
        if m:
            versions.setdefault(m["datastack"], []).append(int(m["version"]))
    out = []
    for ds, vs in versions.items():
        t0 = time.perf_counter()
        try:
            done = lake_query.prewarm(ds, vs)
        except Exception as e:  # never keep a worker from starting
            logger.warning(f"lake prewarm of {ds} failed: {type(e).__name__}: {e}")
            continue
        logger.info(f"lake prewarm of {ds} v{vs}: {done} in {time.perf_counter() - t0:.2f}s")
        out.append(f"lakes:{ds}:{len(done)}")
    return out


def after_fork(app) -> None:
    """Run in each uwsgi worker right after it is forked from the master.

    The master built the app, and create_app opens database clients (the staging client, at
    least). Forget them without closing them first: their sockets are shared with the master and
    the sibling workers, and using them from two processes interleaves the protocol. Then prewarm.
    """
    from materializationengine.database import reset_database_connections

    reset_database_connections(close_connections=False)
    if as_bool(_setting(app, "WEB_PREWARM", True), default=True, name="WEB_PREWARM"):
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
