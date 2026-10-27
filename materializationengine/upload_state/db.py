"""The upload state database: created on first use, on the instance the app uses.

The API, the RayJob driver and the cleanup CronJob all call get_engine(), which creates
the database (CREATE DATABASE, as dynamicannotationdb does for aligned volumes) and its
tables if they are missing. The app's database user can create databases (Cloud SQL's
postgres user is in cloudsqlsuperuser), so a deployment needs no extra setup step.
"""

import logging
import threading
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.pool import NullPool

from materializationengine.upload_state import models

logger = logging.getLogger(__name__)

DEFAULT_DATABASE = "upload_state"
# pg_advisory_xact_lock key serialising schema creation across pods.
SCHEMA_LOCK_ID = 7321005  # arbitrary, constant

_engines = {}
_lock = threading.Lock()


class UploadStateSchemaError(RuntimeError):
    """The database holds a newer schema than this code understands."""


def _config(name: str, default=None):
    from materializationengine.utils import get_config_param

    return get_config_param(name, default)


def database_name() -> str:
    return _config("UPLOAD_STATE_DATABASE") or DEFAULT_DATABASE


def admin_url() -> str:
    """The app's own database URI, used to check for and create the state database."""
    url = _config("SQLALCHEMY_DATABASE_URI")
    if not url:
        raise RuntimeError("SQLALCHEMY_DATABASE_URI is not configured")
    return url


def database_url(admin: str, name: str) -> str:
    return f"{admin.rpartition('/')[0]}/{name}"


def ensure_database(admin: str, name: str) -> bool:
    """Create database name if it does not exist; True if it was created here."""
    engine = create_engine(admin, poolclass=NullPool, isolation_level="AUTOCOMMIT")
    try:
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": name}
            ).scalar()
            if exists:
                return False
            try:
                conn.execute(text(f'CREATE DATABASE "{name}"'))
            except (ProgrammingError, IntegrityError) as e:
                # another pod created it between the check and here
                if "already exists" in str(e) or "duplicate key" in str(e):
                    return False
                raise
            logger.info(f"Created upload state database {name}")
            return True
    finally:
        engine.dispose()


def ensure_schema(engine) -> int:
    """Create the tables if they are missing; returns the schema version in use."""
    with engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(:id)"), {"id": SCHEMA_LOCK_ID})
        models.metadata.create_all(bind=conn, checkfirst=True)
        current = conn.execute(text("SELECT max(version) FROM schema_version")).scalar()
        if current is not None and current > models.SCHEMA_VERSION:
            raise UploadStateSchemaError(
                f"upload state schema is version {current}; this code knows up to "
                f"{models.SCHEMA_VERSION}"
            )
        if current is None or current < models.SCHEMA_VERSION:
            # Version 1 is the first: create_all above is all it needs. Later versions
            # add their migration steps here, before recording the new version.
            conn.execute(
                text("INSERT INTO schema_version (version) VALUES (:v) ON CONFLICT DO NOTHING"),
                {"v": models.SCHEMA_VERSION},
            )
    return models.SCHEMA_VERSION


def get_engine(name: Optional[str] = None, admin: Optional[str] = None):
    """The engine for the upload state database, creating the database and tables once."""
    admin = admin or admin_url()
    name = name or database_name()
    key = (admin, name)
    with _lock:
        engine = _engines.get(key)
        if engine is None:
            ensure_database(admin, name)
            engine = create_engine(
                database_url(admin, name),
                pool_size=int(_config("UPLOAD_STATE_POOL_SIZE", 2)),
                max_overflow=int(_config("UPLOAD_STATE_MAX_OVERFLOW", 3)),
                pool_pre_ping=True,
                pool_recycle=1800,
            )
            ensure_schema(engine)
            _engines[key] = engine
    return engine


def dispose_engines() -> None:
    with _lock:
        for engine in _engines.values():
            engine.dispose()
        _engines.clear()


def init_upload_state(app) -> bool:
    """Create the upload state database at app startup; never stops the app starting.

    If it fails here (e.g. the database server is briefly unreachable), the first
    get_engine() call creates it instead.
    """
    try:
        with app.app_context():
            get_engine()
        return True
    except Exception as e:
        logger.error(f"Could not set up the upload state database yet: {e}")
        return False
