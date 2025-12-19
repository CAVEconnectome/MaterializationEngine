from contextlib import contextmanager
from urllib.parse import urlparse

from dynamicannotationdb import DynamicAnnotationInterface
from flask import current_app
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import QueuePool

from materializationengine.celery_worker import celery_logger
from materializationengine.utils import get_config_param


def get_sql_url_params(sql_url):
    if not isinstance(sql_url, str):
        sql_url = str(sql_url)
    result = urlparse(sql_url)
    url_mapping = {
        "user": result.username,
        "password": result.password,
        "dbname": result.path[1:],
        "host": result.hostname,
        "port": result.port,
    }
    return url_mapping


def reflect_tables(sql_base, database_name):
    sql_uri = f"{sql_base}/{database_name}"
    engine = create_engine(sql_uri)
    meta = MetaData(engine)
    meta.reflect(views=True)
    tables = [table for table in meta.tables]
    engine.dispose()
    return tables


def ping_connection(session):
    is_database_working = True
    try:
        # to check database we will execute raw query
        session.execute("SELECT 1")
    except Exception as e:
        celery_logger.warning(e)
        is_database_working = False
    return is_database_working


class DatabaseConnectionManager:
    """Manages database connections and session lifecycle."""
    
    def __init__(self):
        self._engines = {}
        self._session_factories = {}
        
    def get_engine(self, database_name: str):
        """Get or create SQLAlchemy engine with proper pooling configuration."""
        if database_name not in self._engines:
            SQL_URI_CONFIG = current_app.config["SQLALCHEMY_DATABASE_URI"]
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = f"{sql_base_uri}/{database_name}"
            
            pool_size = current_app.config.get("DB_CONNECTION_POOL_SIZE", 20)
            max_overflow = current_app.config.get("DB_CONNECTION_MAX_OVERFLOW", 30)
            
            try:
                engine = create_engine(
                    sql_uri,
                    poolclass=QueuePool,
                    pool_size=pool_size,
                    max_overflow=max_overflow,
                    pool_timeout=30,
                    pool_recycle=1800,  # Recycle connections after 30 minutes
                    pool_pre_ping=True,  # Ensure connections are still valid
                )
                
                # Test the connection to make sure the database exists and is accessible
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                # Only store engine if connection test passes
                self._engines[database_name] = engine
                celery_logger.info(f"Created new connection pool for {database_name} "
                           f"(size={pool_size}, max_overflow={max_overflow})")
                
            except Exception as e:
                # Clean up engine if it was created but connection failed
                if 'engine' in locals():
                    engine.dispose()
                
                celery_logger.error(f"Failed to create/connect to database {database_name}: {e}")
                raise ConnectionError(f"Cannot connect to database '{database_name}'. "
                                    f"Please check if the database exists and is accessible. "
                                    f"Connection URI: {sql_uri}. "
                                    f"Error: {e}")
            
        return self._engines[database_name]
    
    def get_session_factory(self, database_name: str):
        """Get or create scoped session factory for a database."""
        if database_name not in self._session_factories:
            engine = self.get_engine(database_name)
            self._session_factories[database_name] = scoped_session(
                sessionmaker(
                    bind=engine,
                    autocommit=False,
                    autoflush=False,
                    expire_on_commit=True  # Ensure objects are not bound to session after commit
                )
            )
        return self._session_factories[database_name]

    @contextmanager
    def session_scope(self, database_name: str):
        """Context manager for database sessions.
        
        Handles proper session lifecycle including commits and rollbacks.
        """
        session_factory = self.get_session_factory(database_name)
        session = session_factory()
        
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def cleanup(self):
        """Cleanup any remaining sessions and dispose of engine pools."""
        self.shutdown()
    
    def shutdown(self):
        """Shutdown all database connections and dispose of all engines.
        
        This method:
        1. Closes all active sessions from session factories
        2. Removes all scoped sessions
        3. Disposes of all engine connection pools
        4. Clears all cached engines and session factories
        
        Should be called when the application is shutting down or when
        you want to ensure all database connections are closed.
        """
        celery_logger.info("Shutting down DatabaseConnectionManager...")
        
        # First, close all active sessions from session factories
        for database_name, session_factory in list(self._session_factories.items()):
            try:
                # Remove all scoped sessions (this closes them)
                session_factory.remove()
                celery_logger.debug(f"Removed scoped sessions for {database_name}")
            except Exception as e:
                celery_logger.warning(f"Error removing sessions for {database_name}: {e}")
        
        # Then dispose of all engines (this closes all connections in the pool)
        for database_name, engine in list(self._engines.items()):
            try:
                # Log pool status before disposal
                pool = engine.pool
                checked_out = pool.checkedout()
                if checked_out > 0:
                    celery_logger.warning(
                        f"Disposing engine for {database_name} with {checked_out} "
                        f"checked-out connections"
                    )
                
                # Dispose closes all connections in the pool
                engine.dispose()
                celery_logger.debug(f"Disposed engine connection pool for {database_name}")
            except Exception as e:
                celery_logger.error(f"Error disposing engine for {database_name}: {e}")
        
        # Clear all caches
        self._session_factories.clear()
        self._engines.clear()
        
        celery_logger.info("DatabaseConnectionManager shutdown complete")
    
    def log_pool_status(self, database_name: str):
        """Log current connection pool status."""
        if database_name in self._engines:
            engine = self._engines[database_name]
            pool = engine.pool
            celery_logger.info(
                f"Pool status for {database_name}: "
                f"checked in={pool.checkedin()}, "
                f"checked out={pool.checkedout()}, "
                f"size={pool.size()}, "
                f"overflow={pool.overflow()}"
            )

class DynamicMaterializationCache:
    def __init__(self):
        self._clients = {}

    def get_db(self, database: str) -> DynamicAnnotationInterface:
        if database not in self._clients:
            db_client = self._get_mat_client(database)
        db_client = self._clients[database]

        connection_ok = ping_connection(db_client.database.cached_session)

        if not connection_ok:
            db_client = self._get_mat_client(database)

        return self._clients[database]

    def _get_mat_client(self, database: str):
        sql_uri_config = get_config_param("SQLALCHEMY_DATABASE_URI")
        pool_size = current_app.config.get("DB_CONNECTION_POOL_SIZE", 20)
        max_overflow = current_app.config.get("DB_CONNECTION_MAX_OVERFLOW", 30)
        mat_client = DynamicAnnotationInterface(
            sql_uri_config, database, pool_size, max_overflow
        )
        self._clients[database] = mat_client
        return self._clients[database]

    def invalidate_cache(self):
        """Invalidate the cache by clearing all client references.
        
        Note: This does NOT close database connections. Use shutdown() for that.
        """
        self._clients = {}
    
    def shutdown(self):
        """Shutdown all database connections and clear the cache.
        
        This method:
        1. Closes all cached sessions in DynamicAnnotationInterface clients
        2. Disposes of underlying database engines if available
        3. Clears all cached clients
        
        Should be called when the application is shutting down or when
        you want to ensure all database connections are closed.
        """
        celery_logger.info("Shutting down DynamicMaterializationCache...")
        
        for database, client in list(self._clients.items()):
            try:
                # Close the cached session if it exists
                if hasattr(client, 'database'):
                    # Close the session
                    if hasattr(client.database, 'close_session'):
                        try:
                            client.database.close_session()
                            celery_logger.debug(f"Closed session for {database}")
                        except Exception as e:
                            celery_logger.warning(
                                f"Error closing session for {database}: {e}"
                            )
                    
                    # Dispose of the engine if it exists
                    if hasattr(client.database, 'engine'):
                        try:
                            engine = client.database.engine
                            if engine:
                                pool = engine.pool
                                checked_out = pool.checkedout()
                                if checked_out > 0:
                                    celery_logger.warning(
                                        f"Disposing engine for {database} with "
                                        f"{checked_out} checked-out connections"
                                    )
                                engine.dispose()
                                celery_logger.debug(f"Disposed engine for {database}")
                        except Exception as e:
                            celery_logger.warning(
                                f"Error disposing engine for {database}: {e}"
                            )
                    
                    # Also try to close any cached session directly
                    if hasattr(client.database, '_cached_session'):
                        try:
                            cached_session = client.database._cached_session
                            if cached_session:
                                cached_session.close()
                                celery_logger.debug(f"Closed cached session for {database}")
                        except Exception as e:
                            celery_logger.warning(
                                f"Error closing cached session for {database}: {e}"
                            )
                
            except Exception as e:
                celery_logger.error(f"Error shutting down client for {database}: {e}")
        
        # Clear all clients
        self._clients.clear()
        
        celery_logger.info("DynamicMaterializationCache shutdown complete")


dynamic_annotation_cache = DynamicMaterializationCache()
db_manager = DatabaseConnectionManager()
