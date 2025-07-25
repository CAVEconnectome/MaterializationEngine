"""
Request-scoped database connection management.

This module provides utilities for managing database connections at the Flask request level,
ensuring that database sessions are reused throughout a single request rather than creating
multiple connections.
"""

import logging
from contextlib import contextmanager
from flask import g, current_app
from materializationengine.database import dynamic_annotation_cache

logger = logging.getLogger(__name__)


@contextmanager
def request_db_session(aligned_volume_name: str):
    """
    Context manager for request-scoped database sessions.
    
    This ensures that a single database session is reused throughout
    a Flask request, reducing connection overhead and improving performance.
    
    Args:
        aligned_volume_name (str): The name of the aligned volume/database
        
    Yields:
        Database session that can be used for queries
        
    Example:
        with request_db_session(aligned_volume_name) as db:
            metadata = db.database.get_table_metadata(table_name)
            # Additional operations use the same connection
    """
    # Initialize request-level session storage if not exists
    if not hasattr(g, 'db_sessions'):
        g.db_sessions = {}
    
    # Get or create session for this aligned volume
    if aligned_volume_name not in g.db_sessions:
        try:
            db = dynamic_annotation_cache.get_db(aligned_volume_name)
            g.db_sessions[aligned_volume_name] = db
            logger.debug(f"Created new request-scoped session for {aligned_volume_name}")
        except Exception as e:
            logger.error(f"Failed to create database session for {aligned_volume_name}: {e}")
            raise
    
    yield g.db_sessions[aligned_volume_name]


def cleanup_request_db_sessions():
    """
    Clean up request-scoped database sessions.
    
    This should be called at the end of each request to properly
    close any database sessions that were created during the request.
    """
    if hasattr(g, 'db_sessions'):
        for volume_name, db in g.db_sessions.items():
            try:
                # Close the cached session if it exists
                if hasattr(db.database, '_cached_session') and db.database._cached_session:
                    db.database.close_session()
                logger.debug(f"Cleaned up session for {volume_name}")
            except Exception as e:
                logger.warning(f"Error cleaning up session for {volume_name}: {e}")
        
        # Clear the session storage
        g.db_sessions = {}


def init_request_db_cleanup(app):
    """
    Initialize request-scoped database session cleanup for a Flask app.
    
    Args:
        app: Flask application instance
    """
    @app.teardown_appcontext
    def teardown_db_sessions(error):
        """Clean up database sessions at the end of each request."""
        cleanup_request_db_sessions()
        if error:
            logger.error(f"Request ended with error: {error}")