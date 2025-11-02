"""
Cached metadata functions for MaterializationEngine.

This module provides cached versions of frequently accessed metadata functions
to reduce database load and improve performance.
"""

from cachetools import TTLCache, cached
from materializationengine.database import dynamic_annotation_cache
from materializationengine.request_db import request_db_session

@cached(cache=TTLCache(maxsize=256, ttl=86400))  # 1 day TTL = 86400 seconds
def get_cached_table_metadata(meta_db_name: str, table_name: str):
    """Get table metadata with 1-day TTL cache.
    
    Args:
        meta_db_name (str): The name of the metadata database
        table_name (str): The name of the table
        
    Returns:
        dict: Table metadata dictionary
    """
    with request_db_session(meta_db_name) as meta_db:  
        return meta_db.database.get_table_metadata(table_name)


@cached(cache=TTLCache(maxsize=256, ttl=86400))  # 1 day TTL = 86400 seconds
def get_cached_view_metadata(meta_db_name: str, datastack_name: str, view_name: str):
    """Get view metadata with 1-day TTL cache.
    
    Args:
        meta_db_name (str): The name of the metadata database
        datastack_name (str): The name of the datastack
        view_name (str): The name of the view
        
    Returns:
        dict: View metadata dictionary
    """
    with request_db_session(meta_db_name) as meta_db:  
        return meta_db.database.get_view_metadata(datastack_name, view_name)