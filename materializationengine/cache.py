"""
Caching utilities for MaterializationEngine.

This module provides the Flask-Caching instance to avoid circular imports.
"""

from flask_caching import Cache

# Global cache instance
cache = Cache()