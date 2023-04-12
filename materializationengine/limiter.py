from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import current_app, g
from functools import wraps
import os


def limit_by_category(category):
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            limiter.limit(limiter.default_limits[0](category),
                          key_func=lambda: g.auth_user["id"])(f)
            return f(*args, **kwargs)
        return wrapped
    return decorator


def get_rate_limit_from_config(category):
    if not current_app or category not in current_app.config['RATE_LIMITS']:
        return None  # Default rate limit if not found
    return current_app.config['LIMITER_CATEGORIES'][category]


limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
