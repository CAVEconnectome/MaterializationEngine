from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import current_app, g
from functools import wraps
import os


def limit_by_category(category):
    def decorator(f):
        @wraps(f)
        @limiter.limit(
            get_rate_limit_from_config(category), key_func=lambda: g.auth_user["id"]
        )
        def wrapped(*args, **kwargs):
            return f(*args, **kwargs)

        return wrapped

    return decorator


def get_rate_limit_from_config(category=None):
    if category:
        if not current_app or category not in current_app.config["LIMITER_CATEGORIES"]:
            return None  # Default rate limit if not found
    else:
        return None
    return current_app.config["LIMITER_CATEGORIES"][category]


limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
