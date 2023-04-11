from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import current_app
import os


def get_limit_string(category):
    return current_app.config.LIMITER_CATEGORIES.get(category, None)


limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
