from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import current_app, g, request
from flask_restx import Resource
from middle_auth_client import auth_requires_permission
from materializationengine.blueprints.reset_auth import reset_auth
from functools import wraps
import os
import logging
import json

logging.basicConfig(level=logging.DEBUG)


class AuthLimitedResourceMixin(Resource):
    limit_category = None
    required_permissions = {
        "get": None,
        "post": None,
        "put": None,
    }
    table_arg = None

    def __init__(self, *args, **kwargs):
        super(AuthLimitedResourceMixin, self).__init__(*args, **kwargs)
        self.dispatch_request_decorated = self.create_decorated_dispatch()

    def create_decorated_dispatch(self):
        def view_func(*args, **kwargs):
            return super(AuthLimitedResourceMixin, self).dispatch_request(
                *args, **kwargs
            )

        method = request.method.lower()
        if method in {"get", "post", "put"}:
            limit = get_rate_limit_from_config(self.limit_category)
            if limit is not None:
                view_func = limiter.limit(limit, key_func=lambda: g.auth_user["id"])(
                    view_func
                )
            if self.required_permissions[method] is not None:
                view_func = auth_requires_permission(
                    self.required_permissions[method], table_arg=self.table_arg
                )(view_func)
            view_func = reset_auth(view_func)

        return view_func

    def dispatch_request(self, *args, **kwargs):
        return self.dispatch_request_decorated(*args, **kwargs)


class AuthLimitedQueryResourceMixin(AuthLimitedResourceMixin):
    limit_category = "query"
    required_permissions = {"get": "view", "post": "view", "put": "edit"}
    table_arg = "datastack_name"


# def limit_by_category(category):
#     limit = get_rate_limit_from_config(category)

#     @limiter.limit(limit, key_func=lambda: g.auth_user["id"])
#     def decorator(func):
#         @wraps(func)
#         def wrapped(*args, **kwargs):
#             return func(*args, **kwargs)

#         return wrapped

#     return decorator

LIMITER_CATEGORIES = {}


def limit_by_category(category):
    
    limit = get_rate_limit_from_config(category)
    if limit is not None:
        return limiter.limit(limit, key_func=lambda: g.auth_user["id"])
    return lambda x: x

    # def limit_by_category(category):
    #     def decorator(func):
    #         limit = get_rate_limit_from_config(category)
    #         if limit is not None:
    #             return limiter.limit(limit, key_func=lambda: g.auth_user["id"])(func)
    #         return func

    # return decorator


def get_rate_limit_from_config(category=None):
    
    if category:
        categories = json.loads(os.environ['LIMITER_CATEGORIES'])
        if category not in categories:
            return None  # Default rate limit if not found
        return categories[category]
    else:
        return None
    


class MyLimiter(Limiter):
    def __init__(self, *args, **kwargs):
        super(MyLimiter, self).__init__(*args, **kwargs)
        self.categories = {}

    def init_app(self, app, *args, **kwargs):
        super(MyLimiter, self).init_app(app, *args, **kwargs)
        self.categories = app.config.get('LIMITER_CATEGORIES', {})


limiter = MyLimiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
