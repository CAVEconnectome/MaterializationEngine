from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask import g
import os
import json


def _load_categories(env_var):
    try:
        categories = json.loads(os.environ.get(env_var, "{}"))
    except json.JSONDecodeError:
        return {}
    return categories if isinstance(categories, dict) else {}


def get_rate_limit_from_config(category=None):
    if category:
        categories = _load_categories("LIMITER_CATEGORIES")
        if category not in categories:
            return None  # Default rate limit if not found
        return categories[category]
    else:
        return None


def get_service_account_rate_limit(category, endpoint=None):
    """Elevated limit for service accounts, or None to apply the normal limit.

    Rate limits are bucketed per (user id, endpoint) -- flask_limiter uses the endpoint as the
    scope when limit() is given no explicit scope (Limit.scope_for). So an internal service that
    funnels through one endpoint exhausts that single bucket no matter how many replicas it runs,
    while the same identity's other endpoints are untouched.

    That makes the useful override per endpoint rather than blanket. LIMITER_SERVICE_ACCOUNT_
    CATEGORIES raises a whole category; LIMITER_SERVICE_ACCOUNT_OVERRIDES raises one endpoint,
    and wins where both apply:

        LIMITER_SERVICE_ACCOUNT_CATEGORIES={"query": "20000/minute"}
        LIMITER_SERVICE_ACCOUNT_OVERRIDES={"LiveTableQuery": {"query": "20000/minute"}}

    Both default to empty, so with no configuration service accounts get exactly the limits
    everyone else does and behaviour is unchanged.
    """
    if endpoint:
        overrides = _load_categories("LIMITER_SERVICE_ACCOUNT_OVERRIDES").get(endpoint) or {}
        if isinstance(overrides, dict) and category in overrides:
            return overrides[category]
    return _load_categories("LIMITER_SERVICE_ACCOUNT_CATEGORIES").get(category)


def is_service_account():
    """True when the caller authenticated with a service account token.

    middle_auth_client populates flask.g.auth_user from the auth service's /user/cache response,
    which carries `service_account`. Auth runs before the limiter -- flask_restx applies
    method_decorators first-to-innermost, so the order is reset_auth -> auth -> limiter -- which
    is why this can be read here at all.

    Keyed on the flag rather than on specific user ids so that swapping in a differently-scoped
    service account token needs no code change.
    """
    user = getattr(g, "auth_user", None)
    if not isinstance(user, dict):
        return False
    return bool(user.get("service_account"))


def _user_key():
    return g.auth_user["id"]


def limit_by_category(category, endpoint=None):
    """Rate limit an endpoint, with an optional higher ceiling for service accounts.

    :param endpoint: name used to look up a per-endpoint service account override. Defaults to
        the category, which matches nothing unless configured, so omitting it is safe.

    When a service account limit is configured, two limits are registered and each is exempt
    when the other applies, so exactly one is ever enforced. Service accounts still get a real,
    enforced ceiling -- removing the limit entirely would just move the pressure onto the
    database.
    """
    limit = get_rate_limit_from_config(category)
    service_account_limit = get_service_account_rate_limit(category, endpoint)

    if limit is None and service_account_limit is None:
        return lambda x: x

    if service_account_limit is None:
        # Unchanged from the original behaviour.
        return limiter.limit(limit, key_func=_user_key)

    def decorator(func):
        func = limiter.limit(
            service_account_limit,
            key_func=_user_key,
            exempt_when=lambda: not is_service_account(),
        )(func)
        if limit is not None:
            func = limiter.limit(
                limit,
                key_func=_user_key,
                exempt_when=is_service_account,
            )(func)
        return func

    return decorator


limiter = Limiter(
    get_remote_address,
    storage_uri=os.environ.get("LIMITER_URI", "memory://"),
    default_limits=None,
)
