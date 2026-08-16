"""Rate limits, and the optional higher ceiling for service accounts.

Two facts drive these tests:

* limit_by_category passes no `scope` to flask_limiter, so the counter bucket is
  (key_func value, endpoint) -- see Limit.scope_for. The limit string is chosen by category,
  but the bucket is the endpoint. An internal consumer funnelling through one endpoint therefore
  exhausts one bucket regardless of replica count.
* middle_auth_client puts `service_account` on flask.g.auth_user, and auth runs before the
  limiter (flask_restx applies method_decorators first-to-innermost), so the limiter can read it.

Verified against production 2026-08-16: the SkeletonService worker token is user id 1174,
service_account=True.
"""

import json

import flask
import pytest

from materializationengine import limiter as limiter_module
from materializationengine.limiter import (
    get_rate_limit_from_config,
    get_service_account_rate_limit,
    is_service_account,
    limit_by_category,
    limiter,
)

CATEGORIES = {"query": "800/minute", "fast_query": "5000/minute"}


@pytest.fixture
def env(monkeypatch):
    def _set(**pairs):
        for key, value in pairs.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value if isinstance(value, str) else json.dumps(value))

    _set(
        LIMITER_CATEGORIES=CATEGORIES,
        LIMITER_SERVICE_ACCOUNT_CATEGORIES=None,
        LIMITER_SERVICE_ACCOUNT_OVERRIDES=None,
    )
    return _set


@pytest.fixture
def app():
    application = flask.Flask(__name__)
    application.config["RATELIMIT_ENABLED"] = True
    limiter.init_app(application)
    return application


def _as(app, user):
    """Request context with flask.g.auth_user set, as middle_auth_client would."""
    ctx = app.test_request_context("/")
    ctx.push()
    flask.g.auth_user = user
    return ctx


SERVICE_ACCOUNT = {"id": 1174, "service_account": True, "name": "minniev2_pcg"}
HUMAN = {"id": 9168, "service_account": False, "name": "someone"}


class TestServiceAccountDetection:
    def test_true_for_a_service_account(self, app):
        with app.test_request_context("/"):
            flask.g.auth_user = SERVICE_ACCOUNT
            assert is_service_account() is True

    def test_false_for_a_human(self, app):
        with app.test_request_context("/"):
            flask.g.auth_user = HUMAN
            assert is_service_account() is False

    def test_false_when_the_flag_is_absent(self, app):
        """Older auth responses may omit it; absence must not grant the higher tier."""
        with app.test_request_context("/"):
            flask.g.auth_user = {"id": 5}
            assert is_service_account() is False

    def test_false_when_unauthenticated(self, app):
        with app.test_request_context("/"):
            assert is_service_account() is False


class TestLimitSelection:
    def test_normal_limit_comes_from_categories(self, env):
        assert get_rate_limit_from_config("query") == "800/minute"

    def test_unknown_category_is_unlimited(self, env):
        assert get_rate_limit_from_config("nope") is None

    def test_no_service_account_limit_by_default(self, env):
        """Unconfigured must mean unchanged behaviour."""
        assert get_service_account_rate_limit("query", "LiveTableQuery") is None

    def test_category_wide_service_account_limit(self, env):
        env(LIMITER_SERVICE_ACCOUNT_CATEGORIES={"query": "20000/minute"})

        assert get_service_account_rate_limit("query", "LiveTableQuery") == "20000/minute"
        assert get_service_account_rate_limit("fast_query", "Whatever") is None

    def test_per_endpoint_override_wins_over_the_category(self, env):
        env(
            LIMITER_SERVICE_ACCOUNT_CATEGORIES={"query": "9000/minute"},
            LIMITER_SERVICE_ACCOUNT_OVERRIDES={"LiveTableQuery": {"query": "20000/minute"}},
        )

        assert get_service_account_rate_limit("query", "LiveTableQuery") == "20000/minute"

    def test_override_does_not_leak_to_other_endpoints(self, env):
        """The point of per-endpoint scoping: other endpoints stay protected."""
        env(LIMITER_SERVICE_ACCOUNT_OVERRIDES={"LiveTableQuery": {"query": "20000/minute"}})

        assert get_service_account_rate_limit("query", "SomeOtherEndpoint") is None
        assert get_service_account_rate_limit("query", None) is None

    @pytest.mark.parametrize("bad", ["not json", "[]", '"a string"'])
    def test_malformed_config_is_ignored_rather_than_crashing(self, env, bad):
        env(LIMITER_SERVICE_ACCOUNT_CATEGORIES=bad)

        assert get_service_account_rate_limit("query", "LiveTableQuery") is None


class TestEnforcement:
    """End to end through flask_limiter, counting real requests against real buckets."""

    def test_human_is_limited_at_the_normal_rate(self, app, env):
        """A tiny limit makes the boundary observable without 800 requests."""
        env(LIMITER_CATEGORIES={"query": "2/minute"})

        @limit_by_category("query", endpoint="LiveTableQuery")
        def view():
            return "ok"

        app.add_url_rule("/q", "LiveTableQuery", view)

        @app.before_request
        def _auth():
            flask.g.auth_user = HUMAN

        client = app.test_client()
        codes = [client.get("/q").status_code for _ in range(4)]
        assert codes == [200, 200, 429, 429], codes

    def test_service_account_gets_the_elevated_ceiling(self, app, env):
        env(
            LIMITER_CATEGORIES={"query": "2/minute"},
            LIMITER_SERVICE_ACCOUNT_OVERRIDES={"LiveTableQuery": {"query": "5/minute"}},
        )

        @limit_by_category("query", endpoint="LiveTableQuery")
        def view():
            return "ok"

        app.add_url_rule("/q", "LiveTableQuery", view)

        @app.before_request
        def _auth():
            flask.g.auth_user = SERVICE_ACCOUNT

        client = app.test_client()
        codes = [client.get("/q").status_code for _ in range(7)]
        assert codes[:5] == [200] * 5, codes
        assert codes[5:] == [429, 429], codes

    def test_service_account_is_still_bounded(self, app, env):
        """Elevated is not unlimited -- the database still needs a backstop."""
        env(
            LIMITER_CATEGORIES={"query": "1/minute"},
            LIMITER_SERVICE_ACCOUNT_CATEGORIES={"query": "3/minute"},
        )

        @limit_by_category("query", endpoint="LiveTableQuery")
        def view():
            return "ok"

        app.add_url_rule("/q", "LiveTableQuery", view)

        @app.before_request
        def _auth():
            flask.g.auth_user = SERVICE_ACCOUNT

        client = app.test_client()
        codes = [client.get("/q").status_code for _ in range(5)]
        assert 429 in codes, codes

    def test_unconfigured_service_account_gets_the_normal_limit(self, app, env):
        """No override configured => a service account is treated like anyone else."""
        env(LIMITER_CATEGORIES={"query": "2/minute"})

        @limit_by_category("query", endpoint="LiveTableQuery")
        def view():
            return "ok"

        app.add_url_rule("/q", "LiveTableQuery", view)

        @app.before_request
        def _auth():
            flask.g.auth_user = SERVICE_ACCOUNT

        client = app.test_client()
        codes = [client.get("/q").status_code for _ in range(4)]
        assert codes == [200, 200, 429, 429], codes
