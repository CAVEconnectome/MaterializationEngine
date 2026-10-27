"""A request must never run as the previous request's user.

A pushed app context (configure_app used to leave one) is reused by every request in
the process, so flask.g carries over between requests; middle_auth_client trusts an
existing flask.g.auth_token. The permission lookup is replaced by a token -> user map.
"""

import flask
import pytest
from flask import Flask
from middle_auth_client import auth_required, auth_requires_permission
from middle_auth_client import decorators as mac

from materializationengine.blueprints.reset_auth import init_reset_auth

USERS = {
    "editor-token": {
        "id": 1,
        "email": "editor@example.com",
        "permissions_v2": {"minnie65": ["view", "edit"]},
    },
    "viewer-token": {
        "id": 2,
        "email": "viewer@example.com",
        "permissions_v2": {"minnie65": ["view"]},
    },
}


def make_app(reset):
    app = Flask(__name__)
    if reset:
        init_reset_auth(app)

    @app.route("/whoami")
    @auth_required
    def whoami():
        return flask.g.auth_user["email"]

    @app.route("/edit/<datastack_name>", methods=["POST"])
    @auth_requires_permission("edit", table_arg="datastack_name", dataset="minnie65")
    def edit(datastack_name):
        return flask.g.auth_user["email"]

    return app


@pytest.fixture
def pushed():
    """Push each app's context and leave it active across its requests, as
    configure_app used to; popped afterwards so other tests are unaffected."""
    contexts = []

    def push(app):
        ctx = app.app_context()
        ctx.push()
        contexts.append(ctx)
        return app.test_client()

    yield push
    for ctx in reversed(contexts):
        ctx.pop()


@pytest.fixture(autouse=True)
def fake_auth(monkeypatch):
    monkeypatch.setattr(mac, "AUTH_DISABLED", False)
    mac.setPermissionLookupOverride(lambda token: USERS.get(token))
    # programmatic access: a missing token is a 401, not a redirect to log in
    monkeypatch.setattr(mac, "is_programmatic_access", lambda: True)
    yield
    mac.setPermissionLookupOverride(None)


def bearer(token):
    return {"Authorization": f"Bearer {token}"}


class TestConfigureApp:
    def test_leaves_no_app_context_pushed(self):
        """A pushed context would be reused by every request in the process."""
        from materializationengine.config import configure_app

        app = configure_app(Flask("t"))
        assert not (flask.has_app_context() and flask.current_app._get_current_object() is app)


class TestResetAuth:
    def test_without_reset_a_request_inherits_the_previous_user(self, pushed):
        """The failure this guards against, shown on an app without the hook."""
        client = pushed(make_app(reset=False))
        assert client.get("/whoami", headers=bearer("editor-token")).data == b"editor@example.com"
        response = client.get("/whoami")
        assert response.status_code == 200
        assert response.data == b"editor@example.com"

    def test_no_token_after_an_authenticated_request_is_refused(self, pushed):
        client = pushed(make_app(reset=True))
        assert client.get("/whoami", headers=bearer("editor-token")).status_code == 200
        assert client.get("/whoami").status_code == 401

    def test_each_request_runs_as_its_own_user(self, pushed):
        client = pushed(make_app(reset=True))
        assert client.get("/whoami", headers=bearer("editor-token")).data == b"editor@example.com"
        assert client.get("/whoami", headers=bearer("viewer-token")).data == b"viewer@example.com"

    def test_permission_follows_the_request_token(self, pushed):
        client = pushed(make_app(reset=True))
        assert client.post("/edit/minnie65_phase3_v1", headers=bearer("viewer-token")).status_code == 403
        assert client.post("/edit/minnie65_phase3_v1", headers=bearer("editor-token")).status_code == 200
        assert client.post("/edit/minnie65_phase3_v1", headers=bearer("viewer-token")).status_code == 403
