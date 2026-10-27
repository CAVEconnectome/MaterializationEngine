import flask

# Flask reuses an already active app context for a request instead of pushing its
# own, and middle_auth_client's auth_required skips authentication when
# flask.g.auth_token is already set. A context left pushed in a worker process
# (configure_app used to push one) makes flask.g outlive the request, and a request
# would then run as whoever the worker last authenticated.
AUTH_ATTRS = ("auth_token", "auth_user")


def clear_auth():
    for name in AUTH_ATTRS:
        flask.g.pop(name, None)


def init_reset_auth(app):
    """Clear any previous request's authentication before every request."""
    app.before_request(clear_auth)
