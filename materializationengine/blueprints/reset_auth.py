from functools import wraps

import flask


def reset_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "auth_token" in flask.g:
            del flask.g.auth_token
        if "auth_user" in flask.g:
            del flask.g.auth_user
        return f(*args, **kwargs)

    return decorated_function
