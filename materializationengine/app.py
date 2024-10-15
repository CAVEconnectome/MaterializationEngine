import json
import logging
from datetime import date, datetime

import numpy as np
from dynamicannotationdb.models import Base, AnalysisVersion
from flask import Blueprint, Flask, current_app, jsonify, redirect
from flask_cors import CORS
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy


from materializationengine import __version__
from materializationengine.admin import setup_admin
from materializationengine.blueprints.client.api import client_bp
from materializationengine.blueprints.client.api2 import client_bp as client_bp2
from materializationengine.blueprints.materialize.api import mat_bp
from materializationengine.config import config, configure_app
from materializationengine.database import sqlalchemy_cache
from materializationengine.schemas import ma
from materializationengine.utils import get_instance_folder_path
from materializationengine.views import views_bp
from materializationengine.limiter import limiter

db = SQLAlchemy(model_class=Base)


class AEEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def create_app(config_name: str = None):
    # Define the Flask Object
    app = Flask(
        __name__,
        static_folder="../static",
        instance_path=get_instance_folder_path(),
        static_url_path="/materialize/static",
        instance_relative_config=True,
        template_folder="../templates",
    )
    CORS(app, expose_headers="WWW-Authenticate")
    logging.basicConfig(level=logging.INFO)
    app.json_encoder = AEEncoder
    app.config["RESTX_JSON"] = {"cls": AEEncoder}

    # load configuration (from test_config if passed)
    if config_name:
        app.config.from_object(config[config_name])
    else:
        app = configure_app(app)
    # register blueprints

    apibp = Blueprint("api", __name__, url_prefix="/materialize")

    @apibp.route("/api/versions")
    def versions():
        return jsonify([2, 3]), 200

    @apibp.route("/version")
    def version():
        return __version__, 200

    db.init_app(app)
    ma.init_app(app)

    with app.app_context():
        api = Api(
            apibp, title="Materialization Engine API", version=__version__, doc="/doc"
        )
        api.add_namespace(mat_bp, path="/api/v2")
        api.add_namespace(client_bp, path="/api/v2")
        api.add_namespace(client_bp2, path="/api/v3")

        app.register_blueprint(apibp)
        app.register_blueprint(views_bp)
        limiter.init_app(app)

        db.init_app(app)
        db.create_all()
        admin = setup_admin(app, db)

    @app.route("/health")
    def health():
        aligned_volume = current_app.config.get("TEST_DB_NAME", "annotation")
        session = sqlalchemy_cache.get(aligned_volume)
        n_versions = session.query(AnalysisVersion).count()
        session.close()
        return jsonify({aligned_volume: n_versions}), 200

    @app.route("/materialize/")
    def index():
        return redirect("/materialize/views")

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        for key in sqlalchemy_cache._sessions:
            session = sqlalchemy_cache.get(key)
            session.remove()

    return app
