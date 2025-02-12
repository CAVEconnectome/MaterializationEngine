import json
import logging
from datetime import date, datetime, timedelta

import numpy as np
import redis
from dynamicannotationdb.models import AnalysisVersion, Base
from flask import Blueprint, Flask, current_app, jsonify, redirect
from flask_cors import CORS
from flask_restx import Api
from flask_session import Session
from flask_sqlalchemy import SQLAlchemy

from materializationengine import __version__
from materializationengine.admin import setup_admin
from materializationengine.blueprints.client.api import client_bp
from materializationengine.blueprints.client.api2 import client_bp as client_bp2
from materializationengine.blueprints.materialize.api import mat_bp
from materializationengine.blueprints.upload.api import upload_bp
from materializationengine.blueprints.upload.storage import StorageService
from materializationengine.blueprints.upload.models import init_staging_database
from materializationengine.config import config, configure_app
from materializationengine.database import sqlalchemy_cache
from materializationengine.limiter import limiter
from materializationengine.migrate import migrator
from materializationengine.schemas import ma
from materializationengine.utils import get_instance_folder_path
from materializationengine.views import views_bp

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
    Session(app)
    

    app.config.update(
        SESSION_TYPE='redis',
        SESSION_REDIS=redis.Redis(
            host=app.config["REDIS_HOST"],
            port=app.config["REDIS_PORT"],
            db=app.config["REDIS_SESSION_DB"],
            password=app.config["REDIS_PASSWORD"]
        ),
        PERMANENT_SESSION_LIFETIME=timedelta(hours=24),
        SESSION_KEY_PREFIX='upload_wizard_'
    )
    app.config.update(
        SESSION_COOKIE_SECURE=True,
        SESSION_COOKIE_SAMESITE='Lax',
        SESSION_COOKIE_HTTPONLY=True
    )


    # register blueprints
    apibp = Blueprint("api", __name__, url_prefix="/materialize")

    @apibp.route("/api/versions")
    def versions():
        return jsonify([2, 3]), 200

    @apibp.route("/version")
    def version():
        return jsonify(__version__), 200

    db.init_app(app)
    ma.init_app(app)

    # add the AppGroup to the app
    app.cli.add_command(migrator)

    with app.app_context():
        api = Api(
            apibp, title="Materialization Engine API", version=__version__, doc="/api/doc"
        )
        api.add_namespace(mat_bp, path="/api/v2")
        api.add_namespace(client_bp, path="/api/v2")
        api.add_namespace(client_bp2, path="/api/v3")

        app.register_blueprint(apibp)
        app.register_blueprint(views_bp)
        app.register_blueprint(upload_bp)
        limiter.init_app(app)

        db.init_app(app)
        db.create_all()
        admin = setup_admin(app, db)
        init_staging_database(app)
        # setup cors on upload bucket
        try:
            bucket_name = app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
            storage_service = StorageService(bucket_name)
            storage_service.configure_cors()
        except Exception as e:
            app.logger.error(f"Error setting up CORS configuration: {e}")

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
