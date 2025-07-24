import json
import logging
from datetime import date, datetime, timedelta

import numpy as np
from dynamicannotationdb.models import Base, AnalysisVersion
from flask import Blueprint, Flask, current_app, jsonify, redirect, url_for
from flask_cors import CORS
from flask_restx import Api
from flask_sqlalchemy import SQLAlchemy

from materializationengine import __version__
from materializationengine.admin import setup_admin
from materializationengine.blueprints.client.api import client_bp
from materializationengine.blueprints.client.api2 import client_bp as client_bp2
from materializationengine.blueprints.materialize.api import mat_bp
from materializationengine.blueprints.upload.api import upload_bp, spatial_lookup_bp
from materializationengine.blueprints.upload.storage import StorageService
from materializationengine.blueprints.upload.models import init_staging_database
from materializationengine.config import config, configure_app
from materializationengine.database import db_manager
from materializationengine.schemas import ma
from materializationengine.utils import get_instance_folder_path
from materializationengine.views import views_bp
from materializationengine.limiter import limiter
from materializationengine.migrate import migrator
from materializationengine.request_db import init_request_db_cleanup

db = SQLAlchemy(model_class=Base)


class AEEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, np.int64):
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
    CORS(app, expose_headers=["WWW-Authenticate", "column_names"])
    
    app.json_encoder = AEEncoder
    app.config["RESTX_JSON"] = {"cls": AEEncoder}

    # load configuration (from test_config if passed)
    if config_name:
        app.config.from_object(config[config_name])
    else:
        app = configure_app(app)
    logging.basicConfig(level=app.config['LOGGING_LEVEL'])
    # Initialize request-scoped database session cleanup
    init_request_db_cleanup(app)
    
    # register blueprints
    apibp = Blueprint("api", __name__, url_prefix="/materialize")

    @apibp.route("/api/versions")
    def versions():
        return jsonify([2, 3]), 200

    @apibp.route("/version")
    def version():
        return jsonify(__version__), 200

    @apibp.route("/")
    def index():
        return redirect("/materialize/views/")

    app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
        'pool_pre_ping': True,
        'pool_recycle': 3600,
    }

    db.init_app(app)
    ma.init_app(app)

    # add the AppGroup to the app
    app.cli.add_command(migrator)

    with app.app_context():
        api = Api(
            apibp,
            title="Materialization Engine API",
            version=__version__,
            doc="/api/doc",
        )
        api.add_namespace(mat_bp, path="/api/v2")
        api.add_namespace(client_bp, path="/api/v2")
        api.add_namespace(client_bp2, path="/api/v3")
        api.add_namespace(spatial_lookup_bp, path="/api/v2")

        app.register_blueprint(apibp)
        app.register_blueprint(views_bp)
        app.register_blueprint(upload_bp)
        limiter.init_app(app)
        try:
            db.create_all()
        except Exception as e:
            app.logger.error(f"Error creating database tables: {e}")
            
        admin = setup_admin(app, db)
        if app.config.get("STAGING_DATABASE_NAME"):
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
        with db_manager.session_scope(aligned_volume) as session:
            n_versions = session.query(AnalysisVersion).count()
        
        return jsonify({aligned_volume: n_versions}), 200

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        db_manager.cleanup()
    return app
