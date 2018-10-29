from .materialize_bp import bp as materialize_bp
from .utils import get_instance_folder_path
from .database import Base
from flask_sqlalchemy import SQLAlchemy
from .admin import setup_admin

__version__ = "0.0.1"


def create_app(test_config=None):
    from flask import Flask
    from materializationengine.config import configure_app

    # Define the Flask Object
    app = Flask(__name__,
                instance_path=get_instance_folder_path(),
                instance_relative_config=True)
    # load configuration (from test_config if passed)
    if test_config is None:
        app = configure_app(app)
    else:
        app.config.update(test_config)
    # register blueprints
    app.register_blueprint(materialize_bp)

    with app.app_context():
        db = SQLAlchemy(model_class=Base)
        db.init_app(app)
        db.create_all()
        admin = setup_admin(app, db)


    return app
