import json
import logging
import os
import sys
from datetime import timedelta

from flask import Flask
from flask.logging import default_handler


class BaseConfig:
    ENV = "base"
    HOME = os.path.expanduser("~")
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    TESTING = False
    LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
    LOGGING_LOCATION = HOME + "/.materializationengine/bookshelf.log"
    LOGGING_LEVEL = logging.DEBUG
    SQLALCHEMY_DATABASE_URI = "sqlite://"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_URL = "redis://"
    CELERY_BROKER_URL = "memory://"
    RATELIMIT_STORAGE_URI = "memory://"
    CELERY_RESULT_BACKEND = REDIS_URL
    LOCAL_SERVER_URL = os.environ.get("LOCAL_SERVER_URL")
    GLOBAL_SERVER_URL = "https://global.daf-apis.com"
    ANNO_ENDPOINT = f"{LOCAL_SERVER_URL}/annotation/"
    INFOSERVICE_ENDPOINT = f"{GLOBAL_SERVER_URL}/info"
    AUTH_URI = f"{GLOBAL_SERVER_URL}/auth"
    SCHEMA_SERVICE_ENDPOINT = f"{GLOBAL_SERVER_URL}/schema/"
    SEGMENTATION_ENDPOINT = f"{GLOBAL_SERVER_URL}/segmentation"
    MASTER_NAME = os.environ.get("MASTER_NAME", None)
    MATERIALIZATION_ROW_CHUNK_SIZE = 500
    QUERY_LIMIT_SIZE = 200000
    QUEUE_LENGTH_LIMIT = 10000
    QUEUES_TO_THROTTLE = ["process"]
    THROTTLE_QUEUES = True
    CELERY_WORKER_IP = os.environ.get("CELERY_WORKER_IP", "127.0.0.1")
    DATASTACKS = ["minnie65_phase3_v1"]
    DAYS_TO_EXPIRE = 7
    LTS_DAYS_TO_EXPIRE = 30
    INFO_API_VERSION = 2
    MIN_DATABASES = 2
    MAX_DATABASES = 2
    MERGE_TABLES = True
    AUTH_SERVICE_NAMESPACE = "datastack"

    REDIS_HOST="localhost"
    REDIS_PORT=6379
    REDIS_PASSWORD=""
    SESSION_TYPE = "redis"
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    SESSION_PREFIX = "annotation_upload_"
    SESSION_USE_SIGNER = True
    REDIS_SESSION_DB = (
        1  # Redis DB for session storage, separate from the celery redis DB
    )

    STAGING_DATABASE_NAME = "staging"
    MATERIALIZATION_DUMP_BUCKET = "test_annotation_csv_upload"

    if os.environ.get("DAF_CREDENTIALS", None) is not None:
        with open(os.environ.get("DAF_CREDENTIALS"), "r") as f:
            AUTH_TOKEN = json.load(f)["token"]
    else:
        AUTH_TOKEN = ""

    DB_CONNECTION_POOL_SIZE = 5
    DB_CONNECTION_MAX_OVERFLOW = 5

    BEAT_SCHEDULES = [
        {
            "name": "Materialized Database Daily (2 Days)",
            "minute": 10,
            "hour": 8,
            "day_of_week": [0, 2, 4, 6],
            "task": "run_daily_periodic_materialization",
        },
        {
            "name": "Materialize Specific Database Daily",
            "minute": 10,
            "hour": 8,
            "day_of_week": [0, 2, 4, 6],
            "task": "run_periodic_materialization",
            "datastack_params": {
                "days_to_expire": 2,
                "merge_tables": False,
                "datastack": "minnie65_phase3_v1",
            },
        },
        {
            "name": "Materialized Database Daily (2 Days) (Wednesdays)",
            "minute": 10,
            "hour": 8,
            "day_of_week": 3,
            "day_of_month": "8-14,22-31",
            "task": "run_periodic_materialization",
            "datastack_params": {
                "days_to_expire": 2,
                "merge_tables": False,
                "datastack": "minnie65_phase3_v1",
            },
        },
        {
            "name": "Materialized Database Weekly (7 Days)",
            "minute": 10,
            "hour": 8,
            "day_of_week": [1, 5],
            "task": "run_periodic_materialization",
            "datastack_params": {
                "days_to_expire": 7,
            },
        },
        {
            "name": "Long Term Support Materialized Database (30 days)",
            "minute": 10,
            "hour": 8,
            "day_of_week": 3,
            "day_of_month": "1-7,15-21",
            "task": "run_periodic_materialization",
        },
        {
            "name": "Remove Expired Databases (Midnight)",
            "minute": 0,
            "hour": 8,
            "task": "remove_expired_databases",
            "datastack_params": {"delete_threshold": 5},
        },
        {
            "name": "Update Live Database",
            "minute": 0,
            "hour": "0-1,17-23",
            "day_of_week": "1-5",
            "task": "run_periodic_database_update",
        },
    ]


class DevConfig(BaseConfig):
    ENV = "development"
    # DEBUG = True
    SQLALCHEMY_DATABASE_URI = "postgres://postgres:materialize@db:5432/materialize"
    REDIS_HOST = os.environ.get("REDIS_HOST")
    REDIS_PORT = os.environ.get("REDIS_PORT")
    REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD")
    REDIS_URL = f"redis://:${REDIS_PASSWORD}@${REDIS_HOST}:${REDIS_PORT}/0"
    CELERY_BROKER_URL = REDIS_URL
    CELERY_RESULT_BACKEND = REDIS_URL
    USE_SENTINEL = os.environ.get("USE_SENTINEL", False)


class TestConfig(BaseConfig):
    ENV = "testing"
    TESTING = True
    SQLALCHEMY_DATABASE_URI = (
        "postgresql://postgres:postgres@localhost:5432/test_aligned_volume"
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    CELERY_BROKER_URL = "memory://"
    CELERY_RESULT_BACKEND = "redis://"
    MATERIALIZATION_ROW_CHUNK_SIZE = 2


class ProductionConfig(BaseConfig):
    ENV = "production"
    LOGGING_LEVEL = logging.INFO
    CELERY_BROKER = os.environ.get("REDIS_URL")
    CELERY_RESULT_BACKEND = os.environ.get("REDIS_URL")
    REDIS_URL = os.environ.get("REDIS_URL")


config = {
    "default": "materializationengine.config.BaseConfig",
    "development": "materializationengine.config.DevConfig",
    "testing": "materializationengine.config.TestConfig",
    "production": "materializationengine.config.ProductionConfig",
}


def configure_app(app: Flask) -> Flask:
    config_name = os.getenv("FLASK_CONFIGURATION", "default")
    # object-based default configuration
    app.config.from_object(config[config_name])
    if "MATERIALIZATION_ENGINE_SETTINGS" in os.environ.keys():
        app.config.from_envvar("MATERIALIZATION_ENGINE_SETTINGS")
    # instance-folders configuration
    app.config.from_pyfile("config.cfg", silent=True)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(app.config["LOGGING_LEVEL"])
    app.logger.removeHandler(default_handler)
    app.logger.addHandler(handler)
    app.logger.setLevel(app.config["LOGGING_LEVEL"])
    app.logger.propagate = False
    app.logger.debug(app.config)
    app.app_context().push()
    return app
