import datetime
import json
import logging
import pathlib
import time
import uuid

import docker
import psycopg2
import pytest
from dynamicannotationdb import DynamicAnnotationInterface
from dynamicannotationdb.models import Base

from materializationengine.app import create_app
from materializationengine.celery_worker import create_celery
from materializationengine.database import db_manager

test_logger = logging.getLogger(__name__)


def pytest_addoption(parser):
    parser.addoption(
        "--docker",
        action="store",
        default=False,
        help="Use docker for postgres testing",
    )


@pytest.fixture(scope="session")
def docker_mode(request):
    return request.config.getoption("--docker")


def pytest_configure(config):
    config.addinivalue_line("markers", "docker: use postgres in docker")


@pytest.fixture(scope="session")
def mat_metadata():
    p = pathlib.Path("tests/test_data", "mat_metadata.json")
    mat_dict = json.loads(p.read_text())
    mat_dict["materialization_time_stamp"] = str(
        datetime.datetime.utcnow() + datetime.timedelta(days=1)
    )
    return mat_dict


@pytest.fixture(scope="session")
def bulk_upload_metadata():
    p = pathlib.Path("tests/test_data", "bulk_upload_metadata.json")
    return json.loads(p.read_text())


@pytest.fixture(scope="session")
def annotation_data():
    p = pathlib.Path("tests/test_data", "annotation_data.json")
    return json.loads(p.read_text())


@pytest.fixture(scope="session")
def aligned_volume_name(mat_metadata):
    yield mat_metadata["aligned_volume"]


@pytest.fixture(scope="session")
def database_uri(mat_metadata):
    yield mat_metadata["sql_uri"]


# Setup Flask and Celery apps
@pytest.fixture(scope="session")
def test_app():
    flask_app = create_app(config_name="testing")
    test_logger.info("Starting test flask app...")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        # Establish an application context
        with flask_app.app_context():
            yield testing_client  #


@pytest.fixture(scope="session")
def test_celery_app(test_app):
    test_logger.info("Starting test celery worker...")
    celery = create_celery(test_app)
    yield celery


# Setup docker image if '--docker=True' in pytest args


@pytest.fixture(scope="session")
def setup_docker_image(docker_mode, mat_metadata):
    if docker_mode:
        postgis_docker_image = mat_metadata["postgis_docker_image"]
        aligned_volume = mat_metadata["aligned_volume"]

        db_environment = [
            "POSTGRES_USER=postgres",
            "POSTGRES_PASSWORD=postgres",
            f"POSTGRES_DB={aligned_volume}",
        ]

        try:
            test_logger.info(f"PULLING {postgis_docker_image} IMAGE")
            container_name = f"test_postgis_server_{uuid.uuid4()}"
            docker_client = docker.from_env()
            docker_client.images.pull(repository=postgis_docker_image)
            connection = docker_client.containers.run(
                image=postgis_docker_image,
                detach=True,
                hostname="test_postgres",
                auto_remove=True,
                name=container_name,
                environment=db_environment,
                ports={"5432/tcp": 5432},
            )
            test_logger.info("STARTING POSTGIS DOCKER IMAGE")
            time.sleep(10)
        except Exception as e:
            test_logger.exception(
                f"Failed to pull {postgis_docker_image} image. Error: {e}"
            )
    yield

    if docker_mode:
        container = docker_client.containers.get(container_name)
        container.stop()


# Setup PostGis Database with test data
@pytest.fixture(scope="session", autouse=True)
def setup_postgis_database(setup_docker_image, mat_metadata, annotation_data) -> None:
    aligned_volume = mat_metadata["aligned_volume"]
    sql_uri = mat_metadata["sql_uri"]

    try:
        is_connected = check_database(sql_uri)
        if not is_connected:
            test_logger.error(f"Could not connect to database {sql_uri}")
            yield False
            return

        is_setup = setup_database(aligned_volume, sql_uri)
        test_logger.info(
            f"DATABASE CAN BE REACHED: {is_connected}, DATABASE IS SETUP: {is_setup}"
        )
        test_logger.info(f"{aligned_volume} DATABASE IS NOW SETUP FOR TESTING...")

        table_info = add_annotation_table(mat_metadata)
        test_logger.info(f"ANNOTATION TABLE {table_info} CREATED")

        is_inserted = insert_test_data(mat_metadata, annotation_data)
        test_logger.info(f"IS TEST DATA INSERTED: {is_inserted}")
        yield True
    except Exception as e:
        test_logger.error(f"Cannot connect to database {sql_uri}: {e}")
        yield False


@pytest.fixture(scope="session")
def db_client(aligned_volume_name):
    with db_manager.session_scope(aligned_volume_name) as session:
        engine = db_manager.get_engine(aligned_volume_name)
        yield session, engine


@pytest.fixture(scope="session")
def mat_client(aligned_volume_name, database_uri):
    mat_client = DynamicAnnotationInterface(aligned_volume_name, database_uri)
    return mat_client


def check_database(sql_uri: str) -> bool:  # Changed return type hint
    try:
        test_logger.info("ATTEMPT TO CONNECT DB")
        conn = psycopg2.connect(sql_uri)
        cur = conn.cursor()
        cur.execute("SELECT 1")
        test_logger.info("CONNECTED TO DB")

        cur.close()
        conn.close()
        return True
    except Exception as e:
        test_logger.info(e)
        return False  # Explicitly return False on failure


def setup_database(aligned_volume_name, database_uri):
    mat_client = DynamicAnnotationInterface(database_uri, aligned_volume_name)
    base = Base
    base.metadata.bind = mat_client.database.engine
    base.metadata.create_all()
    return True


def add_annotation_table(mat_metadata: dict):
    aligned_volume_name = mat_metadata["aligned_volume"]
    database_uri = mat_metadata["sql_uri"]
    anno_client = DynamicAnnotationInterface(database_uri, aligned_volume_name)

    table_name = mat_metadata["annotation_table_name"]
    schema_type = mat_metadata["schema_type"]
    description = "Test synapse table"
    user_id = "foo@bar.com"
    vx = mat_metadata["voxel_resolution_x"]
    vy = mat_metadata["voxel_resolution_y"]
    vz = mat_metadata["voxel_resolution_z"]

    table_info = anno_client.annotation.create_table(
        table_name,
        schema_type,
        description,
        user_id,
        voxel_resolution_x=vx,
        voxel_resolution_y=vy,
        voxel_resolution_z=vz,
    )
    return table_info


def insert_test_data(mat_metadata: dict, annotation_data: dict):
    aligned_volume_name = mat_metadata["aligned_volume"]
    annotation_table_name = mat_metadata["annotation_table_name"]
    synapse_data = annotation_data["synapse_data"]
    schema_type = mat_metadata["schema_type"]

    segmentation_data = annotation_data["segmentation_data"]

    database_uri = mat_metadata["sql_uri"]
    pcg_name = mat_metadata["pcg_table_name"]

    anno_client = DynamicAnnotationInterface(database_uri, aligned_volume_name)

    data = anno_client.annotation.insert_annotations(
        annotation_table_name, synapse_data
    )
    test_logger.info(f"DATA INSERTED: {data}")
    is_created = anno_client.segmentation.create_segmentation_table(
        annotation_table_name, schema_type, pcg_name
    )
    return anno_client.segmentation.insert_linked_segmentation(
        annotation_table_name, pcg_name, segmentation_data
    )


def pytest_collection_modifyitems(session, config, items):
    """Modified from: https://stackoverflow.com/a/70758938
    In place reordering of class based tests to conform to dependencies
    in statefulness between tests"""

    # Follow how the workflow processes tasks Ingest > Update > Copy
    test_ordering = [
        "TestIngestMissingAnnotations",
        "TestSharedTasks",
        "TestUpdateRootIds",
        "TestCreateFrozenVersion",
    ]

    # filter items and remove non-class based tests
    class_mapping = {item: item.cls.__name__ for item in items if item.cls is not None}

    sorted_items = items.copy()

    # Iteratively move tests of each class to the end of the test queue
    for test_class in test_ordering:
        sorted_items = [
            it for it in sorted_items if class_mapping[it] != test_class
        ] + [it for it in sorted_items if class_mapping[it] == test_class]
    items[:] = sorted_items
