"""
Periodically clean up expired materialized databases.
"""
import itertools
from datetime import datetime
from typing import List

from celery.utils.log import get_task_logger
from dynamicannotationdb.models import AnalysisVersion
from materializationengine.celery_init import celery
from materializationengine.database import create_session
from materializationengine.info_client import get_aligned_volumes, get_datastack_info
from materializationengine.utils import get_config_param
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url

celery_logger = get_task_logger(__name__)


def get_aligned_volumes_databases():
    aligned_volumes = get_aligned_volumes()
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]

    engine = create_engine(sql_base_uri)
    with engine.connect() as connection:
        result = connection.execute("SELECT datname FROM pg_database;")
        databases = [database[0] for database in result]
    aligned_volume_databases = list(set(aligned_volumes).intersection(databases))
    return aligned_volume_databases


def get_existing_databases(engine) -> List:
    result = engine.execute("SELECT datname FROM pg_database;").fetchall()
    return list(itertools.chain.from_iterable(result))


def get_all_versions(session):
    versions = session.query(AnalysisVersion).all()
    return [str(version) for version in versions]


def get_valid_versions(session):
    valid_versions = (
        session.query(AnalysisVersion).filter(AnalysisVersion.valid == True).all()
    )
    return [str(version) for version in valid_versions]


@celery.task(name="workflow:remove_expired_databases")
def remove_expired_databases(delete_threshold: int = 5, datastack: str = None) -> str:
    """
    Remove expired database from time this method is called.
    """
    aligned_volume_databases = get_aligned_volumes_databases()

    datastacks = [datastack] if datastack else get_config_param("DATASTACKS")
    current_time = datetime.utcnow()
    remove_db_cron_info = []

    for datastack in datastacks:
        datastack_info = get_datastack_info(datastack)
        aligned_volume = datastack_info["aligned_volume"]["name"]
        if aligned_volume in aligned_volume_databases:
            SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
            sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
            sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
            session, engine = create_session(sql_uri)
            session.expire_on_commit = False
            # get number of expired dbs that are ready for deletion
            try:
                expired_results = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.expires_on <= current_time)
                    .order_by(AnalysisVersion.time_stamp)
                    .all()
                )
                expired_versions = [str(expired_db) for expired_db in expired_results]

                non_valid_results = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.valid == False)
                    .order_by(AnalysisVersion.time_stamp)
                    .all()
                ) # some databases might have failed to materialize completely but are still present on disk

                non_valid_versions = [str(non_valid_version) for non_valid_version in non_valid_results]
                versions = list(set(expired_versions + non_valid_versions))
            except Exception as sql_error:
                celery_logger.error(f"Error: {sql_error}")
                continue
            # get databases that exist currently, filter by materialized dbs
            database_list = get_existing_databases(engine)

            databases = [
                database for database in database_list if database.startswith(datastack)
            ]

            # get databases to delete that are currently present (ordered by timestamp)
            databases_to_delete = [
                database for database in versions if database in databases
            ]

            dropped_dbs = []

            if len(databases) > delete_threshold:
                with engine.connect() as conn:
                    conn.execution_options(isolation_level="AUTOCOMMIT")
                    for database in databases_to_delete:
                        # see if any materialized databases exist
                        existing_databases = get_existing_databases(engine)
                        mat_versions = get_all_versions(session)
                        remaining_databases = set(mat_versions).intersection(
                            existing_databases
                        )
                        # double check to see if there is only one valid db remaining
                        valid_versions = get_valid_versions(session)
                        remaining_valid_databases = set(valid_versions).intersection(
                            existing_databases
                        )
                        if (
                            len(remaining_databases) == 1
                            or len(remaining_valid_databases) == 1
                        ):
                            celery_logger.info(
                                f"Only one materialized database remaining: {database}, removal stopped."
                            )
                            break

                        if len(remaining_databases) == delete_threshold:
                            break

                        if (len(databases) - len(dropped_dbs)) > delete_threshold:
                            try:
                                sql = (
                                    "SELECT 1 FROM pg_database WHERE datname='%s'"
                                    % database
                                )
                                result_proxy = conn.execute(sql)
                                result = result_proxy.scalar()
                                celery_logger.info(
                                    f"Database to be dropped: {database} exists: {result}"
                                )
                                if result:
                                    drop_connections = f"""
                                    SELECT 
                                        pg_terminate_backend(pid) 
                                    FROM 
                                        pg_stat_activity
                                    WHERE 
                                        datname = '{database}'
                                    AND pid <> pg_backend_pid()
                                    """

                                    conn.execute(drop_connections)
                                    celery_logger.info(
                                        f"Dropped connections to: {database}"
                                    )
                                    sql = f"DROP DATABASE {database}"
                                    result_proxy = conn.execute(sql)
                                    celery_logger.info(f"Database: {database} removed")

                                    # strip version from database string
                                    database_version = database.rsplit("__mat")[-1]

                                    expired_database = (
                                        session.query(AnalysisVersion)
                                        .filter(
                                            AnalysisVersion.version == database_version
                                        )
                                        .one()
                                    )
                                    expired_database.valid = False
                                    expired_database.status = "EXPIRED"
                                    session.commit()
                                    celery_logger.info(
                                        f"Database '{expired_database}' dropped"
                                    )
                                    dropped_dbs.append(database)
                            except Exception as e:
                                celery_logger.error(
                                    f"ERROR: {e}: {database} does not exist"
                                )
            remove_db_cron_info.append(dropped_dbs)
            session.close()
    return remove_db_cron_info
