"""
Periodically clean up expired materialized databases.
"""
import itertools
from datetime import datetime
from typing import List

from celery.utils.log import get_task_logger
from dynamicannotationdb.models import AnalysisVersion
from materializationengine.celery_init import celery
from materializationengine.database import db_manager
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
    """Remove expired databases and clean up their metadata.
    
    Args:
        delete_threshold (int): Minimum number of databases to maintain
        datastack (str, optional): Specific datastack to clean, or all if None
        
    Returns:
        list: Information about dropped databases per datastack
    """
    aligned_volume_databases = get_aligned_volumes_databases()
    datastacks = [datastack] if datastack else get_config_param("DATASTACKS")
    current_time = datetime.utcnow()
    remove_db_cron_info = []

    for datastack in datastacks:
        try:
            datastack_info = get_datastack_info(datastack)
            aligned_volume = datastack_info["aligned_volume"]["name"]
            
            if aligned_volume not in aligned_volume_databases:
                continue

            with db_manager.session_scope(aligned_volume) as session:
                # Get expired and non-valid databases
                expired_results = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.expires_on <= current_time)
                    .order_by(AnalysisVersion.time_stamp)
                    .all()
                )
                latest_valid_version_row = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.valid == True)
                    .order_by(AnalysisVersion.time_stamp.desc())
                    .first()
                )
                
                
                if latest_valid_version_row:
                    expired_results_ids = [version.id for version in expired_results]
                    if latest_valid_version_row.id in expired_results_ids:
                        celery_logger.warning(
                            f"Latest valid version {latest_valid_version_row} is in expired list, "
                            "removing it from the expired versions."
                        )
                        # then we want to remove the latest_valid_version from the expired list
                        expired_results = [
                            version for version in expired_results 
                            if version.id != latest_valid_version_row.id
                        ]
                expired_versions = [str(expired_db) for expired_db in expired_results]
                non_valid_results = (
                    session.query(AnalysisVersion)
                    .filter(AnalysisVersion.valid == False)
                    .order_by(AnalysisVersion.time_stamp)
                    .all()
                ) # some databases might have failed to materialize completely but are still present on disk
                
                non_valid_versions = [str(non_valid_db) for non_valid_db in non_valid_results]
                versions_to_remove = list(set(expired_versions + non_valid_versions))

                # Get existing databases
                engine = db_manager.get_engine(aligned_volume)
                database_list = get_existing_databases(engine)
                databases = [db for db in database_list if db.startswith(datastack)]
                
                # get databases to delete that are currently present (ordered by timestamp)
                databases_to_delete = [
                    db for db in versions_to_remove if db in databases
                ]

                dropped_dbs = []

                if len(databases) > delete_threshold:
                    with engine.connect() as conn:
                        conn.execution_options(isolation_level="AUTOCOMMIT")
                        
                        for database in databases_to_delete:
                            # see if any materialized databases exist
                            existing_databases = get_existing_databases(engine)
                            mat_versions = get_all_versions(session)
                            remaining_databases = set(mat_versions).intersection(existing_databases)
                            # double check to see if there is only one valid db remaining
                            valid_versions = get_valid_versions(session)
                            remaining_valid_databases = set(valid_versions).intersection(existing_databases)

                            # Stop if we're at minimum database threshold
                            if (len(remaining_databases) == 1 or 
                                len(remaining_valid_databases) == 1):
                                celery_logger.info(
                                    f"Only one materialized database remaining: {database}, "
                                    "removal stopped."
                                )
                                break

                            if len(remaining_databases) == delete_threshold:
                                break
                            
                            if (len(databases) - len(dropped_dbs)) > delete_threshold:
                                try:
                                    exists_query = f"SELECT 1 FROM pg_database WHERE datname='{database}'"
                                    result = conn.execute(exists_query).scalar()
                                    
                                    if not result:
                                        continue

                                    celery_logger.info(f"Preparing to drop database: {database}")

                                    drop_connections = f"""
                                        SELECT pg_terminate_backend(pid) 
                                        FROM pg_stat_activity
                                        WHERE datname = '{database}'
                                        AND pid <> pg_backend_pid()
                                    """
                                    conn.execute(drop_connections)
                                    celery_logger.info(f"Dropped connections to: {database}")

                                    # Drop the database
                                    conn.execute(f"DROP DATABASE {database}")
                                    celery_logger.info(f"Database dropped: {database}")

                                    # Update version metadata
                                    database_version = database.rsplit("__mat")[-1]
                                    expired_db = (
                                        session.query(AnalysisVersion)
                                        .filter(AnalysisVersion.datastack == datastack)
                                        .filter(AnalysisVersion.version == database_version)
                                        .one()
                                    )
                                    expired_db.valid = False
                                    expired_db.status = "EXPIRED"
                                    
                                    dropped_dbs.append(database)
                                    celery_logger.info(f"Successfully removed database: {database}")

                                except Exception as e:
                                    celery_logger.error(
                                        f"Failed to remove database {database}: {e}"
                                    )
                                    continue

                remove_db_cron_info.append(dropped_dbs)

        except Exception as e:
            celery_logger.error(f"Error processing datastack {datastack}: {e}")
            continue

    return remove_db_cron_info