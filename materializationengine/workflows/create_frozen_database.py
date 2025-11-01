import datetime
import json
import os
from collections import OrderedDict
from typing import List

import pandas as pd
from celery import chain, chord
from celery.utils.log import get_task_logger
from dynamicannotationdb.models import (
    AnalysisTable,
    AnalysisVersion,
    AnnoMetadata,
    Base,
    MaterializedMetadata,
)
from emannotationschemas import get_schema
from emannotationschemas.flatten import create_flattened_schema
from emannotationschemas.models import (
    create_table_dict,
    make_flat_model,
    make_reference_annotation_model,
)
from psycopg2 import sql
from sqlalchemy import MetaData, create_engine, func
from sqlalchemy.engine import reflection
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError

from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.celery_init import celery
from materializationengine.database import (
    db_manager,
    dynamic_annotation_cache,
)
from materializationengine.errors import IndexMatchError
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import (
    add_index,
    fin,
    get_materialization_info,
    query_id_range,
)
from materializationengine.utils import (
    create_annotation_model,
    create_segmentation_model,
    get_config_param,
)

celery_logger = get_task_logger(__name__)


@celery.task(name="workflow:materialize_database")
def materialize_database(
    days_to_expire: int = 5, merge_tables: bool = True, **kwargs
) -> None:
    """
    Materialize database. Steps are as follows:
    1. Create new versioned database.
    2. Copy tables into versioned database
    3. Merge annotation and semgentation tables
    4. Re-index merged tables
    5. Check merge tables for row count and index consistency

    """
    try:
        datastacks = json.loads(os.environ["DATASTACKS"])
    except KeyError as e:
        celery_logger.error(f"KeyError: {e}")
        datastacks = get_config_param("DATASTACKS")
    for datastack in datastacks:
        try:
            celery_logger.info(f"Materializing {datastack} database")
            datastack_info = get_datastack_info(datastack)
            task = create_versioned_materialization_workflow.s(
                datastack_info, days_to_expire, merge_tables
            )
            task.apply_async(kwargs={"Datastack": datastack_info["datastack"]})
        except Exception as e:
            celery_logger.error(e)
            raise e
    return True


@celery.task(name="workflow:create_versioned_materialization_workflow")
def create_versioned_materialization_workflow(
    datastack_info: dict, days_to_expire: int = 5, merge_tables: bool = True, **kwargs
):
    """Create a timelocked database of materialization annotations
    and associated segmentation data.

    Parameters
    ----------
    aligned_volume : str
        aligned volumed relating to a datastack
    """
    materialization_time_stamp = datetime.datetime.utcnow()
    new_version_number = create_new_version(
        datastack_info=datastack_info,
        materialization_time_stamp=materialization_time_stamp,
        days_to_expire=days_to_expire,
        merge_tables=merge_tables,
    )

    mat_info = get_materialization_info(
        datastack_info, new_version_number, materialization_time_stamp
    )

    setup_versioned_database = create_materialized_database_workflow(
        datastack_info, new_version_number, materialization_time_stamp, mat_info
    )

    if merge_tables:
        format_workflow = format_materialization_database_workflow(mat_info)
        analysis_database_workflow = chain(
            chord(format_workflow, fin.s()), rebuild_reference_tables.si(mat_info)
        )

    else:
        analysis_database_workflow = fin.si()
    return chain(
        setup_versioned_database,
        analysis_database_workflow,
        check_tables.si(mat_info, new_version_number),
    )


def create_materialized_database_workflow(
    datastack_info: dict,
    new_version_number: int,
    materialization_time_stamp: datetime.datetime.utcnow,
    mat_info: List[dict],
):
    """Celery workflow to create a materialized database.

    Workflow:
        - Copy live database as a versioned materialized database.
        - Create materialization metadata table and populate.
        - Drop tables that are unneeded in the materialized database.

    Args:
        datastack_info (dict): database information
        new_version_number (int): version number of database
        materialization_time_stamp (datetime.datetime.utcnow):
            materialized timestamp
        mat_info (dict): materialization metadata information

    Returns:
        chain: chain of celery tasks
    """
    return chain(
        create_analysis_database.si(datastack_info, new_version_number),
        create_materialized_metadata.si(
            datastack_info, mat_info, new_version_number, materialization_time_stamp
        ),
        update_table_metadata.si(mat_info),
        drop_tables.si(mat_info, new_version_number),
    )


def format_materialization_database_workflow(mat_info: List[dict]):
    """Celery workflow to format the materialized database.

    Workflow:
        - Merge annotation and segmentation tables into a single table.
        - Add indexes into merged tables.

    Args:
        mat_info (dict): materialization metadata information

    Returns:
        chain: chain of celery tasks
    """
    create_frozen_database_tasks = []
    for mat_metadata in mat_info:
        if not mat_metadata[
            "reference_table"
        ]:  # need to build tables before adding reference tables with fkeys
            create_frozen_database_workflow = chain(
                merge_tables.si(mat_metadata), add_indices.si(mat_metadata)
            )
            create_frozen_database_tasks.append(create_frozen_database_workflow)
    return create_frozen_database_tasks


def clean_split_table_workflow(mat_info: List[dict]):
    """Remove rows from all tables that are past the materialization timestamp
    or deleted or are not valid.

    Workflow:
        - Iterate through all tables and drop non-valid rows

    Args:
        mat_info (dict): materialization metadata information

    Returns:
        chain: chain of celery tasks
    """
    clean_table_tasks = []
    for mat_metadata in mat_info:
        clean_table_workflow = chain(clean_table.si(mat_metadata))
        clean_table_tasks.append(clean_table_workflow)
    return clean_table_tasks


@celery.task(
    name="workflow:rebuild_reference_tables",
    bind=True,
    acks_late=True,
    autoretry_for=(OperationalError,),
    max_retries=3,
)
def rebuild_reference_tables(self, mat_info: List[dict]):
    reference_table_tasks = []
    for mat_metadata in mat_info:
        if mat_metadata["reference_table"]:
            if mat_metadata.get("segmentation_table_name"):
                reference_table_workflow = chain(
                    merge_tables.si(mat_metadata), add_indices.si(mat_metadata)
                )
            else:
                reference_table_workflow = add_indices.si(mat_metadata)
            reference_table_tasks.append(reference_table_workflow)

    if reference_table_tasks:
        return self.replace(chain(reference_table_tasks))
    else:
        return fin.si()


def create_new_version(
    datastack_info: dict,
    materialization_time_stamp: datetime.datetime.utcnow,
    days_to_expire: int = None,
    minus_hours: int = 1,
    merge_tables: bool = True,
):
    """Create new versioned database row in the analysis_version table.

    Args:
        datastack_info (dict): datastack info from infoservice
        materialization_time_stamp (datetime): UTC timestamp of root_id lookup
        days_to_expire (int, optional): Days until db is flagged to be expired
        minus_hours (int): Hours before days_to_expire to set expiration
        merge_tables (bool): Whether to merge tables

    Returns:
        int: Version number of materialized database
    """
    aligned_volume = datastack_info["aligned_volume"]["name"]
    datastack = datastack_info.get("datastack")

    table_objects = [AnalysisVersion.__tablename__, AnalysisTable.__tablename__]

    engine = db_manager.get_engine(aligned_volume)

    for table in table_objects:
        if not engine.dialect.has_table(engine, table):
            Base.metadata.tables[table].create(bind=engine)

    with db_manager.session_scope(aligned_volume) as session:
        top_version = session.query(func.max(AnalysisVersion.version)).scalar()
        new_version_number = 1 if top_version is None else top_version + 1

        if days_to_expire and days_to_expire > 0:
            expiration_date = (
                materialization_time_stamp
                + datetime.timedelta(days=days_to_expire)
                - datetime.timedelta(hours=minus_hours)
            )
        else:
            expiration_date = None

        analysisversion = AnalysisVersion(
            datastack=datastack,
            time_stamp=materialization_time_stamp,
            version=new_version_number,
            valid=False,
            expires_on=expiration_date,
            status="RUNNING",
            is_merged=merge_tables,
        )

        session.add(analysisversion)

        celery_logger.info(
            f"Created version {new_version_number} for {datastack}"
            f" (expires: {expiration_date})"
        )

    return new_version_number


@celery.task(
    name="workflow:create_analysis_database",
    bind=True,
    acks_late=True,
    autoretry_for=(OperationalError,),
    max_retries=3,
)
def create_analysis_database(self, datastack_info: dict, analysis_version: int) -> str:
    """Copies live database to new versioned database for materialized annotations.

    Args:
        datastack_info (dict): datastack metadata
        analysis_version (int): analysis database version number

    Raises:
        e: error if dropping table(s) fails.

    Returns:
        bool: True if analysis database creation is successful
    """

    aligned_volume = datastack_info["aligned_volume"]["name"]
    datastack = datastack_info["datastack"]
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    sql_base_uri = SQL_URI_CONFIG.rpartition("/")[0]
    sql_uri = make_url(f"{sql_base_uri}/{aligned_volume}")
    analysis_sql_uri = create_analysis_sql_uri(
        str(sql_uri), datastack, analysis_version
    )

    engine = create_engine(sql_uri, isolation_level="AUTOCOMMIT", pool_pre_ping=True)

    connection = engine.connect()
    connection.connection.set_session(autocommit=True)

    result = connection.execute(
        f"SELECT 1 FROM pg_catalog.pg_database \
                WHERE datname = '{analysis_sql_uri.database}'"
    )
    if not result.fetchone():
        try:
            # create new database from template_postgis database
            celery_logger.info(
                f"Creating new materialized database {analysis_sql_uri.database}"
            )

            drop_connections = f"""
            SELECT 
                pg_terminate_backend(pid) 
            FROM 
                pg_stat_activity
            WHERE 
                datname = '{aligned_volume}'
            AND pid <> pg_backend_pid()
            """

            connection.execute(drop_connections)

            connection.execute(
                f"""CREATE DATABASE {analysis_sql_uri.database} 
                    WITH TEMPLATE {aligned_volume}"""
            )
            # lets reconnect
            try:
                connection = engine.connect()
                # check if database exists
                db_result = connection.execute(
                    f"SELECT 1 FROM pg_catalog.pg_database \
                        WHERE datname = '{analysis_sql_uri.database}'"
                )
                db_result.fetchone()
            except Exception as e:
                celery_logger.error(f"Connection was lost: {e}")

        except OperationalError as sql_error:
            celery_logger.error(f"ERROR: {sql_error}")
            raise self.retry(exc=sql_error, countdown=3)
        finally:
            # invalidate caches since we killed connections to the live db
            dynamic_annotation_cache.invalidate_cache()
            db_manager.cleanup()

    connection.close()
    engine.dispose()
    return True


@celery.task(
    name="workflow:create_materialized_metadata",
    bind=True,
    acks_late=True,
)
def create_materialized_metadata(
    self,
    datastack_info: dict,
    mat_info: List[dict],
    analysis_version: int,
    materialization_time_stamp: datetime.datetime.utcnow,
):
    """Creates a metadata table in a materialized database. Reads row counts
    from annotation tables copied to the materialized database. Inserts row count
    and table info into the metadata table.

    Args:
        datastack_info (dict): Information about the datastack
        mat_info (List[dict]): List of dicts containing table metadata
        analysis_version (int): Version number for the analysis
        materialization_time_stamp (datetime): Timestamp for materialization

    Returns:
        bool: True if Metadata table was created and table info was inserted.
    """
    datastack = datastack_info["datastack"]
    analysis_database = f"{datastack}__mat{analysis_version}"

    try:
        with db_manager.session_scope(analysis_database) as session:
            engine = db_manager.get_engine(analysis_database)

            try:
                mat_table = MaterializedMetadata()
                mat_table.__table__.create(bind=engine)
            except Exception as e:
                celery_logger.error(f"Materialized Metadata table creation failed: {e}")
                raise

            for mat_metadata in mat_info:
                annotation_table_name = mat_metadata["annotation_table_name"]
                schema_type = mat_metadata["schema"]
                valid_row_count = mat_metadata.get("row_count")
                segmentation_source = mat_metadata.get("segmentation_source")
                merge_table = mat_metadata.get("merge_table")

                if valid_row_count == 0:
                    continue

                metadata_entry = MaterializedMetadata(
                    schema=schema_type,
                    table_name=annotation_table_name,
                    row_count=valid_row_count,
                    materialized_timestamp=materialization_time_stamp,
                    segmentation_source=segmentation_source,
                    is_merged=merge_table,
                )
                session.add(metadata_entry)

        return True

    except Exception as e:
        celery_logger.error(f"Failed to create materialized metadata: {e}")
        raise e


@celery.task(
    name="workflow:update_table_metadata",
    bind=True,
    acks_late=True,
)
def update_table_metadata(self, mat_info: List[dict]):
    """Update 'analysistables' with all the tables
    to be created in the frozen materialized database.

    Args:
        mat_info (List[dict]): list of dicts containing table metadata

    Returns:
        list: list of tables that were added to 'analysistables'
    """
    aligned_volume = mat_info[0]["aligned_volume"]
    version = mat_info[0]["analysis_version"]

    with db_manager.session_scope(aligned_volume) as session:
        tables = []
        for mat_metadata in mat_info:
            version_id = (
                session.query(AnalysisVersion.id)
                .filter(AnalysisVersion.version == version)
                .first()
            )
            analysis_table = AnalysisTable(
                aligned_volume=aligned_volume,
                schema=mat_metadata["schema"],
                table_name=mat_metadata["annotation_table_name"],
                valid=False,
                created=mat_metadata["materialization_time_stamp"],
                analysisversion_id=version_id,
            )
            tables.append(analysis_table.table_name)
            session.add(analysis_table)
    return tables


@celery.task(
    name="workflow:drop_tables",
    bind=True,
    acks_late=True,
)
def drop_tables(self, mat_info: List[dict], analysis_version: int):
    """Drop all tables that don't match valid in the live 'aligned_volume' database
    as well as tables that were copied from the live table that are not needed in
    the frozen version (e.g. metadata tables).

    Args:
        datastack_info (dict): datastack info for the aligned_volume from the infoservice
        analysis_version (int): materialized version number

    Raises:
        e: error if dropping table(s) fails.

    Returns:
        str: tables that have been dropped
    """
    datastack = mat_info[0]["datastack"]

    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    analysis_sql_uri = create_analysis_sql_uri(
        SQL_URI_CONFIG, datastack, analysis_version
    )

    mat_engine = create_engine(analysis_sql_uri)

    mat_inspector = reflection.Inspector.from_engine(mat_engine)
    mat_table_names = mat_inspector.get_table_names()
    mat_table_names.remove("materializedmetadata")

    annotation_tables = [table.get("annotation_table_name") for table in mat_info]
    segmentation_tables = [
        table.get("segmentation_table_name")
        for table in mat_info
        if table.get("segmentation_table_name") is not None
    ]

    filtered_tables = annotation_tables + segmentation_tables + ["analysisviews"]

    tables_to_drop = set(mat_table_names) - set(filtered_tables)
    tables_to_drop.remove("spatial_ref_sys")  # keep postgis spatial info table

    try:
        connection = mat_engine.connect()
        for table in tables_to_drop:
            drop_statement = f"DROP TABLE {table} CASCADE"
            connection.execute(drop_statement)
    except Exception as e:
        celery_logger.error(e)
        raise e
    finally:
        connection.close()
        mat_engine.dispose()
    tables_dropped = list(tables_to_drop)
    return f"Tables dropped {tables_dropped}"


@celery.task(
    name="workflow:insert_annotation_data",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def insert_annotation_data(self, chunk: List[int], mat_metadata: dict) -> bool:
    """Insert annotation data into database using connection manager.

    Args:
        chunk (List[int]): chunk of annotation ids
        mat_metadata (dict): materialized metadata
    """
    aligned_volume = mat_metadata["aligned_volume"]
    analysis_version = mat_metadata["analysis_version"]
    annotation_table_name = mat_metadata["annotation_table_name"]
    datastack = mat_metadata["datastack"]
    analysis_db = f"{datastack}__mat{analysis_version}"

    # Get models
    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=False)
    SegmentationModel = create_segmentation_model(mat_metadata)

    # Setup query columns
    query_columns = list(AnnotationModel.__table__.columns)
    query_columns.extend(
        col for col in SegmentationModel.__table__.columns if col.name != "id"
    )

    # Use source database session for query
    with db_manager.session_scope(aligned_volume) as source_session:
        # Query the data
        chunked_id_query = query_id_range(AnnotationModel.id, chunk[0], chunk[1])
        query = (
            source_session.query(*query_columns)
            .join(SegmentationModel)
            .filter(chunked_id_query)
            .filter(AnnotationModel.valid == True)
        )

        data = query.all()
        records = pd.DataFrame(data).to_dict(orient="records")

        # Insert into analysis database
        with db_manager.session_scope(analysis_db) as analysis_session:
            analysis_table = get_analysis_table(
                aligned_volume, datastack, annotation_table_name, analysis_version
            )
            analysis_session.execute(analysis_table.insert(), records)

    return True


@celery.task(
    name="workflow:merge_tables",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def merge_tables(self, mat_metadata: dict):
    """Merge all the annotation and segmentation rows into a new table that are
    flagged as valid. Drop the original split tables after inserting all the rows
    into the new table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume from the infoservice

    Raises:
        Exception: error during table merging operation

    Returns:
        str: number of rows copied
    """
    analysis_version = mat_metadata["analysis_version"]
    annotation_table_name = mat_metadata["annotation_table_name"]
    segmentation_table_name = mat_metadata["segmentation_table_name"]
    temp_table_name = mat_metadata["temp_mat_table_name"]
    schema = mat_metadata["schema"]
    datastack = mat_metadata["datastack"]
    mat_time_stamp = mat_metadata["materialization_time_stamp"]
    analysis_database = f"{datastack}__mat{analysis_version}"

    anno_schema = get_schema(schema)
    flat_schema = create_flattened_schema(anno_schema)
    ordered_model_columns = create_table_dict(
        table_name=annotation_table_name,
        Schema=flat_schema,
        segmentation_source=None,
        table_metadata={"reference_table": mat_metadata.get("reference_table")},
        with_crud_columns=False,
    )

    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=True)
    SegmentationModel = create_segmentation_model(mat_metadata)

    crud_columns = ["created", "deleted", "superceded_id"]
    query_columns = {
        col.name: col
        for col in AnnotationModel.__table__.columns
        if col.name not in crud_columns
    }

    for col in SegmentationModel.__table__.columns:
        if col.name != "id":
            query_columns[col.name] = col

    sorted_columns = OrderedDict(
        [
            (key, query_columns[key])
            for key in ordered_model_columns
            if key in query_columns
        ]
    )
    sorted_columns_list = list(sorted_columns.values())
    columns = [f'"{col.table}".{col.name}' for col in sorted_columns_list]

    celery_logger.debug(
        f"SORTED COLUMNS: {sorted_columns_list}, COLUMNS: {columns}, "
        f"ANNOTATION_COLS: {AnnotationModel.__table__.columns}"
    )

    query = f"""
        SELECT 
            {', '.join(columns)}
        FROM 
            {AnnotationModel.__table__.name}
        JOIN 
            "{SegmentationModel.__table__.name}"
            ON {AnnotationModel.id} = "{SegmentationModel.__table__.name}".id
        WHERE
            {AnnotationModel.id} = "{SegmentationModel.__table__.name}".id
        AND {AnnotationModel.created} <= '{mat_time_stamp}'
        AND {AnnotationModel.valid} = true
    """

    try:
        engine = db_manager.get_engine(analysis_database)
        with engine.connect() as connection:
            with connection.begin():
                # Create new merged table
                insert_result = connection.execute(
                    f"CREATE TABLE {temp_table_name} AS ({query});"
                )
                row_count = insert_result.rowcount

                # Drop original tables
                connection.execute(
                    f'DROP TABLE {annotation_table_name}, "{segmentation_table_name}" CASCADE;'
                )

                connection.execute(
                    f"ALTER TABLE {temp_table_name} RENAME TO {annotation_table_name};"
                )

        return f"Number of rows copied: {row_count}"

    except Exception as e:
        celery_logger.error(f"Failed to merge tables: {e}")
        raise


@celery.task(
    name="workflow:clean_table",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def clean_table(self, mat_metadata: dict):
    """Remove non-valid rows from a table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume from the infoservice

    Raises:
        Exception: error during row deletion operation

    Returns:
        str: number of rows deleted
    """
    analysis_version = mat_metadata["analysis_version"]
    datastack = mat_metadata["datastack"]
    mat_time_stamp = mat_metadata["materialization_time_stamp"]
    analysis_database = f"{datastack}__mat{analysis_version}"

    AnnotationModel = create_annotation_model(mat_metadata, with_crud_columns=True)

    try:
        with db_manager.session_scope(analysis_database) as session:
            non_valid_rows = session.query(AnnotationModel).filter(
                (AnnotationModel.created > mat_time_stamp)
                | (AnnotationModel.valid != True)
            )

            num_rows_to_delete = non_valid_rows.delete(synchronize_session=False)

            return f"Number of rows deleted: {num_rows_to_delete}"

    except Exception as e:
        celery_logger.error(f"Failed to clean table: {e}")
        raise


def insert_chunked_data(
    annotation_table_name: str,
    sql_statement: str,
    cur,
    engine,
    next_key: int,
    batch_size: int = 100_000,
):
    pagination_query = f"""AND
                {annotation_table_name}.id > {next_key}
            ORDER BY {annotation_table_name}.id ASC
            LIMIT {batch_size} RETURNING  {annotation_table_name}.id"""
    insert_statement = sql.SQL(sql_statement + pagination_query)

    try:
        cur.execute(insert_statement)
        engine.commit()
    except Exception as e:
        celery_logger.error(e)

    results = cur.fetchmany(batch_size)

    if len(results) < batch_size:
        return
    # Find highest returned uid in results to get next key
    next_key = results[-1][0]
    return insert_chunked_data(
        annotation_table_name, sql_statement, cur, engine, next_key
    )


@celery.task(
    name="workflow:set_version_status",
    bind=True,
    acks_late=True,
)
def set_version_status(self, mat_info: list, analysis_version: int, status: str):
    aligned_volume = mat_info[0]["aligned_volume"]  #
    with db_manager.session_scope(aligned_volume) as session:

        versioned_database = (
            session.query(AnalysisVersion)
            .filter(AnalysisVersion.version == analysis_version)
            .one()
        )
        if status == "AVAILABLE":
            versioned_database.valid = True
        else:
            versioned_database.valid = False

        versioned_database.status = status

    return f"Mat db version {analysis_version} to {status}"


@celery.task(
    name="workflow:check_tables",
    bind=True,
    acks_late=True,
)
def check_tables(self, mat_info: list, analysis_version: int) -> str:
    """Check materialized table validity by comparing row counts and indexes.

    Args:
        mat_info: List of dicts containing metadata for each materialized table
        analysis_version: The materialized version number

    Returns:
        str: Success message if all tables are valid

    Raises:
        ValueError: If row counts don't match or table counts are invalid
    """
    aligned_volume = mat_info[0]["aligned_volume"]
    table_count = len(mat_info)
    analysis_database = mat_info[0]["analysis_database"]
    valid_table_count = 0

    live_client = dynamic_annotation_cache.get_db(aligned_volume)
    mat_client = dynamic_annotation_cache.get_db(analysis_database)

    try:
        with db_manager.session_scope(aligned_volume) as session:
            versioned_database = (
                session.query(AnalysisVersion)
                .filter(AnalysisVersion.version == analysis_version)
                .one()
            )

            for mat_metadata in mat_info:
                merge_table = mat_metadata.get("merge_table")
                annotation_table_name = mat_metadata["annotation_table_name"]
                mat_timestamp = mat_metadata["materialization_time_stamp"]

                if merge_table:
                    live_count = live_client.database.get_table_row_count(
                        annotation_table_name,
                        filter_valid=True,
                        filter_timestamp=mat_timestamp,
                    )
                    mat_count = mat_client.database.get_table_row_count(
                        annotation_table_name, filter_valid=True
                    )

                    celery_logger.info(
                        f"Table {annotation_table_name} counts - "
                        f"Live: {live_count}, Materialized: {mat_count}"
                    )

                    if mat_count == 0:
                        celery_logger.warning(
                            f"Table {annotation_table_name} has no rows, skipping"
                        )
                        continue

                    if live_count != mat_count:
                        raise ValueError(
                            f"Row count mismatch for '{annotation_table_name}': "
                            f"Live ({aligned_volume}): {live_count}, "
                            f"Materialized ({analysis_database}): {mat_count}"
                        )

                    schema = mat_metadata["schema"]
                    table_metadata = (
                        {"reference_table": mat_metadata["reference_table"]}
                        if mat_metadata.get("reference_table")
                        else None
                    )

                    anno_model = make_flat_model(
                        table_name=annotation_table_name,
                        schema_type=schema,
                        table_metadata=table_metadata,
                    )

                    with db_manager.session_scope(analysis_database) as mat_session:
                        mat_engine = mat_session.get_bind()

                        live_indexes = index_cache.get_index_from_model(
                            annotation_table_name, anno_model, mat_engine
                        )
                        mat_indexes = index_cache.get_table_indices(
                            annotation_table_name, mat_engine
                        )

                        if live_indexes.keys() != mat_indexes.keys():
                            celery_logger.warning(
                                f"Index mismatch for {annotation_table_name}: "
                                f"Live: {live_indexes}, "
                                f"Materialized: {mat_indexes}"
                            )

                valid_table_count += 1
                table_validity = (
                    session.query(AnalysisTable)
                    .filter(AnalysisTable.analysisversion_id == versioned_database.id)
                    .filter(AnalysisTable.table_name == annotation_table_name)
                    .one()
                )
                table_validity.valid = True

            if valid_table_count != table_count:
                raise ValueError(
                    f"Table count mismatch: "
                    f"Valid: {valid_table_count}, Total: {table_count}"
                )

            return "All materialized tables match valid row number from live tables"

    except Exception as e:
        celery_logger.error(f"Table validation failed: {str(e)}")
        raise

    finally:
        mat_client.database.cached_session.close()


def create_analysis_sql_uri(sql_uri: str, datastack: str, mat_version: int):
    sql_base_uri = sql_uri.rpartition("/")[0]
    analysis_sql_uri = make_url(f"{sql_base_uri}/{datastack}__mat{mat_version}")
    return analysis_sql_uri


def get_analysis_table(
    aligned_volume: str, datastack: str, table_name: str, mat_version: int = 1
):
    """Helper method that returns a table model.

    Args:
        aligned_volume (str): aligned_volume name
        datastack (str): datastack name
        table_name (str): table to reflect a model
        mat_version (int, optional): target database version

    Returns:
        SQLAlchemy model: returns a sqlalchemy model of a target table
    """
    anno_db = dynamic_annotation_cache.get_db(aligned_volume)
    schema_name = anno_db.database.get_table_metadata(table_name)
    SQL_URI_CONFIG = get_config_param("SQLALCHEMY_DATABASE_URI")
    analysis_sql_uri = create_analysis_sql_uri(SQL_URI_CONFIG, datastack, mat_version)
    analysis_engine = create_engine(analysis_sql_uri)

    meta = MetaData()
    meta.reflect(bind=analysis_engine)

    anno_schema = get_schema(schema_name)
    flat_schema = create_flattened_schema(anno_schema)

    if not analysis_engine.dialect.has_table(analysis_engine, table_name):
        annotation_dict = create_table_dict(
            table_name=table_name,
            Schema=flat_schema,
            segmentation_source=None,
            table_metadata=None,
            with_crud_columns=False,
        )
        analysis_table = type(table_name, (Base,), annotation_dict)
    else:
        analysis_table = meta.tables[table_name]

    analysis_engine.dispose()
    return analysis_table


@celery.task(name="workflow:drop_indices", bind=True, acks_late=True)
def drop_indices(self, mat_metadata: dict):
    """Drop all indices of a given table.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice

    Returns:
        str: Message indicating whether indices were dropped
    """
    add_indices = mat_metadata.get("add_indices", False)
    if not add_indices:
        return "No indices dropped"

    analysis_version = mat_metadata.get("analysis_version")
    datastack = mat_metadata["datastack"]
    temp_mat_table_name = mat_metadata["temp_mat_table_name"]
    analysis_database = f"{datastack}__mat{analysis_version}"

    try:
        engine = db_manager.get_engine(analysis_database)

        index_cache.drop_table_indices(temp_mat_table_name, engine)

        return "Indices DROPPED"

    except Exception as e:
        celery_logger.error(f"Failed to drop indices: {e}")
        raise


@celery.task(name="workflow:add_indices", bind=True, acks_late=True)
def add_indices(self, mat_metadata: dict):
    """Find missing indices for a given table and create a chain of tasks to add them.

    Each index is added in its own task that runs synchronously within the chain.

    Args:
        mat_metadata (dict): datastack info for the aligned_volume derived from the infoservice

    Returns:
        chain: Chain of celery tasks to add indices, or fin task if no indices needed
    """
    if not mat_metadata.get("add_indices", False):
        return "Indices already exist"

    analysis_database = mat_metadata["analysis_database"]
    annotation_table_name = mat_metadata.get("annotation_table_name")
    schema = mat_metadata.get("schema")

    try:
        engine = db_manager.get_engine(analysis_database)

        if mat_metadata.get("reference_table"):
            model = make_reference_annotation_model(
                table_name=annotation_table_name,
                schema_type=schema,
                target_table=mat_metadata.get("reference_table"),
                segmentation_source=None,
                with_crud_columns=False,
            )
        else:
            model = make_flat_model(
                table_name=annotation_table_name,
                schema_type=schema,
                table_metadata=None,
            )

        commands = index_cache.add_indices_sql_commands(
            annotation_table_name, model, engine
        )

        if commands:
            add_index_tasks = chain(
                [add_index.si(analysis_database, command) for command in commands]
            )
            return self.replace(add_index_tasks)
        return fin.si()

    except Exception as e:
        celery_logger.error(f"Failed to prepare index creation tasks: {e}")
        raise
