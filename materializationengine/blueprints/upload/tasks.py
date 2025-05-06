import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List
import shlex

import pandas as pd
from celery import chain
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from flask import current_app
from redis import Redis
from sqlalchemy import text
from dynamicannotationdb.key_utils import build_segmentation_table_name

from materializationengine.blueprints.upload.gcs_processor import GCSCsvProcessor
from materializationengine.blueprints.upload.processor import SchemaProcessor
from materializationengine.celery_init import celery
from materializationengine.database import db_manager, dynamic_annotation_cache
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import add_index
from materializationengine.utils import get_config_param
from materializationengine.workflows.ingest_new_annotations import (
    create_missing_segmentation_table,
    create_segmentation_model,
)
from materializationengine.workflows.spatial_lookup import run_spatial_lookup_workflow

celery_logger = get_task_logger(__name__)

# Redis client for storing job status
REDIS_CLIENT = Redis(
    host=get_config_param("REDIS_HOST"),
    port=get_config_param("REDIS_PORT"),
    password=get_config_param("REDIS_PASSWORD"),
    db=0,
)


def update_job_status(job_id: str, status: Dict[str, Any]) -> None:
    """Update job status in Redis"""
    status["last_updated"] = datetime.now(timezone.utc).isoformat()
    REDIS_CLIENT.set(
        f"csv_processing:{job_id}", json.dumps(status), ex=3600  # Expires in 1 hour
    )


def get_job_status(job_id: str) -> Dict[str, Any]:
    """Get job status from Redis"""
    status = REDIS_CLIENT.get(f"csv_processing:{job_id}")
    return json.loads(status) if status else None


@celery.task(name="process:process_and_upload", bind=True)
def process_and_upload(
    self,
    file_path: str,
    file_metadata: dict,
    datastack_info: dict,
    **kwargs,
) -> Dict[str, Any]:
    """Chain CSV processing tasks"""
    job_id = f"csv_processing_{datetime.now(timezone.utc)}"
    materialization_time_stamp = datetime.utcnow()
    chunk_scale_factor = current_app.config.get("CHUNK_SCALE_FACTOR", 2)
    use_staging_database = current_app.config.get("USE_STAGING_DATABASE")
    sql_instance_name = current_app.config.get("SQL_INSTANCE_NAME")
    supervoxel_batch_size = current_app.config.get("SUPERVOXEL_BATCH_SIZE", 50)
    table_name = file_metadata["metadata"]["table_name"]
    schema_type = file_metadata["metadata"]["schema_type"]
    reference_table = file_metadata["metadata"].get("reference_table")
    column_mapping = file_metadata["column_mapping"]
    ignored_columns = file_metadata.get("ignored_columns")

    workflow = chain(
        process_csv.si(
            file_path,
            schema_type,
            column_mapping,
            reference_table=reference_table,
            ignored_columns=ignored_columns,
        ),
        upload_to_database.s(
            sql_instance_name=sql_instance_name,
            file_metadata=file_metadata,
            datastack_info=datastack_info,
        ),
        run_spatial_lookup_workflow.si(
            datastack_info=datastack_info,
            table_name=table_name,
            chunk_scale_factor=chunk_scale_factor,
            supervoxel_batch_size=supervoxel_batch_size,
            use_staging_database=True,
        ),
        transfer_to_production.si(
            datastack_info=datastack_info,
            table_name=table_name,
            transfer_segmentation=True,
            materialization_time_stamp=str(materialization_time_stamp),
        ),
    )

    result = workflow.apply_async()
    return {"job_id": result.id}


@celery.task(name="process:get_status")
def get_chain_status(job_id: str) -> Dict[str, Any]:
    """Get status of processing chain"""
    result = AsyncResult(job_id)
    if result.ready():
        # get status of the job
        status = get_job_status(job_id)
        return {
            "status": "completed" if result.successful() else "failed",
            "result": result.get() if result.successful() else str(result.result),
        }
    return {"status": "processing", "state": result.state}


@celery.task(name="process:process_csv", bind=True)
def process_csv(
    self,
    file_path: str,
    schema_type: str,
    column_mapping: Dict[str, str],
    reference_table: str = None,
    ignored_columns: List[str] = None,
    chunk_size: int = 10000,
) -> Dict[str, Any]:
    """Process CSV file in chunks using GCSCsvProcessor"""
    try:
        schema_processor = SchemaProcessor(
            schema_type,
            reference_table,
            column_mapping=column_mapping,
            ignored_columns=ignored_columns,
        )

        bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")

        gcs_processor = GCSCsvProcessor(bucket_name, chunk_size=chunk_size)

        timestamp = datetime.now(timezone.utc)
        timestamp_str = timestamp.strftime("%Y%m%d_%H%M%S")

        source_blob_name = file_path.split("/")[-1]
        destination_blob_name = f"processed_{schema_type}_{timestamp_str}.csv"

        def transform_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
            return schema_processor.process_chunk(chunk, timestamp)

        def print_progress(progress):
            celery_logger.info(f"Upload formatted progress: {progress:.2f}%")

        gcs_processor.process_csv_in_chunks(
            source_blob_name,
            destination_blob_name,
            transform_chunk,
            chunk_upload_callback=print_progress,
        )

        return {
            "status": "completed",
            "output_path": f"{bucket_name}/{destination_blob_name}",
        }
    except Exception as e:
        celery_logger.error(f"Error processing CSV: {str(e)}")
        raise


@celery.task(name="process:upload_to_db", bind=True)
def upload_to_database(
    self,
    process_result: Dict[str, Any],
    sql_instance_name: str,
    file_metadata: Dict[str, Any],
    datastack_info: str,
    job_id: str = None,
) -> Dict[str, Any]:
    """Upload processed CSV to database"""
    try:
        table_name = file_metadata["metadata"]["table_name"]
        schema = file_metadata["metadata"]["schema_type"]
        description = file_metadata["metadata"]["description"]
        user_id = file_metadata["metadata"]["user_id"]
        voxel_resolution_nm_x = file_metadata["metadata"]["voxel_resolution_nm_x"]
        voxel_resolution_nm_y = file_metadata["metadata"]["voxel_resolution_nm_y"]
        voxel_resolution_nm_z = file_metadata["metadata"]["voxel_resolution_nm_z"]
        write_permission = file_metadata["metadata"]["write_permission"]
        read_permission = file_metadata["metadata"]["read_permission"]
        flat_segmentation_source = file_metadata["metadata"].get(
            "flat_segmentation_source"
        )
        table_metadata = file_metadata["metadata"].get("table_metadata")
        notice_text = file_metadata["metadata"].get("notice_text")

        staging_database = current_app.config.get("STAGING_DATABASE_NAME")
        google_app_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if not sql_instance_name:
            error_msg = "SQL_INSTANCE_NAME is not configured or is None."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        if not google_app_creds:
            error_msg = "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        if not staging_database:
            error_msg = "STAGING_DATABASE_NAME is not configured or is None."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        if not table_name:
            error_msg = "Table name is missing or None in file_metadata."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        output_path = process_result.get("output_path")
        if not output_path:
            error_msg = "Output path from CSV processing is missing or None."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        db_client = dynamic_annotation_cache.get_db(staging_database)
        try:
            db_client.annotation.create_table(
                table_name=table_name,
                schema_type=schema,
                description=description,
                user_id=user_id,
                voxel_resolution_x=voxel_resolution_nm_x,
                voxel_resolution_y=voxel_resolution_nm_y,
                voxel_resolution_z=voxel_resolution_nm_z,
                table_metadata=table_metadata,
                flat_segmentation_source=flat_segmentation_source,
                write_permission=write_permission,
                read_permission=read_permission,
                notice_text=notice_text,
            )
        except Exception as e:
            celery_logger.error(f"Error creating table: {str(e)}")

        # create table in staging database and drop indices
        index_cache.drop_table_indices(table_name, db_client.database.engine)

        # Activate gcloud service account
        activate_command = [
            "gcloud",
            "auth",
            "activate-service-account",
            "--key-file",
            google_app_creds,
        ]
        try:
            celery_logger.info("Activating service account")
            activate_process = subprocess.run(
                activate_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            celery_logger.info(f"Service account activation stdout: {activate_process.stdout}")
            if activate_process.stderr:
                celery_logger.warning(f"Service account activation stderr: {activate_process.stderr}")
        except subprocess.CalledProcessError as e:
            error_msg = f"Failed to activate service account: {e}, stderr: {e.stderr}"
            celery_logger.error(error_msg)
            if job_id:
                update_job_status(
                    job_id,
                    {
                        "status": "error",
                        "phase": "Service Account Activation",
                        "progress": 0,
                        "error": e.stderr or str(e),
                    },
                )
            raise
        except subprocess.TimeoutExpired as e:
            error_msg = f"Service account activation timed out: {e}"
            celery_logger.error(error_msg)
            if job_id:
                update_job_status(
                    job_id,
                    {
                        "status": "error",
                        "phase": "Service Account Activation",
                        "progress": 0,
                        "error": "Service account activation timed out",
                    },
                )
            raise

        # TODO move to deployment scripts
        # get sql service account email
        # get_service_account_command = [
        #     "gcloud",
        #     "sql",
        #     "instances",
        #     "describe",
        #     sql_instance_name,
        #     "--format=json",
        # ]
        # try:
        #     sql_instance_info = subprocess.run(get_service_account_command, check=True, capture_output=True, text=True)
        #     sql_instance_info = json.loads(sql_instance_info.stdout)
        #     service_account_email = sql_instance_info["serviceAccountEmailAddress"]
        # except subprocess.CalledProcessError as e:
        #     celery_logger.error(f"Database upload failed: {e}")
        #     raise e

        # auth_command = [
        #     "gcloud",
        #     "storage",
        #     "buckets",
        #     "add-iam-policy-binding",
        #     f"gs://{bucket_name}",
        #     "--member",
        #     f"serviceAccount:{service_account_email}",
        #     "--role",
        #     "roles/storage.objectAdmin",
        # ]

        load_command = [
            "gcloud",
            "sql",
            "import",
            "csv",
            sql_instance_name,
            f"gs://{output_path}",
            "--database",
            staging_database,
            "--table",
            table_name,
            "--user",
            "postgres",
            "--quiet",
        ]
        try:
            celery_logger.info(f"Running command: {shlex.join(load_command)}")

            result = subprocess.run(
                load_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=1200,
            )
            celery_logger.info(f"Subprocess output: {result.stdout}")
            if result.stderr:
                celery_logger.warning(f"Subprocess stderr: {result.stderr}")
        except subprocess.CalledProcessError as e:
            celery_logger.error(f"Database upload failed: {e}, stderr: {e.stderr}")
            if job_id:
                update_job_status(
                    job_id,
                    {
                        "status": "error",
                        "phase": "Uploading to Database",
                        "progress": 0,
                        "error": e.stderr or str(e),
                    },
                )
            raise
        except subprocess.TimeoutExpired as e:
            celery_logger.error(f"Subprocess timed out: {e}")
            if job_id:
                update_job_status(
                    job_id,
                    {
                        "status": "error",
                        "phase": "Uploading to Database",
                        "progress": 0,
                        "error": "Subprocess timed out",
                    },
                )
            raise

        schema_model = db_client.schema.create_annotation_model(table_name, schema)

        anno_indices = index_cache.add_indices_sql_commands(
            table_name, schema_model, db_client.database.engine
        )
        for index in anno_indices:
            celery_logger.info(f"Adding index: {index}")
            result = add_index(staging_database, index)
            celery_logger.info(f"Index added: {result}")

        return {
            "status": "completed",
        }
    except Exception as e:
        celery_logger.error(f"Database upload failed: {str(e)}")
        raise


@celery.task(name="process:transfer_to_production", bind=True)
def transfer_to_production(
    self,
    datastack_info: dict,
    table_name: str,
    transfer_segmentation: bool = True,
    materialization_time_stamp: datetime = datetime.utcnow(),
    job_id: str = None,
):
    """Transfer tables from staging schema to production database.

    Uses pg_dump and psql for efficient data transfer.
    Optimizes transfer by dropping indexes before transfer and rebuilding them afterward.

    Args:
        datastack_info (dict): Information about the datastack
        table_name (str): Name of the annotation table to transfer
        transfer_segmentation (bool): Whether to also transfer corresponding segmentation table

    Returns:
        dict: Result information including success status
    """
    try:
        staging_schema = get_config_param("STAGING_DATABASE_NAME")
        production_schema = datastack_info["aligned_volume"]["name"]
        pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]

        celery_logger.info(
            f"Transferring {table_name} from schema {staging_schema} to schema {production_schema}"
        )

        staging_db = dynamic_annotation_cache.get_db(staging_schema)
        production_db = dynamic_annotation_cache.get_db(production_schema)

        table_metadata = staging_db.database.get_table_metadata(table_name)
        schema_type = table_metadata.get("schema_type")

        if not schema_type:
            raise ValueError(f"Could not determine schema type for table {table_name}")

        needs_segmentation = staging_db.schema.is_segmentation_table_required(
            schema_type
        )

        # Create the annotation table in the production schema if it doesn't exist
        table_exists = False
        try:
            production_db.database.get_table_metadata(table_name)
            table_exists = True
            celery_logger.info(
                f"Table {table_name} already exists in production schema"
            )
        except Exception:
            table_exists = False

        if not table_exists:
            celery_logger.info(f"Creating table {table_name} in production schema")
            production_db.annotation.create_table(
                table_name=table_name,
                schema_type=schema_type,
                description=table_metadata.get("description", ""),
                user_id=table_metadata.get("user_id", ""),
                voxel_resolution_x=table_metadata.get("voxel_resolution_x", 1),
                voxel_resolution_y=table_metadata.get("voxel_resolution_y", 1),
                voxel_resolution_z=table_metadata.get("voxel_resolution_z", 1),
                table_metadata=table_metadata.get("table_metadata"),
                flat_segmentation_source=table_metadata.get("flat_segmentation_source"),
                write_permission=table_metadata.get("write_permission", "PRIVATE"),
                read_permission=table_metadata.get("read_permission", "PRIVATE"),
                notice_text=table_metadata.get("notice_text"),
            )

        # Get database connection info from engine
        engine = db_manager.get_engine(production_schema)
        db_url = engine.url
        db_info = get_db_connection_info(db_url)

        # Transfer annotation table
        celery_logger.info(f"Transferring annotation table {table_name}")
        row_count = transfer_table_using_pg_dump(
            table_name=table_name,
            source_db=staging_schema,
            target_db=production_schema,
            db_info=db_info,
            drop_indices=True,
            rebuild_indices=True,
            engine=engine,
            model_creator=lambda: production_db.schema.create_annotation_model(
                table_name, schema_type
            ),
        )

        # Handle segmentation table if needed
        segmentation_results = None
        if transfer_segmentation and needs_segmentation:
            segmentation_table_name = build_segmentation_table_name(
                table_name, pcg_table_name
            )

            # Check if segmentation table exists in staging
            segmentation_exists = False
            try:
                staging_db.database.get_table_metadata(segmentation_table_name)
                segmentation_exists = True
            except Exception:
                segmentation_exists = False

            if segmentation_exists:
                celery_logger.info(
                    f"Transferring segmentation table {segmentation_table_name}"
                )

                # Create mat_metadata for segmentation table
                mat_metadata = {
                    "annotation_table_name": table_name,
                    "segmentation_table_name": segmentation_table_name,
                    "schema": schema_type,
                    "database": production_schema,
                    "aligned_volume": production_schema,
                    "pcg_table_name": datastack_info.get(
                        "segmentation_source", ""
                    ).split("/")[-1],
                    "last_updated": materialization_time_stamp,
                }

                create_missing_segmentation_table(mat_metadata)

                seg_row_count = transfer_table_using_pg_dump(
                    table_name=segmentation_table_name,
                    source_db=staging_schema,
                    target_db=production_schema,
                    db_info=db_info,
                    drop_indices=True,
                    rebuild_indices=True,
                    engine=engine,
                    model_creator=lambda: create_segmentation_model(mat_metadata),
                )

                segmentation_results = {
                    "name": segmentation_table_name,
                    "success": True,
                    "rows": seg_row_count,
                }

        return {
            "status": "success",
            "tables_transferred": {
                "annotation_table": {
                    "name": table_name,
                    "success": True,
                    "rows": row_count,
                },
                "segmentation_table": segmentation_results,
            },
        }

    except Exception as e:
        celery_logger.error(f"Error transferring tables: {str(e)}")
        return {"status": "error", "error": str(e)}


def get_db_connection_info(db_url):
    """Extract connection information from SQLAlchemy URL object."""
    db_info = {}
    if hasattr(db_url, "database"):
        db_info = {
            "host": db_url.host,
            "port": db_url.port,
            "user": db_url.username,
            "password": db_url.password,
            "dbname": db_url.database,
        }
    else:
        db_info = {
            "host": db_url.host,
            "port": db_url.port,
            "user": db_url.username,
            "password": db_url.password,
            "dbname": str(db_url).split("/")[-1],
        }
    return db_info


def transfer_table_using_pg_dump(
    table_name: str,
    source_db: str,
    target_db: str,
    db_info: dict,
    drop_indices: bool = True,
    rebuild_indices: bool = True,
    engine=None,
    model_creator=None,
    job_id: str = None,
) -> int:
    """
    Transfer a table using pg_dump and psql.

    Args:
        table_name: Name of the table to transfer
        source_db: Source database name
        target_db: Target database name
        db_info: Dictionary with database connection information
        drop_indices: Whether to drop indices before transfer
        rebuild_indices: Whether to rebuild indices after transfer
        engine: SQLAlchemy engine (required if drop_indices or rebuild_indices is True)
        model_creator: Function that returns the SQLAlchemy model (required if rebuild_indices is True)

    Returns:
        int: Number of rows transferred
    """
    if (drop_indices or rebuild_indices) and engine is None:
        raise ValueError(
            "Engine is required when drop_indices or rebuild_indices is True"
        )

    if rebuild_indices and model_creator is None:
        raise ValueError("model_creator is required when rebuild_indices is True")

    if drop_indices:
        celery_logger.info(f"Dropping indexes on {table_name}")
        index_cache.drop_table_indices(table_name, engine)

    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    # Build pg_dump and psql commands
    pg_dump_cmd = [
        "pg_dump",
        f'--host={db_info["host"]}',
        f'--port={db_info["port"]}',
        f'--username={db_info["user"]}',
        f"--dbname={source_db}",
        "--data-only",
        f"--table={table_name}",
    ]

    psql_cmd = [
        "psql",
        f'--host={db_info["host"]}',
        f'--port={db_info["port"]}',
        f'--username={db_info["user"]}',
        f"--dbname={target_db}",
    ]

    pg_env = os.environ.copy()
    if db_info["password"]:
        pg_env["PGPASSWORD"] = db_info["password"]

    celery_logger.info(f"Transferring data for {table_name} using pg_dump/psql")
    try:
        celery_logger.info(f"Running pg_dump command: {shlex.join(pg_dump_cmd)}")
        celery_logger.info(f"Running psql command: {shlex.join(psql_cmd)}")

        with subprocess.Popen(
            pg_dump_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=pg_env
        ) as dump_proc:
            result = subprocess.run(
                psql_cmd,
                stdin=dump_proc.stdout,
                capture_output=True,
                text=True,
                timeout=1200,
                env=pg_env,
            )
            celery_logger.info(f"psql output: {result.stdout}")
            if result.stderr:
                celery_logger.warning(f"psql stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        celery_logger.error(f"pg_dump/psql error: {e}")
        if job_id:
            update_job_status(
                job_id,
                {
                    "status": "error",
                    "phase": "Transferring to Production",
                    "progress": 0,
                    "error": e.stderr or str(e),
                },
            )
        raise
    except subprocess.TimeoutExpired as e:
        celery_logger.error(f"Subprocess timed out: {e}")
        if job_id:
            update_job_status(
                job_id,
                {
                    "status": "error",
                    "phase": "Transferring to Production",
                    "progress": 0,
                    "error": "Subprocess timed out",
                },
            )
        raise

    with engine.connect() as conn:
        row_count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()

    if rebuild_indices:
        celery_logger.info(f"Rebuilding indexes on {table_name}")
        model = model_creator()
        indices = index_cache.add_indices_sql_commands(table_name, model, engine)
        for index in indices:
            celery_logger.info(f"Adding index: {index}")
            with engine.begin() as conn:
                conn.execute(text(index))

    return row_count


@celery.task(name="process:cancel_processing")
def cancel_processing(job_id: str) -> Dict[str, Any]:
    """Cancel processing job"""
    try:
        status = {"status": "cancelled", "phase": "Cancelled", "progress": 0}
        celery.control.revoke(job_id, terminate=True)

        update_job_status(job_id, status)
        return status
    except Exception as e:
        celery_logger.error(f"Error cancelling job: {str(e)}")
        raise
