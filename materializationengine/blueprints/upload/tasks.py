import json
import os
import shlex
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from celery import chain
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from dynamicannotationdb.key_utils import build_segmentation_table_name
from flask import current_app
from redis import Redis
from sqlalchemy import text

from materializationengine.blueprints.upload.checkpoint_manager import (
    CHUNK_STATUS_COMPLETED,
    CHUNK_STATUS_ERROR,
    CHUNK_STATUS_FAILED_PERMANENT,
    CHUNK_STATUS_FAILED_RETRYABLE,
    CHUNK_STATUS_PROCESSING,
    CHUNK_STATUS_PROCESSING_SUBTASKS,
    RedisCheckpointManager,
)
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
    existing_status = get_job_status(job_id)
    if existing_status:
        if "user_id" in existing_status and "user_id" not in status:
            status["user_id"] = existing_status["user_id"]
        if "datastack_name" in existing_status and "datastack_name" not in status:
            status["datastack_name"] = existing_status["datastack_name"]

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
    use_staging_database: bool = True,
    **kwargs,
) -> Dict[str, Any]:
    """Chain CSV processing tasks"""

    user_id = kwargs.get("user_id", "unknown")
    datastack_name = datastack_info.get("datastack", "unknown")

    materialization_time_stamp = datetime.utcnow()

    chunk_scale_factor = current_app.config.get("CHUNK_SCALE_FACTOR", 2)
    sql_instance_name = current_app.config.get("SQL_INSTANCE_NAME")
    supervoxel_batch_size = current_app.config.get("SUPERVOXEL_BATCH_SIZE", 50)
    table_name = file_metadata["metadata"]["table_name"]
    schema_type = file_metadata["metadata"]["schema_type"]
    reference_table = file_metadata["metadata"].get("reference_table")
    column_mapping = file_metadata["column_mapping"]
    ignored_columns = file_metadata.get("ignored_columns")
    main_job_id = f"{datastack_name}_{table_name}_{materialization_time_stamp.strftime('%Y%m%d_%H%M%S')}"

    workflow = chain(
        process_csv.si(
            file_path=file_path,
            schema_type=schema_type,
            column_mapping=column_mapping,
            job_id_for_status=main_job_id,
            reference_table=reference_table,
            ignored_columns=ignored_columns,
        ),
        upload_to_database.s(
            sql_instance_name=sql_instance_name,
            file_metadata=file_metadata,
            datastack_info=datastack_info,
            job_id=main_job_id,
        ),
        run_spatial_lookup_workflow.si(
            datastack_info=datastack_info,
            table_name=table_name,
            chunk_scale_factor=chunk_scale_factor,
            supervoxel_batch_size=supervoxel_batch_size,
            use_staging_database=True,
        ),
        monitor_spatial_workflow_completion.s(
            datastack_info=datastack_info,
            table_name_for_transfer=table_name,
            materialization_time_stamp=str(materialization_time_stamp),
            job_id_for_status=main_job_id,
        ),
        transfer_to_production.s(
            transfer_segmentation=True,
        ),
    )

    result = workflow.apply_async()
    update_job_status(
        main_job_id,
        {
            "phase": "Workflow Chain Initialized",
            "chain_id": result.id,
            "user_id": user_id,
            "datastack_name": datastack_name,
        },
    )
    return {"status": "workflow_initiated", "chain_root_id": result.id}


@celery.task(name="process:get_status")
def get_chain_status(job_id: str) -> Dict[str, Any]:
    """Get status of the overall processing job from Redis."""

    status = get_job_status(job_id)
    if status:
        return status
    else:
        main_task_result = AsyncResult(job_id)
        if main_task_result.state == "PENDING":
            return {
                "status": "pending",
                "phase": "Job Queued",
                "progress": 0,
                "message": "Job is queued and waiting to start.",
            }
        elif main_task_result.state == "FAILURE":
            return {
                "status": "error",
                "phase": "Job Initialization",
                "error": str(main_task_result.result),
                "progress": 0,
            }
        return {
            "status": "not_found",
            "message": "Job status not found or job ID is invalid.",
        }


@celery.task(name="process:process_csv", bind=True)
def process_csv(
    self,
    file_path: str,
    schema_type: str,
    column_mapping: Dict[str, str],
    job_id_for_status: str,
    reference_table: str = None,
    ignored_columns: List[str] = None,
    chunk_size: int = 10000,
) -> Dict[str, Any]:
    """Process CSV file in chunks using GCSCsvProcessor"""
    try:
        update_job_status(
            job_id_for_status,
            {
                "status": "processing",
                "phase": "Processing CSV Data",
                "progress": 0,
                "processed_rows": 0,
                "total_rows": "Calculating...",
                "current_chunk_num": 0,
                "total_chunks": "Calculating...",
            },
        )
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

        def progress_callback(progress_details: Dict[str, Any]):
            celery_logger.info(
                f"CSV Processing Progress (Job: {job_id_for_status}): "
                f"{progress_details['progress']:.2f}%, "
                f"Rows: {progress_details['processed_rows']}/{progress_details['total_rows']}, "
                f"Chunk: {progress_details['current_chunk_num']}/{progress_details['total_chunks']}"
            )
            update_job_status(
                job_id_for_status,
                {
                    "status": "processing",
                    "phase": "Processing CSV Data",
                    "progress": progress_details["progress"],
                    "processed_rows": progress_details["processed_rows"],
                    "total_rows": progress_details["total_rows"],
                    "current_chunk_num": progress_details["current_chunk_num"],
                    "total_chunks": progress_details["total_chunks"],
                },
            )

        gcs_processor.process_csv_in_chunks(
            source_blob_name,
            destination_blob_name,
            transform_chunk,
            chunk_upload_callback=progress_callback,
        )

        return {
            "status": "completed_csv_processing",
            "output_path": f"{bucket_name}/{destination_blob_name}",
            "job_id_for_status": job_id_for_status,
        }
    except Exception as e:
        celery_logger.error(
            f"Error processing CSV for job {job_id_for_status}: {str(e)}", exc_info=True
        )
        try:
            update_job_status(
                job_id_for_status,
                {
                    "status": "error",
                    "phase": "Processing CSV Data",
                    "error": f"CSV processing failed: {str(e)}",
                    "progress": 0,
                },
            )
        except Exception as update_err:
            celery_logger.error(f"Failed to update job status with error: {update_err}")
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

    job_id_for_status = process_result.get("job_id_for_status")
    output_path = process_result.get("output_path")

    processed_rows_from_csv = process_result.get("processed_rows", 0)
    total_rows_from_csv = process_result.get("total_rows", "N/A")
    datastack_name_from_info = datastack_info.get("datastack", "unknown_datastack")
    staging_database_name = current_app.config.get("STAGING_DATABASE_NAME")

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
        if file_metadata["metadata"].get("is_reference_schema"):
            target_table = file_metadata["metadata"].get(
                "reference_table"
            )
            table_metadata = {"reference_table": target_table}
        else:
            table_metadata = None
        notice_text = file_metadata["metadata"].get("notice_text")

        staging_database = current_app.config.get("STAGING_DATABASE_NAME")
        google_app_creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if not sql_instance_name:
            error_msg = "SQL_INSTANCE_NAME is not configured or is None."
            celery_logger.error(error_msg)
            raise ValueError(error_msg)

        if not google_app_creds:
            error_msg = (
                "GOOGLE_APPLICATION_CREDENTIALS environment variable is not set."
            )
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

        update_job_status(
            job_id_for_status,
            {
                "status": "processing",
                "phase": "Uploading to Database: Initializing",
                "progress": 5,
                "processed_rows": processed_rows_from_csv,
                "total_rows": total_rows_from_csv,
            },
        )
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

        update_job_status(
            job_id_for_status,
            {"phase": "Uploading to Database: Dropping Indices", "progress": 10},
        )

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
            update_job_status(
                job_id_for_status,
                {"phase": "Uploading to Database: Setting up transfer", "progress": 15},
            )

            activate_process = subprocess.run(
                activate_command,
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
            celery_logger.info(
                f"Service account activation stdout: {activate_process.stdout}"
            )
            if activate_process.stderr:
                celery_logger.warning(
                    f"Service account activation stderr: {activate_process.stderr}"
                )
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
            update_job_status(
                job_id_for_status,
                {
                    "status": "error",
                    "phase": "Service Account Activation",
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
            update_job_status(
                job_id_for_status,
                {
                    "phase": "Uploading to Database: Importing CSV via gcloud",
                    "progress": 30,
                },
            )

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
            update_job_status(
                job_id_for_status,
                {
                    "status": "error",
                    "phase": "Uploading to Database (gcloud import)",
                    "error": e.stderr or str(e),
                },
            )

            raise
        except subprocess.TimeoutExpired as e:
            celery_logger.error(f"Subprocess timed out: {e}")
            update_job_status(
                job_id_for_status,
                {
                    "status": "error",
                    "phase": "Uploading to Database (gcloud import)",
                    "error": "gcloud sql import timed out",
                },
            )

            raise
        update_job_status(
            job_id_for_status,
            {"phase": "Uploading to Database: Rebuilding Indices", "progress": 80},
        )

        schema_model = db_client.schema.create_annotation_model(table_name, schema)

        anno_indices = index_cache.add_indices_sql_commands(
            table_name, schema_model, db_client.database.engine
        )
        for i, index_sql in enumerate(anno_indices):
            celery_logger.info(f"Adding index ({i+1}/{len(anno_indices)}): {index_sql}")
            current_progress = (
                80 + int((i / len(anno_indices)) * 20) if len(anno_indices) > 0 else 80
            )
            update_job_status(
                job_id_for_status,
                {
                    "phase": f"Uploading to Database: Adding Index {i+1}",
                    "progress": current_progress,
                },
            )
            add_index(staging_database, index_sql)
            celery_logger.info(f"Index {i+1} added/command submitted.")

        update_job_status(
            job_id_for_status,
            {
                "status": "processing",
                "phase": "Database Upload Complete, awaiting next step",
                "progress": 100,
            },
        )

        spatial_lookup_config = {
            "table_name": table_name,
            "datastack_name": datastack_name_from_info,
            "database_name": staging_database_name,
        }
        update_job_status(
            job_id_for_status,
            {
                "status": "processing",
                "phase": "Preparing Spatial Lookup",
                "progress": 0,
                "active_workflow_part": "spatial_lookup",
                "total_rows": total_rows_from_csv,
                "processed_rows": processed_rows_from_csv,
            },
        )

        return {
            "status_task": "completed_db_upload",
            "job_id_for_status": job_id_for_status,
            "table_name": table_name,
            "datastack_info": datastack_info,
        }
    except Exception as e:
        celery_logger.error(
            f"Database upload failed for job {job_id_for_status}: {str(e)}",
            exc_info=True,
        )
        if job_id_for_status:
            update_job_status(
                job_id_for_status,
                {
                    "status": "error",
                    "phase": "Uploading to Database",
                    "error": f"DB upload unexpected error: {str(e)}",
                },
            )
        raise

@celery.task(
    name="process:monitor_spatial_workflow_completion", bind=True, max_retries=None
)
def monitor_spatial_workflow_completion(
    self,
    spatial_launch_result: Dict[str, Any],
    datastack_info: dict,
    table_name_for_transfer: str,
    materialization_time_stamp: str,
    job_id_for_status: str,
):
    """
    Monitors the spatial workflow's completion status using RedisCheckpointManager.
    """
    workflow_to_monitor = spatial_launch_result.get("workflow_name")
    db_for_monitor = spatial_launch_result.get("database_name")

    if spatial_launch_result.get("status") == "completed_no_data_found":
        celery_logger.info(
            f"Spatial workflow for '{workflow_to_monitor}' was a no-op (completed_no_data_found). Proceeding to transfer."
        )
        return {
            "datastack_info": datastack_info,
            "table_name": table_name_for_transfer,
            "materialization_time_stamp": materialization_time_stamp,
            "spatial_workflow_final_status": "skipped_no_data",
            "job_id_for_status": job_id_for_status,
        }

    if not workflow_to_monitor or not db_for_monitor:
        error_msg = f"monitor_spatial_workflow_completion: Missing workflow_name or database_name in spatial_launch_result: {spatial_launch_result}"
        celery_logger.error(error_msg)
        raise ValueError(error_msg)

    log_prefix = f"[Monitor WF:'{workflow_to_monitor}', DB:'{db_for_monitor}']"
    celery_logger.info(
        f"{log_prefix} Checking overall completion status of spatial workflow."
    )

    try:
        checkpoint_manager = RedisCheckpointManager(db_for_monitor)
        workflow_data = checkpoint_manager.get_workflow_data(workflow_to_monitor)
    except Exception as e:
        celery_logger.error(
            f"{log_prefix} Error initializing RedisCheckpointManager or getting workflow data: {e}. Retrying...",
            exc_info=True,
        )
        raise self.retry(exc=e, countdown=60)

    if not workflow_data:
        celery_logger.warning(
            f"{log_prefix} No workflow data found in Redis for '{workflow_to_monitor}' yet. Retrying in 60s."
        )
        raise self.retry(countdown=60)

    current_workflow_status = workflow_data.status
    celery_logger.info(
        f"{log_prefix} Current overall workflow status from Redis: '{current_workflow_status}'"
    )

    if current_workflow_status == CHUNK_STATUS_COMPLETED:
        celery_logger.info(
            f"{log_prefix} Workflow '{workflow_to_monitor}' is '{CHUNK_STATUS_COMPLETED}'. Proceeding to transfer."
        )
        update_job_status(
            job_id_for_status,
            {
                "status": "processing",
                "phase": "Spatial Lookup Complete, Preparing Transfer",
                "progress": 0,
                "active_workflow_part": "transfer",
            },
        )
        return {
            "datastack_info": datastack_info,
            "table_name": table_name_for_transfer,
            "materialization_time_stamp": materialization_time_stamp,
            "spatial_workflow_final_status": CHUNK_STATUS_COMPLETED,
            "workflow_details": {
                "name": workflow_to_monitor,
                "completed_chunks": workflow_data.completed_chunks,
                "total_chunks": workflow_data.total_chunks,
                "rows_processed": workflow_data.rows_processed,
                "start_time": workflow_data.start_time,
                "updated_at": workflow_data.updated_at,
            },
            "job_id_for_status": job_id_for_status,
        }
    elif current_workflow_status in {CHUNK_STATUS_ERROR, "failed"}:
        err_msg = (
            f"Spatial workflow '{workflow_to_monitor}' has terminally FAILED with status "
            f"'{current_workflow_status}'. Last Error recorded: {workflow_data.last_error}"
        )
        celery_logger.error(f"{log_prefix} {err_msg}")
        update_job_status(
            job_id_for_status,
            {
                "status": "error",
                "phase": f"Spatial Lookup Failed ({workflow_to_monitor})",
                "error": f"Spatial workflow failed: {workflow_data.last_error or 'Unknown spatial error'}",
            },
        )
        raise Exception(err_msg)
    else:

        celery_logger.info(
            f"{log_prefix} Workflow '{workflow_to_monitor}' status is '{current_workflow_status}'. "
            f"Not yet complete. Retrying in 60 seconds."
        )
        raise self.retry(countdown=60)
    
@celery.task(name="process:transfer_to_production", bind=True, ack_late=True)
def transfer_to_production(
    self,
    monitor_result: dict,
    transfer_segmentation: bool = True,
):
    """
    Transfer tables from staging schema to production database after spatial lookup.
    Uses pg_dump and psql for efficient data transfer.
    Optimizes transfer by dropping indexes before transfer and rebuilding them afterward.
    """
    try:
        datastack_info = monitor_result["datastack_info"]
        table_name_to_transfer = monitor_result["table_name"]
        materialization_time_stamp_str = monitor_result["materialization_time_stamp"]
        spatial_workflow_status = monitor_result.get("spatial_workflow_final_status", "UNKNOWN")
       
        try:
            materialization_time_stamp_dt = datetime.fromisoformat(materialization_time_stamp_str)
        except ValueError:
            materialization_time_stamp_dt = datetime.strptime(materialization_time_stamp_str, '%Y-%m-%d %H:%M:%S.%f')
          

        celery_logger.info(
            f"Executing transfer_to_production for table: '{table_name_to_transfer}'. "
            f"Spatial workflow final status: '{spatial_workflow_status}'."
        )
        if "workflow_details" in monitor_result: 
            celery_logger.info(f"Spatial workflow details: {monitor_result['workflow_details']}")

        staging_schema_name = get_config_param("STAGING_DATABASE_NAME")
        production_schema_name = datastack_info["aligned_volume"]["name"]
        pcg_table_name = datastack_info["segmentation_source"].split("/")[-1]

        celery_logger.info(
            f"Transferring table '{table_name_to_transfer}' from staging schema '{staging_schema_name}' "
            f"to production schema '{production_schema_name}'"
        )

        staging_db_client = dynamic_annotation_cache.get_db(staging_schema_name) 
        production_db_client = dynamic_annotation_cache.get_db(production_schema_name) 

        table_metadata_from_staging = staging_db_client.database.get_table_metadata(table_name_to_transfer)
        schema_type = table_metadata_from_staging.get("schema_type")

        if not schema_type:
            raise ValueError(f"Could not determine schema type for table '{table_name_to_transfer}' in staging schema '{staging_schema_name}'")

        needs_segmentation_table = staging_db_client.schema.is_segmentation_table_required(schema_type)

        production_table_exists = False
        try:
            production_db_client.database.get_table_metadata(table_name_to_transfer)
            production_table_exists = True
            celery_logger.info(
                f"Annotation table '{table_name_to_transfer}' already exists in production schema '{production_schema_name}'."
            )
        except Exception: 
            production_table_exists = False
            celery_logger.info(
                f"Annotation table '{table_name_to_transfer}' does not exist in production schema '{production_schema_name}'. Will create."
            )

        if not production_table_exists:
            celery_logger.info(f"Creating annotation table '{table_name_to_transfer}' in production schema '{production_schema_name}'")
            production_db_client.annotation.create_table(
                table_name=table_name_to_transfer,
                schema_type=schema_type,
                description=table_metadata_from_staging.get("description", ""),
                user_id=table_metadata_from_staging.get("user_id", ""),
                voxel_resolution_x=table_metadata_from_staging.get("voxel_resolution_x", 1.0),
                voxel_resolution_y=table_metadata_from_staging.get("voxel_resolution_y", 1.0),
                voxel_resolution_z=table_metadata_from_staging.get("voxel_resolution_z", 1.0),
                table_metadata=table_metadata_from_staging.get("table_metadata"),
                flat_segmentation_source=table_metadata_from_staging.get("flat_segmentation_source"),
                write_permission=table_metadata_from_staging.get("write_permission", "PRIVATE"),
                read_permission=table_metadata_from_staging.get("read_permission", "PRIVATE"),  
                notice_text=table_metadata_from_staging.get("notice_text"),
            )

        
        production_engine = db_manager.get_engine(production_schema_name)
        db_url_obj = production_engine.url 
        
   
        db_connection_info_for_cli = get_db_connection_info(db_url_obj)


        celery_logger.info(f"Transferring data for annotation table '{table_name_to_transfer}'")
        annotation_rows_transferred = transfer_table_using_pg_dump(
            table_name=table_name_to_transfer,
            source_db=staging_schema_name,   
            target_db=production_schema_name,  
            db_info=db_connection_info_for_cli, 
            drop_indices=True,
            rebuild_indices=True,
            engine=production_engine,
            model_creator=lambda: production_db_client.schema.create_annotation_model(
                table_name_to_transfer, schema_type
            ),
            job_id=monitor_result.get("job_id_for_status"),
        )

        segmentation_transfer_results = None
        if transfer_segmentation and needs_segmentation_table:
            segmentation_table_name = build_segmentation_table_name(
                table_name_to_transfer, pcg_table_name
            )
            celery_logger.info(f"Segmentation table processing for: '{segmentation_table_name}'")

            staging_segmentation_exists = False
            try:
                staging_db_client.database.get_table_metadata(segmentation_table_name)
                staging_segmentation_exists = True
            except Exception: 
                celery_logger.info(f"Segmentation table '{segmentation_table_name}' not found in staging schema '{staging_schema_name}'. Skipping transfer for it.")

            if staging_segmentation_exists:
                celery_logger.info(f"Preparing to transfer segmentation table '{segmentation_table_name}'")
                
                mat_metadata_for_segmentation_table = {
                    "annotation_table_name": table_name_to_transfer,
                    "segmentation_table_name": segmentation_table_name,
                    "schema_type": schema_type,
                    "database": production_schema_name, 
                    "aligned_volume": production_schema_name, 
                    "pcg_table_name": pcg_table_name,
                    "last_updated": materialization_time_stamp_dt, 
                    "voxel_resolution_x": table_metadata_from_staging.get("voxel_resolution_x", 1.0),
                    "voxel_resolution_y": table_metadata_from_staging.get("voxel_resolution_y", 1.0),
                    "voxel_resolution_z": table_metadata_from_staging.get("voxel_resolution_z", 1.0),
                }

 
                create_missing_segmentation_table(mat_metadata_for_segmentation_table, db_client=production_db_client)
                
                celery_logger.info(f"Transferring data for segmentation table '{segmentation_table_name}'")
                segmentation_rows_transferred = transfer_table_using_pg_dump(
                    table_name=segmentation_table_name,
                    source_db=staging_schema_name,
                    target_db=production_schema_name,
                    db_info=db_connection_info_for_cli,
                    drop_indices=True,
                    rebuild_indices=True,
                    engine=production_engine,
                    model_creator=lambda: create_segmentation_model(mat_metadata_for_segmentation_table),
                )
                segmentation_transfer_results = {
                    "name": segmentation_table_name,
                    "success": True,
                    "rows_transferred": segmentation_rows_transferred,
                }
            else:
                 segmentation_transfer_results = {
                    "name": segmentation_table_name,
                    "success": False,
                    "message": "Not found in staging",
                    "rows_transferred": 0,
                }


        return {
            "status": "success",
            "message": f"Transfer completed for table '{table_name_to_transfer}'.",
            "tables_transferred": {
                "annotation_table": {
                    "name": table_name_to_transfer,
                    "success": True,
                    "rows_transferred": annotation_rows_transferred,
                },
                "segmentation_table": segmentation_transfer_results,
            },
        }

    except Exception as e:
        celery_logger.error(f"Error during transfer_to_production for table '{monitor_result.get('table_name', 'UNKNOWN')}': {str(e)}", exc_info=True)
        raise

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
def cancel_processing_job(job_id: str) -> Dict[str, Any]:
    """Cancel processing job associated with main_job_id."""
    try:

        celery.control.revoke(job_id, terminate=True, signal="SIGUSR1")

        status_update = {
            "status": "cancelled",
            "phase": "Job Cancelled by User",
            "progress": 0,
            "error": "User initiated cancellation.",
        }
        update_job_status(job_id, status_update)  # Update Redis status

        return status_update
    except Exception as e:
        celery_logger.error(f"Error cancelling job {job_id}: {str(e)}")
        update_job_status(
            job_id, {"status": "error", "phase": "Cancellation Failed", "error": str(e)}
        )
        raise
