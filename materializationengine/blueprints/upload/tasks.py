import json
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from celery import chain
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from flask import current_app
from redis import Redis

from materializationengine.blueprints.upload.gcs_processor import GCSCsvProcessor
from materializationengine.blueprints.upload.processor import SchemaProcessor
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache
from materializationengine.index_manager import index_cache
from materializationengine.shared_tasks import add_index
from materializationengine.utils import get_config_param
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

    chunk_scale_factor = current_app.config.get("CHUNK_SCALE_FACTOR")
    use_staging_database = current_app.config.get("USE_STAGING_DATABASE")
    sql_instance_name = current_app.config.get("SQL_INSTANCE_NAME")

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
            datastack_info,
            table_name=table_name,
            chunk_scale_factor=chunk_scale_factor,
            get_root_ids=True,
            upload_to_database=True,
            use_staging_database=use_staging_database,
        ),
    )

    result = workflow.apply_async()
    return {"job_id": result.id}


@celery.task(name="process:get_status")
def get_chain_status(job_id: str) -> Dict[str, Any]:
    """Get status of processing chain"""
    result = AsyncResult(job_id)
    if result.ready():
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

        gcs_processor = GCSCsvProcessor(
            bucket_name, chunk_size=chunk_size
        )

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


@celery.task(name="process:upload_to_db")
def upload_to_database(
    process_result: Dict[str, Any],
    sql_instance_name: str,
    file_metadata: Dict[str, Any],
    datastack_info: str,
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
        flat_segmentation_source = file_metadata["metadata"].get("flat_segmentation_source")
        table_metadata = file_metadata["metadata"].get("table_metadata")    
        notice_text = file_metadata["metadata"].get("notice_text")

        staging_database = current_app.config.get("STAGING_DATABASE_NAME")
        bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")

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
                notice_text=notice_text
            )
        except Exception as e:
            celery_logger.error(f"Error creating table: {str(e)}")
            

        # create table in staging database and drop indices
        index_cache.drop_table_indices(table_name, db_client.database.engine)

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
            f"gs://{process_result['output_path']}",
            "--database",
            staging_database,
            "--table",
            table_name,
            "--user",
            "postgres",
        ]
        try:

            # auth_process = subprocess.run(auth_command, check=True, capture_output=True, text=True)
            db_process = subprocess.run(load_command, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as e:
            celery_logger.error(f"Database upload failed: {e}, db_process: {e.stderr}")
            raise e
        
        schema_model = db_client.schema.create_annotation_model(table_name, schema)


        anno_indices = index_cache.add_indices_sql_commands(table_name, schema_model, db_client.database.engine)
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
