import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from celery import chain
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from flask import current_app
from redis import Redis
from materializationengine.info_client import (
    get_datastack_info,
)
from materializationengine.blueprints.upload.processor import SchemaProcessor
from materializationengine.blueprints.upload.storage import StorageService
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache
from materializationengine.utils import get_config_param
from materializationengine.workflows.spatial_lookup import run_spatial_lookup_workflow

logger = get_task_logger(__name__)

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
    datastack_name: str,
    table_name: str,
    schema_type: str,
    column_mapping: Dict[str, str],
    reference_table: str = None,
    ignored_columns: List[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Chain CSV processing tasks"""
    job_id = f"csv_processing_{datetime.now(timezone.utc)}"
    datastack_info = get_datastack_info(datastack_name)

    chunk_scale_factor = current_app.config.get("CHUNK_SCALE_FACTOR")
    use_staging_database = current_app.config.get("USE_STAGING_DATABASE")
    sql_instance_name = current_app.config.get("SQLALCHEMY_DATABASE_URI")

    workflow = chain(
        process_csv.s(
            file_path,
            datastack_name,
            table_name,
            schema_type,
            column_mapping,
            reference_table=reference_table,
            ignored_columns=ignored_columns,
        ),
        upload_to_database.s(
            sql_instance_name=sql_instance_name,
            database_name=kwargs.get("database_name"),
            table_name=table_name,
        ),
        run_spatial_lookup_workflow.s(
            datastack_info,
            table_name=table_name,
            chunk_scale_factor=chunk_scale_factor,
            get_root_ids=True,
            upload_to_database=True,
            use_staging_database=use_staging_database,
        ),
    )

    # Execute chain
    result = workflow.apply_async(task_id=job_id)
    return {"job_id": job_id}


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


@celery.task(name="process:process_csv")
def process_csv(
    file_path: str,
    datastack_name: str,
    table_name: str,
    schema_type: str,
    column_mapping: Dict[str, str],
    reference_table: str = None,
    ignored_columns: List[str] = None,
    chunk_size: int = 10000,
) -> Dict[str, Any]:
    """Process CSV file in chunks and upload to bucket"""
    try:
        processor = SchemaProcessor(
            schema_type,
            reference_table,
            column_mapping=column_mapping,
            ignored_columns=ignored_columns,
        )
        bucket_name = current_app.config.get("MATERIALIZATION_UPLOAD_BUCKET_PATH")
        
        storage_service = StorageService(bucket_name)
        url = storage_service.create_upload_session()

        file_name = file_path.split("/")[-1]
        storage_service.initiate_upload(url, file_name)

        timestamp = datetime.now(timezone.utc)
        output_path = f"processed_{schema_type}_{timestamp}.csv"

        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            processed_data = processor.process_chunk(chunk)
            storage_service.upload_chunk(url, processed_data)

        return {
            "status": "completed",
            "processed_rows": processor.processed_rows,
            "output_path": output_path,
        }
    except Exception as e:
        logger.error(f"Error processing CSV: {str(e)}")
        raise
      


@celery.task(name="process:upload_to_db")
def upload_to_database(
    process_result: Dict[str, Any],
    sql_instance_name: str,
    database_name: str,
    table_name: str,
) -> Dict[str, Any]:
    """Upload processed CSV to database"""
    try:
        load_command = [
            "gcloud",
            "sql",
            "import",
            "csv",
            sql_instance_name,
            process_result["output_path"],
            "--database",
            database_name,
            "--table",
            table_name,
            "--user",
            "postgres",
        ]

        result = subprocess.run(load_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(f"Database upload failed: {result.stderr}")

        return {
            "status": "completed",
            "rows_uploaded": process_result["processed_rows"],
            "output_path": process_result["output_path"],
        }
    except Exception as e:
        logger.error(f"Database upload failed: {str(e)}")
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
        logger.error(f"Error cancelling job: {str(e)}")
        raise
