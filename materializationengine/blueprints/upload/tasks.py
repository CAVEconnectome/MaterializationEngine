import json
import os
import subprocess
from datetime import datetime, timezone
from typing import Any, Dict, List

import pandas as pd
from celery.utils.log import get_task_logger
from flask import current_app
from redis import Redis

from materializationengine.blueprints.upload.processor import SchemaProcessor
from materializationengine.celery_init import celery
from materializationengine.database import dynamic_annotation_cache
from materializationengine.utils import get_config_param

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
    schema_type: str,
    column_mapping: Dict[str, str],
    ignored_columns: List[str] = None,
    reference_table: str = None,
    chunk_size: int = 10000,
    sql_instance_name: str = None,
    bucket_name: str = None,
    database_name: str = None,
) -> Dict[str, Any]:
    """Process CSV file in chunks and upload to GCS

    Args:
        file_path: Path to CSV file in GCS
        schema_type: Schema to validate against
        column_mapping: Maps CSV columns to schema fields
        chunk_size: Number of rows per chunk
        sql_instance_name: CloudSQL instance name
        bucket_name: GCS bucket name
        database_name: Target database name
    """
    timestamp = datetime.now(timezone.utc)

    job_id = f"csv_processing_{timestamp}"


    status = {
        "status": "processing",
        "phase": "Starting",
        "progress": 0,
        "processed_rows": 0,
        "total_rows": 0,
        "error": None,
    }
    update_job_status(job_id, status)

    status.update({"phase": "Validating Schema", "progress": 5})
    update_job_status(job_id, status)
    
    processor = SchemaProcessor(
        schema_type=schema_type,
        reference_table=reference_table,
        column_mapping=column_mapping,
        ignored_columns=ignored_columns

    )
    
    output_filename = f"processed_{os.path.basename(file_path)}"
    output_path = f"gs://{bucket_name}/processed/{output_filename}"

    processed_rows = 0
    first_chunk = True

    output_path = f"processed_{schema_type}_{timestamp}.csv"

    try:
        first_chunk = True
        with pd.read_csv(file_path, chunksize=chunk_size) as chunks:
            for chunk_num, chunk in enumerate(chunks):
                logger.info(f"Processing chunk {chunk_num + 1} with {len(chunk)} rows")
                processed_chunk = processor.process_chunk(chunk, timestamp)
                
                processed_chunk.to_csv(
                    output_path,
                    mode='w' if first_chunk else 'a',
                    header=False,
                    index=False
                )
                first_chunk = False
                
                update_job_status(job_id, status)
        
        logger.info(f"Processed file saved to: {output_path}")
        logger.info(f"Columns in processed file: {processor.column_order}")
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}", exc_info=True)
        raise



    stagging_db = current_app.config.get("STAGING_DATABASE_NAME")

    db = dynamic_annotation_cache.get_db(stagging_db)

  
    load_command = [
        "gcloud",
        "sql",
        "import",
        "csv",
        sql_instance_name,
        output_path,
        "--database",
        database_name,
        "--table",
        schema_type,
    ]

    result = subprocess.run(load_command, capture_output=True, text=True)

    if result.returncode != 0:
        raise Exception(f"Database upload failed: {result.stderr}")

    status.update(
        {
            "status": "completed",
            "phase": "Complete",
            "progress": 100,
            "processed_rows": processed_rows,
            "output_path": output_path,
        }
    )
    update_job_status(job_id, status)

    return {
        "job_id": job_id,
        "status": "completed",
        "total_rows": processed_rows,
        "output_path": output_path,
    }


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
