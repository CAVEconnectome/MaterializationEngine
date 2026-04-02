import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from redis import Redis

from materializationengine.utils import get_config_param

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


def get_job_status(job_id: str) -> Optional[Dict[str, Any]]:
    """Get job status from Redis"""
    status = REDIS_CLIENT.get(f"csv_processing:{job_id}")
    return json.loads(status) if status else None
