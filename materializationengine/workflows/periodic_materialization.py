"""
Create frozen dataset.
"""
import json
import os
from typing import List

from celery.utils.log import get_task_logger
from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.celery_init import celery
from materializationengine.database import sqlalchemy_cache
from dynamicannotationdb.models import AnalysisVersion
from materializationengine.shared_tasks import check_if_task_is_running
from materializationengine.utils import get_config_param
from materializationengine.workflows.complete_workflow import run_complete_workflow

celery_logger = get_task_logger(__name__)


def _get_datastacks() -> List:
    raise NotImplementedError


@celery.task(name="workflow:run_periodic_materialization")
def run_periodic_materialization(
    days_to_expire: int = None, merge_tables: bool = True
) -> None:
    """
    Run complete materialization workflow. Steps are as follows:
    1. Find missing segmentation data in a given datastack and lookup.
    2. Update expired root ids
    3. Copy database to new frozen version
    4. Merge annotation and segmentation tables together
    5. Drop non-materializied tables
    """
    is_update_roots_running = check_if_task_is_running(
        "workflow:update_database_workflow", "worker.workflow"
    )
    if is_update_roots_running:
        return "Update Roots Workflow is running, delaying materialization until update roots is complete."
    try:
        datastacks = json.loads(os.environ["DATASTACKS"])
    except:
        datastacks = get_config_param("DATASTACKS")

    for datastack in datastacks:
        try:
            celery_logger.info(f"Start periodic materialization job for {datastack}")

            datastack_info = get_datastack_info(datastack)
            aligned_volume = datastack_info["aligned_volume"]["name"]

            session = sqlalchemy_cache.get(aligned_volume)
            max_databases = get_config_param("MAX_DATABASES")

            valid_databases = (
                session.query(AnalysisVersion)
                .filter(AnalysisVersion.valid == True)
                .order_by(AnalysisVersion.time_stamp)
                .count()
            )
            if valid_databases >= max_databases:
                return f"Number of valid materialized databases is {valid_databases}, threshold is set to: {max_databases}"
            datastack_info["database_expires"] = True
            task = run_complete_workflow.s(
                datastack_info, days_to_expire=days_to_expire, merge_tables=merge_tables
            )
            task.apply_async(kwargs={"Datastack": datastack})
        except Exception as e:
            celery_logger.error(e)
            raise e
    return True
