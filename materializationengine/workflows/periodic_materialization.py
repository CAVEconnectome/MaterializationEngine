"""
Create frozen dataset.
"""
import json
import os
from typing import List

from celery.utils.log import get_task_logger
from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.workflows.complete_workflow import run_complete_workflow
from materializationengine.celery_init import celery
from materializationengine.utils import get_config_param

celery_logger = get_task_logger(__name__)


def _get_datastacks() -> List:
    raise NotImplementedError


@celery.task(name="process:run_periodic_materialization")
def run_periodic_materialization(days_to_expire: int = None) -> None:
    """
    Run complete materialization workflow. Steps are as follows:
    1. Find missing segmentation data in a given datastack and lookup.
    2. Update expired root ids
    3. Copy database to new frozen version
    4. Merge annotation and segmentation tables together
    5. Drop non-materializied tables
    """
    try:
        datastacks = json.loads(os.environ["DATASTACKS"])
    except:
        datastacks = get_config_param("DATASTACKS")

    for datastack in datastacks:
        try:
            celery_logger.info(f"Start periodic materialization job for {datastack}")
            datastack_info = get_datastack_info(datastack)
            datastack_info["database_expires"] = True
            task = run_complete_workflow.s(
                datastack_info, days_to_expire=days_to_expire
            )
            task.apply_async(kwargs={"Datastack": datastack})
        except Exception as e:
            celery_logger.error(e)
            raise e
    return True
