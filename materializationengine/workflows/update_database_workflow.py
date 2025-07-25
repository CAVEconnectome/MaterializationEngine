import datetime
import json
import os

from celery import chain
from celery.utils.log import get_task_logger
from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.celery_init import celery
from materializationengine.shared_tasks import (
    get_materialization_info,
    monitor_workflow_state,
    workflow_complete,
    fin,
)
from materializationengine.task import LockedTask
from materializationengine.utils import get_config_param
from materializationengine.workflows.ingest_new_annotations import (
    ingest_new_annotations_workflow,
    find_missing_root_ids_workflow,
)
from materializationengine.workflows.update_root_ids import (
    update_root_ids_workflow,
)

celery_logger = get_task_logger(__name__)


@celery.task(name="orchestration:run_periodic_database_update")
def run_periodic_database_update(datastack: str = None) -> None:
    """
    Run update database workflow. Steps are as follows:
    1. Find missing segmentation data in a given datastack and lookup.
    2. Update expired root ids

    """
    if datastack:
        datastacks = [datastack]
    else:
        try:
            datastacks = json.loads(os.environ["DATASTACKS"])
        except Exception:
            datastacks = get_config_param("DATASTACKS")

    for datastack in datastacks:
        try:
            celery_logger.info(f"Start periodic database update job for {datastack}")
            datastack_info = get_datastack_info(datastack)
            task = update_database_workflow.s(datastack_info)
            task.apply_async(kwargs={"Datastack": datastack})
        except Exception as e:
            celery_logger.error(e)
            raise e
    return True


@celery.task(o
    bind=True,
    base=LockedTask,
    timeout=int(60 * 60 * 24),  # Task locked for 1 day
    name="orchestration:update_database_workflow",
)
def update_database_workflow(self, datastack_info: dict, **kwargs):
    """Updates 'live' database:
        - Find all annotations with missing segmentation rows
        and lookup supervoxel_id and root_id
        - Lookup all expired root_ids and update them

    Args:
        datastack_info (dict): [description]
        days_to_expire (int, optional): [description]. Defaults to 5.
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    mat_info = get_materialization_info(
        datastack_info=datastack_info,
        analysis_version=None,
        materialization_time_stamp=materialization_time_stamp,
    )
    celery_logger.info(mat_info)

    update_live_database_workflow = []
    celery_logger.debug(mat_info)

    # lookup missing segmentation data for new annotations and update expired root_ids
    # skip tables that are larger than 1,000,000 rows due to performance.
    try:
        for mat_metadata in mat_info:
            if mat_metadata.get("segmentation_table_name"):
                workflow = chain(
                    ingest_new_annotations_workflow(mat_metadata),
                    # find_missing_root_ids_workflow(mat_metadata), # skip for now
                    update_root_ids_workflow(mat_metadata),
                )

                update_live_database_workflow.append(workflow)
            else:
                update_live_database_workflow.append(fin.si())

        run_update_database_workflow = chain(
            *update_live_database_workflow, workflow_complete.si("update_root_ids")
        ).apply_async(kwargs={"Datastack": datastack_info["datastack"]})
    except Exception as e:
        celery_logger.error(f"An error has occurred: {e}")
        raise e
    tasks_completed = monitor_workflow_state(run_update_database_workflow)
    if tasks_completed:
        return True
