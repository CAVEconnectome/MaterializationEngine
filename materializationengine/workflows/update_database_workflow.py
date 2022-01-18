import datetime
import json
import os

from celery import chain, chord
from celery.utils.log import get_task_logger
from materializationengine.blueprints.materialize.api import get_datastack_info
from materializationengine.celery_init import celery
from materializationengine.shared_tasks import (fin,
                                                generate_chunked_model_ids,
                                                get_materialization_info,
                                                workflow_complete)
from materializationengine.utils import get_config_param
from materializationengine.workflows.ingest_new_annotations import \
    ingest_new_annotations_workflow
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids, update_root_ids_workflow)

celery_logger = get_task_logger(__name__)


@celery.task(name="process:run_periodic_database_update")
def run_periodic_database_update() -> None:
    """
    Run update database workflow. Steps are as follows:
    1. Find missing segmentation data in a given datastack and lookup.
    2. Update expired root ids

    """
    try:
        datastacks = json.loads(os.environ["DATASTACKS"])
    except:
        datastacks = get_config_param("DATASTACKS")

    for datastack in datastacks:
        try:
            celery_logger.info(f"Start periodic database update job for {datastack}")
            datastack_info = get_datastack_info(datastack)
            task = update_database_workflow.s(datastack_info)
            task.apply_async()
        except Exception as e:
            celery_logger.error(e)
            raise e
    return True


@celery.task(name="process:update_database_workflow")
def update_database_workflow(datastack_info: dict):
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
    for mat_metadata in mat_info:
        if not mat_metadata["reference_table"]:
            chunked_roots = get_expired_root_ids(mat_metadata)
            if mat_metadata["row_count"] < 1_000_000:
                annotation_chunks = generate_chunked_model_ids(mat_metadata)
                new_annotations = True
                new_annotation_workflow = ingest_new_annotations_workflow(
                    mat_metadata, annotation_chunks
                )
            else:
                new_annotations = None

            if chunked_roots:
                update_expired_roots_workflow = update_root_ids_workflow(
                    mat_metadata, chunked_roots
                )
                if new_annotations:
                    ingest_and_update_root_ids_workflow = chain(
                        new_annotation_workflow, update_expired_roots_workflow
                    )
                    update_live_database_workflow.append(
                        ingest_and_update_root_ids_workflow
                    )
                else:
                    update_live_database_workflow.append(update_expired_roots_workflow)
            elif new_annotations:
                update_live_database_workflow.append(new_annotation_workflow)
            else:
                return "Nothing to update"

    run_update_database_workflow = chain(
        chord(update_live_database_workflow, workflow_complete.si("update_root_ids")),
    )
    run_update_database_workflow.apply_async()
