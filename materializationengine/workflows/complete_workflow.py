import datetime

from celery import chain, chord
from celery.utils.log import get_task_logger
from materializationengine.celery_init import celery
from materializationengine.shared_tasks import (
    generate_chunked_model_ids,
    fin,
    get_materialization_info,
)
from materializationengine.workflows.create_frozen_database import (
    check_tables,
    create_materializied_database_workflow,
    create_new_version,
    format_materialization_database_workflow,
    rebuild_reference_tables,
)
from materializationengine.workflows.ingest_new_annotations import (
    ingest_new_annotations_workflow,
)
from materializationengine.workflows.update_root_ids import (
    get_expired_root_ids,
    update_root_ids_workflow,
)


celery_logger = get_task_logger(__name__)


@celery.task(name="process:run_complete_workflow")
def run_complete_workflow(datastack_info: dict, days_to_expire: int = 5):
    """Run complete materialization workflow.
    Workflow overview:
        - Find all annotations with missing segmentation rows
        and lookup supervoxel_id and root_id
        - Lookup all expired root_ids and update them
        - Copy the database to a new versioned database
        - Merge annotation and segmentation tables

    Args:
        datastack_info (dict): [description]
        days_to_expire (int, optional): [description]. Defaults to 5.
    """
    materialization_time_stamp = datetime.datetime.utcnow()

    new_version_number = create_new_version(
        datastack_info, materialization_time_stamp, days_to_expire
    )

    mat_info = get_materialization_info(
        datastack_info, new_version_number, materialization_time_stamp
    )
    celery_logger.info(mat_info)

    update_live_database_workflow = []

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
                update_live_database_workflow.append(fin.si())

    # copy live database as a materialized version and drop unneeded tables
    setup_versioned_database_workflow = create_materializied_database_workflow(
        datastack_info, new_version_number, materialization_time_stamp, mat_info
    )

    # drop indices, merge annotation and segmentation tables and re-add indices on merged table
    format_database_workflow = format_materialization_database_workflow(mat_info)

    # combine all workflows into final workflow and run
    final_workflow = chain(
        chord(update_live_database_workflow, fin.si()),
        setup_versioned_database_workflow,
        chord(format_database_workflow, fin.si()),
        rebuild_reference_tables.si(mat_info),
        check_tables.si(mat_info, new_version_number),
    )
    final_workflow.apply_async()
