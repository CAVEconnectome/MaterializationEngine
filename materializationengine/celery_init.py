from celery import Celery

celery = Celery(
    include=[
        "materializationengine.workflows.ingest_new_annotations",
        "materializationengine.workflows.create_frozen_database",
        "materializationengine.workflows.update_root_ids",
        "materializationengine.workflows.bulk_upload",
        "materializationengine.workflows.complete_workflow",
        "materializationengine.workflows.dummy_workflow",
        "materializationengine.workflows.periodic_database_removal",
        "materializationengine.workflows.periodic_materialization",
        "materializationengine.workflows.spatial_lookup",
        "materializationengine.shared_tasks",
        "materializationengine.views",
        "materializationengine.monitor",
        "materializationengine.blueprints.upload.tasks",
    ]
)
