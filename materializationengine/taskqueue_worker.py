from taskqueue import TaskQueue
from materializationengine.workflows.spatial_lookup import (
    process_spatially_chunked_svids_func,
)

from materializationengine.app import create_app
from materializationengine.celery_worker import create_celery
from materializationengine.utils import get_config_param

application = create_app()

with application.app_context():
    tq = TaskQueue(get_config_param("TASKQUEUE_QURL"))
    tq.poll(
        lease_seconds=int(
            get_config_param("TASKQUEUE_TIMEOUT")
        )  # print out a success message
    )
