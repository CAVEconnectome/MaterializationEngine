from taskqueue import TaskQueue
from materializationengine.app import create_app
from materializationengine.utils import get_config_param

application = create_app()

with application.app_context():
    from materializationengine.workflows.spatial_lookup import (
        process_spatially_chunked_svids_func,
    )

    tq = TaskQueue(get_config_param("TASKQUEUE_QURL"))
    tq.poll(
        lease_seconds=int(
            get_config_param("TASKQUEUE_TIMEOUT")
        )  # print out a success message
    )
