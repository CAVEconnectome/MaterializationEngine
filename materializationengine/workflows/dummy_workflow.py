import time

from celery import chain, chord
from celery.utils.log import get_task_logger

from materializationengine.celery_init import celery
from materializationengine.shared_tasks import fin

celery_logger = get_task_logger(__name__)


@celery.task(name="process:start_test_workflow")
def start_test_workflow(iterator_length: int = 50):
    """Test workflow for exploring scaling in kubernetes
    Args:
        iterator_length (int): Number of parallel tasks to run. Default = 50
    """
    workflow = []

    for i in range(0, 3):
        test_workflow = chain(
            chord(
                [chain(dummy_task.si(i)) for i in range(0, iterator_length)], fin.si()
            ),  # return here is required for chords
            fin.si(),
        )  # final task which will process a return status/timing etc...

        test_workflow_2 = chain(
            chord(
                [chain(dummy_task.si(i)) for i in range(0, iterator_length)], fin.si()
            ),  # return here is required for chords
            fin.si(),
        )

        update_roots_and_freeze = chain(
            chord([dummy_task.si(i) for i in range(0, iterator_length)], fin.si()),
            dummy_task.si(i),
            chord(
                [chain(dummy_task.si(i)) for i in range(0, iterator_length)], fin.si()
            ),
            fin.si(),
        )  # final task which will process a return status/timing etc...

        test_complete_workflow = chain(
            test_workflow, test_workflow_2, update_roots_and_freeze
        )
        workflow.append(test_complete_workflow)

    final_workflow = chord(workflow, final_task.s())
    status = final_workflow.apply_async()
    return status


@celery.task(
    name="process:dummy_task",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def dummy_task(self, i):
    time.sleep(1)
    return True


@celery.task(
    name="process:dummy_arg_task",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def dummy_arg_task(self, arg: str = None):
    time.sleep(1)
    return arg


@celery.task(
    name="process:final_task",
    bind=True,
    acks_late=True,
    autoretry_for=(Exception,),
    max_retries=3,
)
def final_task(self, *args, **kwargs):
    time.sleep(1)
    return "FINAL TASK"
