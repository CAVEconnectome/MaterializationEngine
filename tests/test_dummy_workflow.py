from materializationengine.workflows.dummy_workflow import (
    dummy_arg_task,
    dummy_task,
    final_task,
)


def test_dummy_task():
    result = dummy_task.s(1).apply()
    assert result.get() == True


def test_dummy_arg_task():
    result = dummy_arg_task.s("test_arg").apply()
    assert result.get() == "test_arg"


def test_final_task():
    result = final_task.s().apply()
    assert result.get() == "FINAL TASK"
