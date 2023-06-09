import pytest

from task_processing.interfaces.task_executor import TaskExecutor


def test_abstract_interface():
    with pytest.raises(TypeError):

        class TestExecutor(TaskExecutor):
            pass

        TestExecutor()
