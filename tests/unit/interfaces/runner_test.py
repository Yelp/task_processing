import pytest

from task_processing.interfaces.runner import Runner


def test_abstract_interface():
    with pytest.raises(TypeError):
        class TestRunner(Runner):
            pass
        TestRunner()
