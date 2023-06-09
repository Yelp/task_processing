import pytest

from task_processing.task_processor import TaskProcessor


def test_load_plugin():
    tp = TaskProcessor()
    tp.load_plugin("tests.mock_plugin")

    assert "mock_plugin" in tp.registry.plugin_modules
    assert "dummy" in tp.registry.task_executors
    assert "dummy2" in tp.registry.task_executors

    with pytest.raises(ValueError):
        tp.load_plugin("tests.mock_plugin")


def test_executor_from_config():
    tp = TaskProcessor()
    tp.load_plugin("tests.mock_plugin")

    executor = tp.executor_from_config(
        provider="dummy", provider_config={"arg": "foobar"}
    )
    assert executor.arg == "foobar"
    executor.run(None)
    executor.kill(None)

    with pytest.raises(ValueError):
        tp.executor_from_config("lol")
