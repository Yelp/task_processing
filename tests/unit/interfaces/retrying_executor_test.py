from unittest.mock import MagicMock

from task_processing.plugins.mesos.retrying_executor import RetryingExecutor


def test_event_creation():
    r = RetryingExecutor(executor=MagicMock())
    assert r
    r.stop()
