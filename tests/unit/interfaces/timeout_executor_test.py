from unittest.mock import MagicMock

from task_processing.plugins.mesos.timeout_executor import TimeoutExecutor


def test_executor_creation():
    t = TimeoutExecutor(downstream_executor=MagicMock())
    assert t
    t.kill('fake_task_id')
    t.stop()
