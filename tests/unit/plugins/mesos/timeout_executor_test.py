import mock
import pytest

from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.timeout_executor import TaskEntry
from task_processing.plugins.mesos.timeout_executor import TimeoutExecutor


@pytest.fixture
def mock_Thread():
    with mock.patch('task_processing.plugins.mesos.timeout_executor.Thread'):
        yield


@pytest.fixture
def mock_downstream():
    return mock.MagicMock()


@pytest.fixture
def mock_timeout_executor(mock_Thread, mock_downstream):
    return TimeoutExecutor(downstream_executor=mock_downstream)


def test_run(mock_timeout_executor, mock_downstream):
    mock_config = MesosTaskConfig(image='fake', cmd='cat', timeout=60)
    mock_timeout_executor.run(mock_config)
    assert mock_downstream.run.call_count == 1

    assert len(mock_timeout_executor.running_tasks) == 1


def test_kill_existing_task(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.running_tasks = [TaskEntry("task", 10)]
    result = mock_timeout_executor.kill("task")
    assert result == mock_downstream.kill.return_value
    assert mock_downstream.kill.call_args == mock.call("task")


def test_reconcile(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.reconcile("task")
    assert mock_downstream.reconcile.call_args == mock.call("task")


def test_stop(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.stop()
    assert mock_downstream.stop.call_args == mock.call()
    assert mock_timeout_executor.stopping
