import threading

import mock
import pytest

from task_processing.plugins.mesos.logging_executor import MesosLoggingExecutor
from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def mock_downstream():
    return mock.MagicMock()


@pytest.fixture
def mock_logging_executor(mock_Thread, mock_downstream):
    return MesosLoggingExecutor(downstream_executor=mock_downstream)


def test_run(mock_logging_executor, mock_downstream):
    mock_config = MesosTaskConfig(image='fake', cmd='cat')
    mock_logging_executor.run(mock_config)
    assert mock_downstream.run.call_count == 1


def test_kill(mock_logging_executor, mock_downstream):
    result = mock_logging_executor.kill("task")
    assert result == mock_downstream.kill.return_value
    assert mock_downstream.kill.call_args == mock.call("task")


def test_reconcile(mock_logging_executor, mock_downstream):
    mock_logging_executor.reconcile("task")
    assert mock_downstream.reconcile.call_args == mock.call("task")


def test_stop(mock_logging_executor, mock_downstream):
    mock_logging_executor.stop()
    assert mock_downstream.stop.call_args == mock.call()
    assert mock_logging_executor.stopping
