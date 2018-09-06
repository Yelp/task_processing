import threading

import mock
import pytest

from task_processing.plugins.stateful.stateful_executor import StatefulTaskExecutor


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def mock_downstream():
    return mock.MagicMock()


@pytest.fixture
def mock_persister():
    return mock.MagicMock()


@pytest.fixture
def mock_stateful_executor(mock_Thread, mock_downstream, mock_persister):
    return StatefulTaskExecutor(
        downstream_executor=mock_downstream,
        persister=mock_persister,
    )


def test_run(mock_stateful_executor, mock_downstream):
    mock_config = mock.MagicMock()
    mock_stateful_executor.run(mock_config)
    assert mock_downstream.run.call_count == 1


def test_kill(mock_stateful_executor, mock_downstream):
    result = mock_stateful_executor.kill("task")
    assert result == mock_downstream.kill.return_value
    assert mock_downstream.kill.call_args == mock.call("task")


def test_reconcile(mock_stateful_executor, mock_downstream):
    mock_stateful_executor.reconcile("task")
    assert mock_downstream.reconcile.call_args == mock.call("task")


def test_stop(mock_stateful_executor, mock_downstream):
    mock_stateful_executor.stop()
    assert mock_downstream.stop.call_args == mock.call()
