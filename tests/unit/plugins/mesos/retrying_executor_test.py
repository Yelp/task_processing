import threading
from queue import Queue

import mock
import pytest

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.retrying_executor import RetryingExecutor
from task_processing.plugins.mesos.task_config import MesosTaskConfig
# from task_processing.plugins.mesos.translator import mesos_status_to_event


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def mock_retrying_executor(mock_Thread):
    return RetryingExecutor(
        downstream_executor=mock.Mock(),
    )


def _get_mock_task_config():
    return MesosTaskConfig(image='mock_image', cmd='mock_cmd', retries=5)


def _get_mock_event(is_terminal=False):
    mock_config = _get_mock_task_config()
    return Event(
        kind='task',
        timestamp=1234.5678,
        terminal=is_terminal,
        success=False,
        task_id=mock_config.task_id,
        platform_type='mesos',
        message='mock_message',
        task_config=mock_config,
        raw='raw_event'
    )


def test_task_retry(mock_retrying_executor):
    mock_event = _get_mock_event()
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 3)
    mock_retrying_executor.run = mock.Mock()

    mock_retrying_executor.retry(mock_event)

    assert mock_retrying_executor.task_retries[mock_event.task_id] == 2
    assert mock_retrying_executor.run.call_count == 1


def test_retries_exhaused(mock_retrying_executor):
    mock_event = _get_mock_event()
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)
    mock_retrying_executor.run = mock.Mock()

    mock_retrying_executor.retry(mock_event)

    assert mock_retrying_executor.task_retries[mock_event.task_id] == 0
    assert mock_retrying_executor.run.call_count == 1


def test_task_config_with_retry(mock_retrying_executor):
    mock_task_config = _get_mock_task_config()
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_task_config.task_id, 2)

    ret_value = mock_retrying_executor._task_config_with_retry(
        mock_task_config
    )

    assert '-retry2' in ret_value.task_id


def test_restore_task_id(mock_retrying_executor):
    mock_event = _get_mock_event()
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)
    task_config = mock_event.task_config
    modified_task_config = task_config.set(
        'uuid',
        str(mock_event.task_config.uuid) + '-retry1'
    )
    mock_event = mock_event.set(
        'task_config',
        modified_task_config
    )

    ret_value = mock_retrying_executor._restore_task_id(
        mock_event,
        original_task_id
    )

    assert mock_event.task_id == ret_value.task_id


def test_is_not_current_attempt(mock_retrying_executor):
    mock_event = _get_mock_event()
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 2)
    task_config = mock_event.task_config
    modified_task_id = str(mock_event.task_config.uuid) + '-retry1'
    modified_task_config = task_config.set(
        'uuid',
        modified_task_id
    )
    modified_mock_event = mock_event.set(
        'task_config',
        modified_task_config
    )
    modified_mock_event = mock_event.set(
        'task_id',
        modified_task_id
    )

    ret_value = mock_retrying_executor._is_current_attempt(
        modified_mock_event,
        original_task_id
    )

    assert ret_value is False


def test_is_current_attempt(mock_retrying_executor):
    mock_event = _get_mock_event()
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 2)
    task_config = mock_event.task_config
    modified_task_id = str(mock_event.task_config.uuid) + '-retry2'
    modified_task_config = task_config.set(
        'uuid',
        modified_task_id
    )
    modified_mock_event = mock_event.set(
        'task_config',
        modified_task_config
    )
    modified_mock_event = mock_event.set(
        'task_id',
        modified_task_id
    )

    ret_value = mock_retrying_executor._is_current_attempt(
        modified_mock_event,
        original_task_id
    )

    assert ret_value is True


def test_retry_loop_retries_task(mock_retrying_executor):
    mock_event = _get_mock_event()
    mock_retrying_executor.stopping = True
    mock_retrying_executor._is_current_attempt = mock.Mock(return_value=True)
    mock_retrying_executor.event_with_retries = mock.Mock()
    mock_retrying_executor.retry = mock.Mock(return_value=True)
    mock_retrying_executor.retry_pred = mock.Mock(return_value=True)
    mock_retrying_executor.src_queue = Queue()
    mock_retrying_executor.src_queue.put(mock_event)
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 0


def test_retry_loop_does_not_retry_task(mock_retrying_executor):
    mock_event = _get_mock_event(is_terminal=True)
    mock_retrying_executor.stopping = True
    mock_retrying_executor._is_current_attempt = mock.Mock(return_value=True)
    mock_retrying_executor.retry = mock.Mock(return_value=False)
    mock_retrying_executor.retry_pred = mock.Mock(return_value=False)
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)
    modified_task_id = mock_event.task_id + '-retry1'
    modified_mock_event = mock_event.set(
        'task_id',
        modified_task_id
    )
    mock_retrying_executor.src_queue = Queue()
    mock_retrying_executor.src_queue.put(modified_mock_event)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 1
    assert len(mock_retrying_executor.task_retries) == 0


def test_retry_loop_filters_out_non_task(mock_retrying_executor):
    mock_event = Event(
        kind='control',
        raw='some message',
        message='stop',
        terminal=True
    )

    mock_retrying_executor.stopping = True
    mock_retrying_executor._is_current_attempt = mock.Mock(return_value=True)
    mock_retrying_executor.event_with_retries = mock.Mock()
    mock_retrying_executor.retry = mock.Mock(return_value=True)
    mock_retrying_executor.retry_pred = mock.Mock(return_value=True)
    mock_retrying_executor.src_queue = Queue()
    mock_retrying_executor.src_queue.put(mock_event)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 1
