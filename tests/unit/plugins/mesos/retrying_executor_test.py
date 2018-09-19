from queue import Queue

import mock
import pytest

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.retrying_executor import RetryingExecutor
from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def mock_Thread():
    with mock.patch('task_processing.plugins.mesos.retrying_executor.Thread'):
        yield


@pytest.fixture
def source_queue():
    return Queue()


@pytest.fixture
def mock_downstream(source_queue):
    executor = mock.MagicMock()
    executor.get_event_queue.return_value = source_queue
    return executor


@pytest.fixture
def mock_retrying_executor(mock_Thread, mock_downstream):
    return RetryingExecutor(
        downstream_executor=mock_downstream,
        retries=2,
    )


@pytest.fixture
def mock_task_config():
    return MesosTaskConfig(
        uuid='mock_uuid',
        name='mock_name',
        image='mock_image',
        cmd='mock_cmd',
        retries=5,
    )


@pytest.fixture
def mock_event(mock_task_config, is_terminal=False):
    return Event(
        kind='task',
        timestamp=1234.5678,
        terminal=is_terminal,
        success=False,
        task_id=mock_task_config.task_id,
        platform_type='mesos',
        message='mock_message',
        task_config=mock_task_config,
        raw='raw_event'
    )


# task_retry ###################################################################
def test_task_retry(mock_retrying_executor, mock_event):
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 3)
    mock_retrying_executor.run = mock.Mock()

    mock_retrying_executor.retry(mock_event)

    assert mock_retrying_executor.task_retries[mock_event.task_id] == 2
    assert mock_retrying_executor.run.call_count == 1


def test_task_retry_retries_exhausted(mock_retrying_executor, mock_event):
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 0)
    mock_retrying_executor.run = mock.Mock()

    retry_attempted = mock_retrying_executor.retry(mock_event)

    assert mock_retrying_executor.task_retries[mock_event.task_id] == 0
    assert mock_retrying_executor.run.call_count == 0
    assert not retry_attempted


# retry_loop ###################################################################
def test_retry_loop_retries_task(mock_retrying_executor, mock_event):
    mock_event = mock_event.set('terminal', True)
    mock_retrying_executor.stopping = True
    mock_retrying_executor._is_current_attempt = mock.Mock(return_value=True)
    mock_retrying_executor._restore_task_id = mock.Mock(
        return_value=mock_event)
    mock_retrying_executor.retry = mock.Mock(return_value=True)
    mock_retrying_executor.retry_pred = mock.Mock(return_value=True)
    mock_retrying_executor.src_queue.put(mock_event)
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 0
    assert mock_retrying_executor.retry.call_count == 1


def test_retry_loop_does_not_retry_task(mock_retrying_executor, mock_event):
    mock_event = mock_event.set('terminal', True)
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
    mock_retrying_executor.src_queue.put(mock_event)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 1


# If retrying_executor receives an event about an attempt for a task the
# executor does not know about, it should add the task into task_retries
# and assume the event's attempt is the current attempt
def test_retry_loop_recover_attempt(mock_retrying_executor, mock_event):
    original_task_id = mock_event.task_id
    modified_mock_event = mock_event.set(
        'task_id',
        original_task_id + '-retry6'
    )
    modified_mock_event = modified_mock_event.set('terminal', True)
    mock_retrying_executor.stopping = True
    mock_retrying_executor.retry = mock.Mock(return_value=True)
    mock_retrying_executor.retry_pred = mock.Mock(return_value=True)
    mock_retrying_executor.src_queue.put(modified_mock_event)

    mock_retrying_executor.retry_loop()

    assert mock_retrying_executor.dest_queue.qsize() == 0
    assert mock_retrying_executor.retry.call_count == 1
    assert mock_retrying_executor.task_retries[original_task_id] == 6


# run ##########################################################################
def test_run(mock_retrying_executor, mock_downstream, mock_task_config):
    mock_retrying_executor.run(mock_task_config)

    assert mock_downstream.run.call_count == 1
    assert mock_retrying_executor.task_retries[mock_task_config.task_id] == 5

    # Config should be the same, except with retry number appended
    config_with_retry = mock_downstream.run.call_args[0][0]
    assert config_with_retry.task_id == mock_task_config.task_id + '-retry5'
    assert config_with_retry.cmd == mock_task_config.cmd
    assert config_with_retry.image == mock_task_config.image


def test_run_default_retries(mock_retrying_executor, mock_downstream):
    mock_config = MesosTaskConfig(image='fake_image', cmd='some command')
    mock_retrying_executor.run(mock_config)
    assert mock_downstream.run.call_count == 1

    assert mock_retrying_executor.task_retries[mock_config.task_id] == 2


# reconcile ####################################################################
def test_reconcile(mock_retrying_executor, mock_downstream):
    mock_retrying_executor.reconcile("task")

    assert mock_downstream.reconcile.call_args == mock.call("task")


# kill #########################################################################
def test_kill(mock_retrying_executor, mock_downstream):
    result = mock_retrying_executor.kill("task")

    assert result == mock_downstream.kill.return_value
    assert mock_downstream.kill.call_args == mock.call("task")
    assert mock_retrying_executor.task_retries["task"] == -1


# stop #########################################################################
def test_stop(mock_retrying_executor, mock_downstream):
    mock_retrying_executor.stop()

    assert mock_downstream.stop.call_args == mock.call()
    assert mock_retrying_executor.stopping is True


# _task_config_with_retry ######################################################
def test_task_config_with_retry(mock_retrying_executor, mock_task_config):
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_task_config.task_id, 2)

    ret_value = mock_retrying_executor._task_config_with_retry(
        mock_task_config
    )

    assert ret_value.task_id == mock_task_config.task_id + '-retry2'


# _restore_task_id #############################################################
def test_restore_task_id(mock_retrying_executor, mock_event):
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 1)
    modified_task_config = mock_event.task_config.set(
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


# _is_current_attempt ##########################################################
def test_is_current_attempt(
    mock_retrying_executor,
    mock_event,
    mock_task_config,
):
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 2)
    modified_task_id = str(mock_event.task_config.uuid) + '-retry2'
    modified_task_config = mock_event.task_config.set(
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


def test_is_not_current_attempt(mock_retrying_executor, mock_event):
    original_task_id = mock_event.task_id
    mock_retrying_executor.task_retries = mock_retrying_executor.\
        task_retries.set(mock_event.task_id, 2)
    modified_task_id = str(mock_event.task_config.uuid) + '-retry1'
    modified_task_config = mock_event.task_config.set(
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


def test_is_unknown_attempt(mock_retrying_executor, mock_event):
    original_task_id = mock_event.task_id
    modified_task_id = str(mock_event.task_config.uuid) + '-retry8'
    modified_task_config = mock_event.task_config.set(
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
        original_task_id,
    )

    assert ret_value is True
    assert mock_retrying_executor.task_retries.get(original_task_id) == 8
