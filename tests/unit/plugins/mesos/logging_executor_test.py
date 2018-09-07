from queue import Queue

import mock
import pytest
from addict import Dict

from task_processing.plugins.mesos.logging_executor import MesosLoggingExecutor
from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def mock_Thread():
    with mock.patch('task_processing.plugins.mesos.logging_executor.Thread'):
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


def test_event_loop_stores_staging_event(mock_logging_executor, source_queue):
    raw = Dict({
        'offer': {
            'url': {
                'scheme': 'http',
                'address': {
                    'ip': '1.2.3.4',
                    'port': 5051,
                },
            },
        },
    })
    mock_event = mock.Mock(
        kind='task',
        platform_type='staging',
        task_id='my_task',
        raw=raw,
    )

    mock_logging_executor.stopping = True
    source_queue.put(mock_event)

    mock_logging_executor.event_loop()
    task_data = mock_logging_executor.staging_tasks['my_task']
    assert task_data == 'http://1.2.3.4:5051'


def test_event_loop_continues_after_unknown_task(mock_logging_executor, source_queue):
    unknown_event = mock.Mock(
        kind='task',
        platform_type='running',
        task_id='new_task',
    )
    other_event = mock.Mock(
        kind='task',
        platform_type='something',
        task_id='other_task',
    )

    mock_logging_executor.stopping = True
    source_queue.put(unknown_event)
    source_queue.put(other_event)

    mock_logging_executor.event_loop()

    dest_queue = mock_logging_executor.get_event_queue()
    assert dest_queue.get() == unknown_event
    assert dest_queue.get() == other_event


def test_event_loop_running_event(mock_logging_executor, source_queue):
    raw = Dict({
        'container_status': {
            'container_id': {
                'value': 'cid',
            },
        },
        'executor_id': {
            'value': 'eid',
        },
    })
    mock_event = mock.Mock(
        kind='task',
        platform_type='running',
        task_id='my_task',
        raw=raw,
    )

    mock_logging_executor.stopping = True
    source_queue.put(mock_event)
    mock_logging_executor.staging_tasks = mock_logging_executor.staging_tasks.set(
        'my_task', 'my_log_url')

    mock_logging_executor.event_loop()
    assert 'my_task' in mock_logging_executor.running_tasks
    assert 'my_task' not in mock_logging_executor.staging_tasks


def test_event_loop_terminal_event(mock_logging_executor, source_queue):
    mock_event = mock.Mock(
        kind='task',
        platform_type='finished',
        task_id='my_task',
        terminal=True,
    )

    mock_logging_executor.stopping = True
    source_queue.put(mock_event)
    mock_logging_executor.running_tasks = mock_logging_executor.running_tasks.set(
        'my_task', mock.Mock())

    mock_logging_executor.event_loop()

    assert 'my_task' in mock_logging_executor.running_tasks
    assert 'my_task' in mock_logging_executor.done_tasks
