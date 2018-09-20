import time
from queue import Queue

import mock
import pytest

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.timeout_executor import TaskEntry
from task_processing.plugins.mesos.timeout_executor import TimeoutExecutor


@pytest.fixture
def mock_Thread():
    with mock.patch('task_processing.plugins.mesos.timeout_executor.Thread'):
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
def mock_timeout_executor(mock_Thread, mock_downstream):
    return TimeoutExecutor(downstream_executor=mock_downstream)


@pytest.fixture
def mock_task_config():
    return MesosTaskConfig(
        uuid='mock_uuid',
        name='mock_name',
        image='mock_image',
        cmd='mock_cmd',
        timeout=1000,
    )


@pytest.fixture
def mock_entry(mock_task_config):
    return TaskEntry(
        task_id=mock_task_config.task_id,
        deadline=mock_task_config.timeout + 2000,
    )


@pytest.fixture
def mock_event(mock_task_config):
    return Event(
        kind='task',
        timestamp=1234.5678,
        terminal=True,
        task_id=mock_task_config.task_id,
        platform_type='mesos',
        message='mock_message',
        task_config=mock_task_config,
        raw='raw_event',
    )


# timeout_loop #################################################################
def test_timeout_loop_nontask(
    mock_timeout_executor,
    mock_event,
):
    mock_event = mock_event.set('kind', 'control')
    mock_entry = TaskEntry('different_id', deadline=1234)
    mock_timeout_executor.stopping = True
    mock_timeout_executor.src_queue.put(mock_event)
    mock_timeout_executor.running_tasks.append(mock_entry)
    time.time = mock.Mock(return_value=0)

    mock_timeout_executor.timeout_loop()

    assert len(mock_timeout_executor.running_tasks) == 1


def test_timeout_loop_terminal_task_timed_out(
    mock_timeout_executor,
    mock_event,
    mock_entry,
):
    mock_timeout_executor.stopping = True
    mock_timeout_executor.src_queue.put(mock_event)
    mock_timeout_executor.running_tasks.append(mock_entry)
    mock_timeout_executor.killed_tasks.append(mock_entry.task_id)
    mock_timeout_executor.downstream_executor.kill = mock.Mock()

    mock_timeout_executor.timeout_loop()

    assert mock_timeout_executor.downstream_executor.kill.call_count == 0
    assert len(mock_timeout_executor.running_tasks) == 0
    assert len(mock_timeout_executor.killed_tasks) == 0


def test_timeout_loop_existing_nonterminal_task(
    mock_timeout_executor,
    mock_event,
    mock_entry,
):
    mock_event = mock_event.set('terminal', False)
    mock_timeout_executor.stopping = True
    mock_timeout_executor.src_queue.put(mock_event)
    mock_timeout_executor.running_tasks.append(mock_entry)
    mock_timeout_executor.downstream_executor.kill = mock.Mock()
    time.time = mock.Mock(return_value=10000)

    mock_timeout_executor.timeout_loop()

    assert mock_timeout_executor.downstream_executor.kill.call_args ==\
        mock.call(mock_entry.task_id)
    assert len(mock_timeout_executor.running_tasks) == 0
    assert len(mock_timeout_executor.killed_tasks) == 1


def test_timeout_loop_nonexistent_nonterminal_task(
    mock_timeout_executor,
    mock_event,
    mock_entry,
):
    mock_event = mock_event.set('terminal', False)
    mock_timeout_executor.stopping = True
    mock_timeout_executor.src_queue.put(mock_event)
    mock_timeout_executor.downstream_executor.kill = mock.Mock()
    time.time = mock.Mock(return_value=10000)

    mock_timeout_executor.timeout_loop()

    assert mock_timeout_executor.downstream_executor.kill.call_args ==\
        mock.call(mock_entry.task_id)
    assert len(mock_timeout_executor.running_tasks) == 0
    assert len(mock_timeout_executor.killed_tasks) == 1


# run ##########################################################################
def test_run(mock_timeout_executor, mock_downstream):
    mock_config = MesosTaskConfig(image='fake', cmd='cat', timeout=60)
    mock_timeout_executor.run(mock_config)
    assert mock_downstream.run.call_count == 1

    assert len(mock_timeout_executor.running_tasks) == 1


# reconcile ####################################################################
def test_reconcile(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.reconcile("task")
    assert mock_downstream.reconcile.call_args == mock.call("task")


# kill #########################################################################
def test_kill_existing_task(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.running_tasks = [TaskEntry("task", 10)]
    mock_timeout_executor.downstream_executor.kill = mock.Mock(
        return_value=True)

    result = mock_timeout_executor.kill("task")

    assert result == mock_downstream.kill.return_value
    assert mock_downstream.kill.call_args == mock.call("task")
    assert len(mock_timeout_executor.running_tasks) == 0
    assert len(mock_timeout_executor.killed_tasks) == 1


# stop #########################################################################
def test_stop(mock_timeout_executor, mock_downstream):
    mock_timeout_executor.stop()
    assert mock_downstream.stop.call_args == mock.call()
    assert mock_timeout_executor.stopping


# _insert_new_running_task_entry ###############################################
def test_insert_new_running_task_entry_enumerate(mock_timeout_executor):
    mock_entry_one = TaskEntry('fake_entry_one', 1)
    mock_entry_two = TaskEntry('fake_entry_two', 2)
    mock_entry_three = TaskEntry('fake_entry_three', 3)
    mock_timeout_executor.running_tasks.append(mock_entry_one)
    mock_timeout_executor.running_tasks.append(mock_entry_three)

    mock_timeout_executor._insert_new_running_task_entry(mock_entry_two)

    assert [entry.deadline for entry in mock_timeout_executor.running_tasks] ==\
        [1, 2, 3]


def test_insert_new_running_task_entry_append(mock_timeout_executor):
    mock_entry_one = TaskEntry('fake_entry_one', 1)
    mock_entry_two = TaskEntry('fake_entry_two', 2)
    mock_entry_three = TaskEntry('fake_entry_three', 3)
    mock_timeout_executor.running_tasks.append(mock_entry_one)
    mock_timeout_executor.running_tasks.append(mock_entry_two)

    mock_timeout_executor._insert_new_running_task_entry(mock_entry_three)

    assert [entry.deadline for entry in mock_timeout_executor.running_tasks] ==\
        [1, 2, 3]
