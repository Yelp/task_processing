from unittest.mock import MagicMock

from mock import patch
from six.moves.queue import Queue

from task_processing.interfaces.event import Event
from task_processing.interfaces.runner import new_task_id
from task_processing.plugins.mesos.retrying_executor import RetryingExecutor


def test_executor_creation():
    retrying = RetryingExecutor(executor=MagicMock())
    retrying.stop()


@patch('time.sleep', return_value=None)
def test_executor_grows_id_by_atttempt(_):
    downstream_tasks = Queue()
    downstream_events = Queue()
    downstream = MagicMock(
        run=lambda *args: downstream_tasks.put(args),
        get_event_queue=lambda: downstream_events
    )
    retrying = RetryingExecutor(executor=downstream)
    task_config = {'name': 'fake_task'}
    task_id = new_task_id()

    retrying.run(task_config, task_id)

    downstream_task = downstream_tasks.get(True, 0.01)
    assert (task_config, task_id + ':0') == downstream_task

    downstream_events.put(
        Event(
            kind='task',
            task_id=task_id + ':0',
            task_config=task_config,
            terminal=True,
            success=False,
            raw=MagicMock(),
        )
    )

    downstream_task = downstream_tasks.get(True, 0.01)
    assert (task_config, task_id + ':1') == downstream_task

    retrying.stop()


@patch('time.sleep', return_value=None)
def test_executor_retries_x_times(_):
    downstream_tasks = Queue()
    downstream_events = Queue()
    downstream = MagicMock(
        run=lambda *args: downstream_tasks.put(args),
        get_event_queue=lambda: downstream_events
    )
    retrying = RetryingExecutor(executor=downstream)
    task_config = {'name': 'fake_task'}
    task_id = new_task_id()

    retrying.run(task_config, task_id)
    for i in range(4):
        downstream_events.put(
            Event(
                kind='task',
                task_id=task_id + ':' + str(i),
                task_config=task_config,
                terminal=True,
                success=False,
                raw=MagicMock(),
            )
        )

    retrying_q = retrying.get_event_queue()
    fail_event = retrying_q.get(True, 0.01)
    assert fail_event

    retrying.stop()
