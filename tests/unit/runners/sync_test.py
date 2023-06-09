import mock
import pytest

from task_processing.runners.sync import Sync


@pytest.fixture
def fake_executor():
    return mock.Mock()


@pytest.fixture
def fake_runner(fake_executor):
    runner = Sync(
        executor=fake_executor,
    )
    yield runner
    runner.stop()


def test_run(fake_runner, fake_executor):
    task_id = "some_task"
    event = mock.Mock(
        task_id=task_id,
        terminal=True,
    )
    fake_executor.get_event_queue.return_value.get.return_value = event
    assert fake_runner.run(mock.Mock(task_id=task_id)) == event
    assert fake_executor.run.call_count == 1


def test_run_stop_event(fake_runner, fake_executor):
    task_id = "some_task"
    event = mock.Mock(
        kind="control",
        message="stop",
    )
    fake_executor.get_event_queue.return_value.get.return_value = event
    assert fake_runner.run(mock.Mock(task_id=task_id)) == event
    assert fake_executor.run.call_count == 1


def test_run_other_task(fake_runner, fake_executor):
    task_id = "some_task"
    incorrect_event = mock.Mock(
        task_id="other",
        terminal=True,
    )
    correct_event = mock.Mock(
        task_id=task_id,
        terminal=True,
    )
    fake_executor.get_event_queue.return_value.get.side_effect = [
        incorrect_event,
        correct_event,
    ]
    assert fake_runner.run(mock.Mock(task_id=task_id)) == correct_event
    assert fake_executor.run.call_count == 1


def test_reconcile(fake_runner, fake_executor):
    fake_runner.reconcile(mock.Mock())
    assert fake_executor.reconcile.call_count == 1


def test_kill(fake_runner, fake_executor):
    result = fake_runner.kill("some_id")
    assert result == fake_executor.kill.return_value
    assert fake_executor.kill.call_count == 1


def test_stop(fake_runner, fake_executor):
    fake_runner.stop()
    assert fake_executor.stop.call_count == 1
