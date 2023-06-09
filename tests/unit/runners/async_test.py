import mock
import pytest

from task_processing.runners.async_runner import Async


@pytest.fixture
def fake_executor():
    return mock.Mock()


@pytest.fixture
def fake_runner(fake_executor):
    runner = Async(
        executor=fake_executor,
        callbacks=[mock.Mock()],
    )
    yield runner
    runner.stop()


def test_run(fake_runner, fake_executor):
    fake_runner.run(mock.Mock())
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
