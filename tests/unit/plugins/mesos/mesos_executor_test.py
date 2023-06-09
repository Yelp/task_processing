import mock
import pytest

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.plugins.mesos.mesos_executor import MesosExecutorCallbacks


@pytest.fixture
def mock_callbacks():
    return (MesosExecutorCallbacks(mock.Mock(), mock.Mock(), mock.Mock()),)


@pytest.fixture
def mesos_executor(request, mock_callbacks, mock_Thread, mock_fw_and_driver):
    dummy_executor = MesosExecutor("role", callbacks=mock_callbacks)

    def mesos_executor_teardown():
        dummy_executor.stop()

    request.addfinalizer(mesos_executor_teardown)

    return dummy_executor


def test_creates_execution_framework_and_driver(
    mock_callbacks,
    mock_Thread,
    mesos_executor,
    mock_fw_and_driver,
):
    execution_framework, mesos_driver = mock_fw_and_driver
    assert mesos_executor.execution_framework is execution_framework.return_value
    assert execution_framework.call_args == mock.call(
        name="taskproc-default",
        task_staging_timeout_s=240,
        initial_decline_delay=1.0,
        pool=None,
        role="role",
        callbacks=mock_callbacks,
        framework_id=None,
    )

    assert mesos_executor.driver is mesos_driver.return_value
    assert mesos_driver.call_args == mock.call(
        sched=execution_framework.return_value,
        framework=execution_framework.return_value.framework_info,
        use_addict=True,
        master_uri="127.0.0.1:5050",
        implicit_acknowledgements=False,
        principal="taskproc",
        secret=None,
        failover=False,
    )

    assert mock_Thread.call_args == mock.call(
        target=mesos_executor._run_driver, args=()
    )


def test_run_passes_task_to_execution_framework(mesos_executor):
    mesos_executor.run("task")
    assert mesos_executor.execution_framework.enqueue_task.call_args == mock.call(
        "task"
    )


def test_stop_shuts_down_properly(mesos_executor):
    mesos_executor.stop()
    assert mesos_executor.execution_framework.stop.call_count == 1
    assert mesos_executor.driver.stop.call_count == 1
    assert mesos_executor.driver.join.call_count == 1


def test_event_queue(mocker, mesos_executor):
    q = mocker.Mock()
    mesos_executor.execution_framework.event_queue = q
    assert mesos_executor.get_event_queue() is q


def test_kill_returns(mesos_executor):
    result = mesos_executor.kill("task")
    assert result == mesos_executor.execution_framework.kill_task.return_value
    assert mesos_executor.execution_framework.kill_task.call_args == mock.call("task")


def test_reconcile(mesos_executor):
    mesos_executor.reconcile("task")
    assert mesos_executor.execution_framework.reconcile_task.call_args == mock.call(
        "task"
    )
