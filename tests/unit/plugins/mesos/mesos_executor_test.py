import mock
import pytest

from task_processing.plugins.mesos.mesos_executor import AbstractMesosExecutor


class DummyMesosExecutor(AbstractMesosExecutor):
    def get_tasks_for_offer(self, task_configs, offer):
        pass

    def process_status_update(self, update, task_config):
        pass


@pytest.fixture
def mesos_executor(request, mock_Thread, mock_fw_and_driver):
    dummy_executor = DummyMesosExecutor('role')

    def mesos_executor_teardown():
        dummy_executor.stop()
    request.addfinalizer(mesos_executor_teardown)

    return dummy_executor


def test_creates_execution_framework_and_driver(mock_Thread, mesos_executor, mock_fw_and_driver):
    execution_framework, mesos_driver = mock_fw_and_driver
    assert mesos_executor.execution_framework is execution_framework.return_value
    assert execution_framework.call_args == mock.call(
        name="taskproc-default",
        task_staging_timeout_s=240,
        initial_decline_delay=1.0,
        pool=None,
        role="role",
        callback_interface=mesos_executor,
    )

    assert mesos_executor.driver is mesos_driver.return_value
    assert mesos_driver.call_args == mock.call(
        sched=execution_framework.return_value,
        framework=execution_framework.return_value.framework_info,
        use_addict=True,
        master_uri='127.0.0.1:5050',
        implicit_acknowledgements=False,
        principal='taskproc',
        secret=None,
    )

    assert mock_Thread.call_args == mock.call(
        target=mesos_executor.driver.run,
        args=()
    )


def test_run_passes_task_to_execution_framework(mesos_executor):
    mesos_executor.run("task")
    assert mesos_executor.execution_framework.enqueue_task.call_args ==\
        mock.call("task")


def test_stop_shuts_down_properly(mesos_executor):
    mesos_executor.stop()
    assert mesos_executor.execution_framework.stop.call_count == 1
    assert mesos_executor.driver.stop.call_count == 1
    assert mesos_executor.driver.join.call_count == 1


def test_event_queue(mocker, mesos_executor):
    q = mocker.Mock()
    mesos_executor.execution_framework.event_queue = q
    assert mesos_executor.get_event_queue() is q
