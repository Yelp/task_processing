import threading

import mock
import pytest

import task_processing.plugins.mesos.mesos_executor as me_module
from task_processing.plugins.mesos.translator import mesos_status_to_event


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def mesos_executor(mocker, request, mock_Thread):
    mocker.patch.object(me_module, 'ExecutionFramework')
    mocker.patch.object(me_module, 'MesosSchedulerDriver')

    ef = me_module.ExecutionFramework.return_value
    fi = mocker.Mock()
    ef.framework_info = fi
    me = me_module.MesosExecutor("role")

    def mesos_executor_teardown():
        me.stop()
    request.addfinalizer(mesos_executor_teardown)

    return me


def test_creates_execution_framework_and_driver(mock_Thread, mesos_executor):
    ef = me_module.ExecutionFramework.return_value
    assert mesos_executor.execution_framework is ef
    assert me_module.ExecutionFramework.call_args == mock.call(
        name="taskproc-default",
        task_staging_timeout_s=60,
        initial_decline_delay=1.0,
        translator=mesos_status_to_event,
        pool=None,
        role="role"
    )

    msd = me_module.MesosSchedulerDriver.return_value
    assert mesos_executor.driver is msd
    assert me_module.MesosSchedulerDriver.call_args == mock.call(
        sched=ef,
        framework=ef.framework_info,
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
