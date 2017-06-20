import pytest

import task_processing.plugins.mesos.mesos_executor as me_module
from task_processing.plugins.mesos.translator import mesos_status_to_event


@pytest.fixture
def mesos_executor(mocker, request):
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


def test_creates_execution_framework_and_driver(mesos_executor):
    ef = me_module.ExecutionFramework.return_value
    assert mesos_executor.execution_framework is ef
    me_module.ExecutionFramework.assert_called_with(
        name="test",
        task_staging_timeout_s=60,
        translator=mesos_status_to_event,
        role="role"
    )

    msd = me_module.MesosSchedulerDriver.return_value
    assert mesos_executor.driver is msd
    me_module.MesosSchedulerDriver.assert_called_with(
        sched=ef,
        framework=ef.framework_info,
        use_addict=True,
        master_uri='127.0.0.1:5050',
        implicit_acknowledgements=False,
        principal='taskproc',
        secret=None,
    )

    mesos_executor.driver.run.assert_called_with()


def test_run_passes_task_to_execution_framework(mesos_executor):
    mesos_executor.run("task")
    mesos_executor.execution_framework.enqueue_task.assert_called_with("task")


def test_stop_shuts_down_properly(mesos_executor):
    mesos_executor.stop()
    mesos_executor.execution_framework.stop.assert_called_with()
    mesos_executor.driver.stop.assert_called_with()
    mesos_executor.driver.join.assert_called_with()


def test_event_queue(mocker, mesos_executor):
    q = mocker.Mock()
    mesos_executor.execution_framework.task_update_queue = q
    assert mesos_executor.event_queue() is q
