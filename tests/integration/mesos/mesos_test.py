from pytest_bdd import given
from pytest_bdd import then
from pytest_bdd import when

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync


@given('mesos executor with {runner} runner')
def mesos_executor_runner(runner):
    executor = MesosExecutor(role='mock-role')

    if runner == 'sync':
        runner_instance = Sync(executor=executor)
    else:
        raise "unknown runner: {}".format(runner)

    return {'executor': executor, 'runner': runner_instance}


@when('I launch a task')
def launch_task(mesos_executor_runner):
    print(mesos_executor_runner)
    return


@then('it should block until finished')
def block_until_finished():
    return
