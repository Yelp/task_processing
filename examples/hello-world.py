#!/usr/bin/env python3
import os

from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

"""Simple hello-world example of how to use Task Processing (taskproc)
"""


def main():
    # get address of the Mesos cluster
    mesos_address = os.getenv('MESOS', 'mesosmaster:5050')

    # read in secret, this is used to authenticate the taskproc scheduler with
    # Mesos
    with open('./examples/cluster/secret') as f:
        secret = f.read().strip()

    # create a processor instance
    processor = TaskProcessor()

    # configure plugins
    processor.load_plugin(provider_module='task_processing.plugins.mesos')

    # create an executor (taskproc executor NOT to be confused with a Mesos
    # executor) using this defined configuration. this config can also be used
    # to specify other Mesos properties, such as which role to use
    executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'role': 'taskproc',
        }
    )

    # creates a new Sync runner that will synchronously execute tasks
    # (i.e. block until completion)
    runner = Sync(executor)

    # next, create a TaskConfig to run
    # this is where properties of the Mesos task can be specified in this
    # example, we use the busybox Docker image and just echo "hello world"
    TaskConfig = executor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(image="busybox", cmd='echo "hello world"')

    # run our task and print the result
    result = runner.run(task_config)
    print(result)

    # this stops the taskproc framework and unregisters it from Mesos
    runner.stop()

    return 0 if result.success else 1


if __name__ == '__main__':
    exit(main())
