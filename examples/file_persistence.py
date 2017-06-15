#!/usr/bin/env python
import logging
import os

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.plugins.persistence.file_persistence import FilePersistence
from task_processing.plugins.stateful.stateful_executor import StatefulTaskExecutor
from task_processing.runners.sync import Sync

logging.basicConfig()


def main():
    mesos_address = '0.0.0.0:32843'
    mesos_executor = MesosExecutor(
        credential_secret_file='./examples/cluster/secret',
        mesos_address=mesos_address,
        role='task-proc'
    )
    executor = StatefulTaskExecutor(
        downstream_executor=mesos_executor,
        persister=FilePersistence(
            output_file='foo'
        )
    )
    runner = Sync(executor=executor)
    tasks = set()
    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    for _ in range(1, 2):
        task_config = TaskConfig(
            image='ubuntu:14.04', cmd='/bin/sleep 2'
        )
        tasks.add(task_config.task_id)
        runner.run(task_config)

    print('Running {} tasks: {}'.format(len(tasks), tasks))


if __name__ == '__main__':
    main()
    exit(0)
