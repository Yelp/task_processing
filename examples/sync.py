#!/usr/bin/env python3
import logging
import os

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync

logging.basicConfig()


def main():
    mesos_address = os.environ.get('MESOS', '127.0.0.1:5050')
    with open('./examples/cluster/secret') as f:
        secret = f.read().strip()
    executor = MesosExecutor(
        secret=secret,
        mesos_address=mesos_address,
        role='task-proc'
    )

    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(image="ubuntu:14.04", cmd="/bin/sleep 10")
    runner = Sync(executor)
    result = runner.run(task_config)
    print(result)
    print(result.raw)
    runner.stop()


if __name__ == "__main__":
    exit(main())
