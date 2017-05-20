#!/usr/bin/env python
import logging
import os

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync

logging.basicConfig()


def main():
    mesos_address = os.environ.get('MESOS', '127.0.0.1:5050')
    executor = MesosExecutor(
        credential_secret_file="/src/task_processing/examples/cluster/secret",
        mesos_address=mesos_address
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
