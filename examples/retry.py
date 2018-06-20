#!/usr/bin/env python3
import logging

from common import parse_args

from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    args = parse_args()
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    mesos_executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': args.secret,
            'mesos_address': args.master,
            'pool': args.pool,
            'role': args.role,
        }
    )

    executor = processor.executor_from_config(
        provider='retrying',
        provider_config={
            'downstream_executor': mesos_executor,
        }
    )

    TaskConfig = mesos_executor.TASK_CONFIG_INTERFACE
    runner = Sync(executor=executor)
    task_config = TaskConfig(
        image='docker-dev.yelpcorp.com/dumb-busybox',
        cmd='/bin/false',
        retries=2
    )
    result = runner.run(task_config)
    print(result)

    runner.stop()


if __name__ == "__main__":
    exit(main())
