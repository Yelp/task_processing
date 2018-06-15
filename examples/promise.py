#!/usr/bin/env python3
import logging
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait

from common import parse_args

from task_processing.runners.promise import Promise
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    args = parse_args()
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': args.secret,
            'mesos_address': args.master,
            'pool': args.pool,
            'role': args.role,
        }
    )

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(image="busybox", cmd='/bin/true')
    # This only works on agents that have added mesos as a containerizer
    # task_config = TaskConfig(containerizer='MESOS', cmd='/bin/true')

    with ThreadPoolExecutor(max_workers=2) as futures_executor:
        runner = Promise(executor, futures_executor)
        future = runner.run(task_config)
        wait([future])
        result = future.result()
        print(result)
        print(result.raw)
        runner.stop()

    return 0 if result.success else 1


if __name__ == "__main__":
    exit(main())
