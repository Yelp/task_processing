#!/usr/bin/env python3
import logging
import os

from task_processing.plugins.persistence.file_persistence \
    import FilePersistence
from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    mesos_address = os.getenv('MESOS', 'mesosmaster:5050')
    with open('./examples/cluster/secret') as f:
        secret = f.read().strip()

    processor = TaskProcessor()
    for p in ['mesos', 'stateful']:
        processor.load_plugin(provider_module='task_processing.plugins.' + p)
    mesos_executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'role': 'taskproc',
        }
    )
    executor = processor.executor_from_config(
        provider='stateful',
        provider_config={
            'downstream_executor': mesos_executor,
            'persister': FilePersistence(output_file='/tmp/foo')
        }
    )

    runner = Sync(executor=executor)
    tasks = set()
    TaskConfig = mesos_executor.TASK_CONFIG_INTERFACE
    for _ in range(1, 2):
        task_config = TaskConfig(image='busybox', cmd='/bin/true')
        tasks.add(task_config.task_id)
        runner.run(task_config)
        print(executor.status(task_config.task_id))


if __name__ == '__main__':
    exit(main())
