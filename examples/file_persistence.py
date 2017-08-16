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
        provider='mesos',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'role': 'task-proc',
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
    task_ids = [runner.new_task_id() for _ in range(2)]
    TaskConfig = mesos_executor.TASK_CONFIG_INTERFACE

    for task_id in task_ids:
        runner.run(TaskConfig(image='busybox', cmd='/bin/true'), task_id)
    runner.stop()

    persistor = FilePersistence(output_file='/tmp/foo')
    for task_id in task_ids:
        events = persistor.read(task_id)
        assert events[0].task_id == task_id
        assert events[-1].task_id == task_id
        assert events[-1].terminal
        assert events[-1].success is not None

    print('OK')


if __name__ == '__main__':
    exit(main())
