#!/usr/bin/env python3
import logging
import os

from six.moves.queue import Empty
from six.moves.queue import Queue

from task_processing.runners.subscription import Subscription
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    mesos_address = os.environ['MESOS']
    with open('./examples/cluster/secret') as f:
        secret = f.read().strip()

    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'role': 'taskproc',
        }
    )

    queue = Queue(100)
    runner = Subscription(executor, queue)

    tasks = set()
    TaskConfig = executor.TASK_CONFIG_INTERFACE
    for _ in range(2):
        task_config = TaskConfig(image='busybox', cmd='/bin/true')
        tasks.add(task_config.task_id)
        runner.run(task_config)

    print('Running {} tasks: {}'.format(len(tasks), tasks))
    while len(tasks) > 0:
        try:
            event = queue.get(block=True, timeout=10)
        except Empty:
            event = None

        if event is None:
            print('Timeout while waiting for {}'.format(tasks))
            break
        else:
            if event.terminal:
                tasks.discard(event.task_id)

    runner.stop()
    return 0 if len(tasks) == 0 else 1


if __name__ == '__main__':
    exit(main())
