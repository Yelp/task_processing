#!/usr/bin/env python
import logging
import os

from six.moves.queue import Empty
from six.moves.queue import Queue

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.subscription import Subscription

logging.basicConfig()


def main():
    mesos_address = os.environ['MESOS']
    executor = MesosExecutor(
        credential_secret_file='./examples/cluster/secret',
        mesos_address=mesos_address,
        role='task-proc'
    )

    queue = Queue(100)
    runner = Subscription(executor, queue)

    tasks = set()
    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    for _ in range(1, 20):
        task_config = TaskConfig(
            image='ubuntu:14.04', cmd='/bin/sleep 2'
        )
        tasks.add(task_config.task_id)
        runner.run(task_config)

    print('Running {} tasks: {}'.format(len(tasks), tasks))
    while len(tasks) > 0:
        try:
            event = queue.get(block=True, timeout=10)
        except Empty:
            event = None

        if event is None:
            print(
                'Timeout on subscription queue, still waiting for {}'.format(
                    tasks
                )
            )
        else:
            if event.terminal:
                tasks.discard(event.task_id)

    runner.stop()


if __name__ == '__main__':
    exit(main())
