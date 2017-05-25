#!/usr/bin/env python
import logging
import os
import time

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.async import Async
from task_processing.runners.async import EventHandler

logging.basicConfig()


class Counter(object):
    def __init__(self):
        self.terminated = 0

    def process_event(self, event):
        self.terminated += 1


def main():
    mesos_address = os.environ['MESOS']
    executor = MesosExecutor(
        credential_secret_file='./examples/cluster/secret',
        mesos_address=mesos_address,
        role='task-proc'
    )

    counter = Counter()
    runner = Async(
        executor,
        [EventHandler(
            predicate=lambda x: x.terminal,
            cb=counter.process_event
        )]
    )

    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    tasks_to_launch = 2
    for _ in range(tasks_to_launch):
        task_config = TaskConfig(image='busybox',
                                 cmd='echo hi')
        runner.run(task_config)

    while True:
        print('terminated {} tasks'.format(counter.terminated))
        if counter.terminated >= tasks_to_launch:
            break
        time.sleep(10)

    runner.stop()


if __name__ == '__main__':
    exit(main())
