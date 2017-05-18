#!/usr/bin/env python
import logging
import os
import time

from task_processing.interfaces.task_executor import TaskConfig
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
        credential_secret_file="/src/examples/cluster/secret",
        mesos_address=mesos_address
    )

    counter = Counter()
    runner = Async(executor, [EventHandler(matcher_func=lambda x: x.terminal, cb=counter.process_event)])


    tasks_to_launch = 2
    for _ in range(tasks_to_launch):
        task_config = TaskConfig(image="busybox",
                                 cmd="echo hi")
        runner.run(task_config)

    while True:
        print('terminated {} tasks'.format(counter.terminated))
        if counter.terminated >= tasks_to_launch:
            return
        time.sleep(10)

    runner.stop()


if __name__ == "__main__":
    exit(main())
