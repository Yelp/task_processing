#!/usr/bin/env python3
import logging
import time

from common import parse_args

from task_processing.runners.async_runner import Async
from task_processing.runners.async_runner import EventHandler
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


class Counter(object):
    def __init__(self):
        self.terminated = 0

    def process_event(self, event):
        self.terminated += 1


def main():
    args = parse_args()

    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': args.secret,
            'mesos_address': args.master,
            'role': args.role,
        }
    )

    counter = Counter()
    runner = Async(
        executor,
        [EventHandler(
            predicate=lambda x: x.terminal,
            cb=counter.process_event
        )]
    )

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    tasks_to_launch = 2
    for _ in range(tasks_to_launch):
        task_config = TaskConfig(image='busybox', cmd='/bin/true')
        runner.run(task_config)

    for _ in range(5):
        print('terminated {} tasks'.format(counter.terminated))
        if counter.terminated >= tasks_to_launch:
            break
        time.sleep(2)

    runner.stop()
    return 0 if counter.terminated >= tasks_to_launch else 1


if __name__ == '__main__':
    exit(main())
