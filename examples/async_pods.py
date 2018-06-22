#!/usr/bin/env python3
import logging
import time

from common import parse_args

from task_processing.plugins.mesos.config import MesosPodConfig
from task_processing.plugins.mesos.config import MesosTaskConfig
from task_processing.runners.async import Async
from task_processing.runners.async import EventHandler
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

    logging.info('hello')
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos_pod',
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

    tasks_to_launch = 5
    tasks = [MesosTaskConfig(image='busybox', cmd='/bin/true') for i in range(tasks_to_launch)]
    pod_config = MesosPodConfig(tasks=tasks)
    runner.run(pod_config)

    for _ in range(tasks_to_launch):
        print('terminated {} tasks'.format(counter.terminated))
        if counter.terminated >= tasks_to_launch:
            break
        time.sleep(2)

    runner.stop()
    return 0 if counter.terminated >= tasks_to_launch else 1


if __name__ == '__main__':
    exit(main())
