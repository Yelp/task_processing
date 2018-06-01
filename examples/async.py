#!/usr/bin/env python3
import logging
import time

from common import parse_args
from pyrsistent import v

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

    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos',
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
    PodConfig = executor.POD_CONFIG_INTERFACE
    tasks_to_launch = 5
    task_configs = []

    pod_config = PodConfig(
        tasks=v(
            TaskConfig(containerizer='MESOS', cni_network='cni-test',
                       image='alpine', cmd='/bin/sleep 1200'),
            TaskConfig(containerizer='MESOS', cni_network='cni-test',
                       image='alpine', cmd='/bin/sleep 1200'),
            TaskConfig(containerizer='MESOS', cni_network='cni-test',
                       image='alpine', cmd='/bin/sleep 1200'),
            TaskConfig(containerizer='MESOS', cni_network='cni-test',
                       image='alpine', cmd='/bin/sleep 1200'),
            TaskConfig(containerizer='MESOS', cni_network='cni-test',
                       image='alpine', cmd='/bin/sleep 1200'),
        )
    )
    runner.run(pod_config)
    # runner.run(TaskConfig(containerizer='MESOS', image='busybox', cmd='/bin/sleep 120'))
    time.sleep(20)

    while True:
        if counter.terminated >= tasks_to_launch:
            break
        time.sleep(2)

    runner.stop()
    return 0 if counter.terminated >= tasks_to_launch else 1


if __name__ == '__main__':
    exit(main())
