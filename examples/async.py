#!/usr/bin/env python3
import logging
import time

from common import parse_args

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
            'secret': 'bee5aeJibee5aeJibee5aeJi',
            'mesos_address': 'mesos3-uswest1cdevc.dev.yelpcorp.com:5050',
            'role': '*',
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
    start_time = time.time()

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    tasks_to_launch = 10000
    for _ in range(tasks_to_launch):
        task_config = TaskConfig(
            containerizer='MESOS', image='alpine', cmd='/bin/true && echo "I run successfully"')
        runner.run(task_config)

    while True:
        if counter.terminated >= tasks_to_launch:
            break
        time.sleep(2)

    print('Finished in {secs}'.format(secs=time.time() - start_time))

    runner.stop()
    return 0 if counter.terminated >= tasks_to_launch else 1


if __name__ == '__main__':
    exit(main())
