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
        print("task %s finished" % (event.task_id))
        self.terminated += 1


def main():
    c = Counter()
    args = parse_args()
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    mesos_executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': args.secret,
            'mesos_address': args.master,
            'pool': args.pool,
            'role': args.role,
        }
    )

    TaskConfig = mesos_executor.TASK_CONFIG_INTERFACE
    runner = Async(
        mesos_executor,
        [EventHandler(
            predicate=lambda x: x.terminal,
            cb=c.process_event,
        )]
    )
    timeout_task_config = TaskConfig(
        image='busybox',
        cmd='exec /bin/sleep 100',
        offer_timeout=5.0,
        cpus=20,
        mem=2048,
        disk=2000,
    )
    runner.run(timeout_task_config)

    for _ in range(50):
        if c.terminated >= 1:
            break
        print("waiting for task %s to finish" % (timeout_task_config.task_id))
        time.sleep(2)

    runner.stop()
    return 0


if __name__ == "__main__":
    exit(main())
