#!/usr/bin/env python3
import logging
import threading
import time

from twisted.internet import reactor

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.async import Async
from task_processing.runners.async import EventHandler

logging.basicConfig()

task_runner = None
task_config = None


def deal_with_status_update(update):
    print(update)


def reactor_runner():
    for _ in range(40):
        reactor.callLater(10, run_container, 2000000)
    reactor.run()


def run_container(job_config):
    global task_runner
    logging.warning('Launching a new container at {}'.format(time.time()))
    task_runner.run(get_new_container_config())
    reactor.callLater(10, run_container, 2000000)


def get_new_container_config():
    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    return TaskConfig(
        image='ubuntu:16.04',
        cmd='/bin/sleep 5'
    )


def main():
    # Iniditialize the task processor so we can run containers on it
    executor = MesosExecutor(
        secret='bee5aeJibee5aeJibee5aeJi',
        mesos_address='10.40.1.51:5050',
        role='*'
    )

    runner = Async(
        executor,
        [EventHandler(
            predicate=lambda x: x.terminal,
            cb=deal_with_status_update
        )],
    )
    global task_runner
    task_runner = runner

    # Start the scheduler which will schedule all the events in the series
    thread = threading.Thread(target=reactor_runner, args=())
    thread.start()
    # Block on the thread
    thread.join()

    # Block forever
    while True:
        time.sleep(10)
    runner.stop()


if __name__ == '__main__':
    exit(main())
