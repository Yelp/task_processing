#!/usr/bin/env python3
import logging

from common import parse_args
from pyrsistent import v

from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    args = parse_args()
    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos',
        provider_config={
            'secret': args.secret,
            'mesos_address': args.master,
            'pool': args.pool,
            'role': args.role,
        }
    )

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    PodConfig = executor.POD_CONFIG_INTERFACE
    # task_config = TaskConfig(image="busybox", cmd='/bin/true')
    t2 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t3 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t4 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t5 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t6 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t7 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t8 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t9 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t10 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t11 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t12 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')
    t13 = TaskConfig(containerizer='MESOS', image="busybox", cmd='/bin/true')

    pod_config = PodConfig(
        tasks=v(t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13))
    # print(pod_config)
    # This only works on agents that have added mesos as a containerizer
    # task_config = TaskConfig(containerizer='DOCKER', image='busybox',  cmd='/bin/sleep 360')

    runner = Sync(executor)
    # result = runner.run(task_config)
    result = runner.run(pod_config)
    print(result)
    print(result.raw)
    runner.stop()

    return 0 if result.success else 1


if __name__ == "__main__":
    exit(main())
