#!/usr/bin/env python3
import argparse
import logging
import os

from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def parse_sync_args():
    parser = argparse.ArgumentParser(
        description='Runs a synchronous task processing task'
    )
    parser.add_argument(
        '-m', '--master',
        dest="master",
        help="mesos master address"
    )
    parser.add_argument(
        '-p', '--pool',
        dest="pool",
        help="mesos resource pool to use"
    )
    parser.add_argument(
        '-r', '--role',
        dest="role",
        default='*',
        help="mesos reservation role to use"
    )
    parser.add_argument(
        '-s', '--secret',
        dest="secret",
        help="mesos secret to use"
    )

    args = parser.parse_args()
    return args


def main():
    args = parse_sync_args()
    if not args.master:
        # mesos_address = os.environ.get('MESOS', '10.40.1.51:5050')
        mesos_address = os.environ.get('MESOS', '169.254.255.254:20716')
    else:
        mesos_address = args.master

    if not args.secret:
        with open('./examples/cluster/secret') as f:
            secret = f.read().strip()
    else:
        secret = args.secret

    processor = TaskProcessor()
    processor.load_plugin(provider_module='task_processing.plugins.mesos')
    executor = processor.executor_from_config(
        provider='mesos',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'pool': args.pool,
            'role': args.role,
        }
    )

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(image="busybox", cmd='/bin/true')

    runner = Sync(executor)
    result = runner.run(task_config)
    print(result)
    print(result.raw)
    runner.stop()

    return 0 if result.success else 1


if __name__ == "__main__":
    exit(main())
