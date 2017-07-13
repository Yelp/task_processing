#!/usr/bin/env python3
import argparse
import logging
import os

from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.runners.sync import Sync

logging.basicConfig()


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
        default='task-proc',
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
        mesos_address = os.environ.get('MESOS', '127.0.0.1:5050')
    else:
        mesos_address = args.master

    if not args.secret:
        with open('./examples/cluster/secret') as f:
            secret = f.read().strip()
    else:
        secret = args.secret

    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(image="busybox", cmd='/bin/true')
    executor = MesosExecutor(
        secret=secret,
        mesos_address=mesos_address,
        pool=args.pool,
        role=args.role
    )
    runner = Sync(executor)
    result = runner.run(task_config)
    print(result)
    print(result.raw)
    runner.stop()

    return 0 if result.success else 1


if __name__ == "__main__":
    exit(main())
