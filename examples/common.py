import argparse
import os


def parse_args():
    parser = argparse.ArgumentParser(
        description='Runs a task processing task'
    )

    parser.add_argument(
        '-m', '--master',
        dest="master",
        default=os.environ.get('MESOS', '127.0.0.1:5050'),
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
        default='taskproc',
        help="mesos reservation role to use"
    )

    with open('./examples/cluster/secret') as f:
        default_secret = f.read().strip()

    parser.add_argument(
        '-s', '--secret',
        dest="secret",
        default=default_secret,
        help="mesos secret to use"
    )

    args = parser.parse_args()
    return args
