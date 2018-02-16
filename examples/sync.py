#!/usr/bin/env python3
import logging

from common import parse_args

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
            'secret': '',
            'mesos_address': '10.40.21.26:5050',
            'pool': None,
            'role': '*',
        }
    )

    TaskConfig = executor.TASK_CONFIG_INTERFACE
    task_config = TaskConfig(
        image="nginx",
        # image="trinitronx/python-simplehttpserver",
        # cmd='cat /etc/*release && ifconfig && /bin/sleep 12000',
        cmd='apt-get update && apt-get install -y net-tools curl procps && which curl && nginx && netstat -ntlp && ifconfig -a && /bin/sleep 123456',
        # cmd='/bin/sleep 12000',
        # volumes=[{
        #     'container_path': '/some_file',
        #     'host_path': '/nail/home/sagarp',
        #     'mode': 'RW',
        # }]
    )
    # This only works on agents that have added mesos as a containerizer
    # task_config = TaskConfig(containerizer='MESOS', cmd='/bin/true')

    runner = Sync(executor)
    result = runner.run(task_config)
    print(result)
    print(result.raw)
    runner.stop()

    return 0 if result.success else 1


if __name__ == "__main__":
    exit(main())
