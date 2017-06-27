#!/usr/bin/env python3
import logging
import os

from boto3 import session

from task_processing.executors.stateful.stateful_executor \
    import StatefulTaskExecutor
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.plugins.persistence.dynamodb_persistence \
    import DynamoDBPersister
from task_processing.runners.sync import Sync

logging.basicConfig()


def main():
    mesos_address = os.getenv('MESOS', 'mesosmaster:5050')
    mesos_executor = MesosExecutor(
        credentials={'principal': 'mesos', 'secret': 'very'},
        mesos_address=mesos_address,
        role='task-proc'
    )
    s = session.Session(
        region_name='foo',
        aws_access_key_id='foo',
        aws_secret_access_key='bar'
    )
    dynamo_address = os.getenv('DYNAMO', 'http://dynamodb:5050')
    executor = StatefulTaskExecutor(
        downstream_executor=mesos_executor,
        persister=DynamoDBPersister(
            table_name='events',
            endpoint_url=dynamo_address,
            session=s
        )
    )
    runner = Sync(executor=executor)
    tasks = set()
    TaskConfig = MesosExecutor.TASK_CONFIG_INTERFACE
    for _ in range(1, 2):
        task_config = TaskConfig(
            image='ubuntu:14.04', cmd='/bin/sleep 2'
        )
        tasks.add(task_config.task_id)
        runner.run(task_config)
        print(executor.status(task_config.task_id))


if __name__ == '__main__':
    exit(main())
