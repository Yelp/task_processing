#!/usr/bin/env python3
import logging
import os

from boto3 import session
from botocore.errorfactory import ClientError

from task_processing.plugins.persistence.dynamodb_persistence \
    import DynamoDBPersister
from task_processing.runners.sync import Sync
from task_processing.task_processor import TaskProcessor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)


def main():
    mesos_address = os.getenv('MESOS', 'mesosmaster:5050')
    with open('./examples/cluster/secret') as f:
        secret = f.read().strip()

    processor = TaskProcessor()
    for p in ['mesos', 'stateful']:
        processor.load_plugin(provider_module='task_processing.plugins.' + p)
    mesos_executor = processor.executor_from_config(
        provider='mesos_task',
        provider_config={
            'secret': secret,
            'mesos_address': mesos_address,
            'role': 'taskproc',
        }
    )

    s = session.Session(
        region_name='foo',
        aws_access_key_id='foo',
        aws_secret_access_key='bar'
    )
    dynamo_address = os.getenv('DYNAMO', 'http://dynamodb:5050')
    client = s.client(
        service_name='dynamodb',
        endpoint_url=dynamo_address,
    )
    try:
        create_table(client)
    except ClientError:
        pass

    executor = processor.executor_from_config(
        provider='stateful',
        provider_config={
            'downstream_executor': mesos_executor,
            'persister': DynamoDBPersister(
                table_name='events',
                endpoint_url=dynamo_address,
                session=s
            )
        }
    )
    runner = Sync(executor=executor)
    tasks = set()
    TaskConfig = mesos_executor.TASK_CONFIG_INTERFACE
    for _ in range(1, 2):
        task_config = TaskConfig(
            image='ubuntu:14.04', cmd='/bin/sleep 2'
        )
        tasks.add(task_config.task_id)
        runner.run(task_config)
        print(executor.status(task_config.task_id))


def create_table(client):
    return client.create_table(
        TableName='events',
        KeySchema=[
            {
                'AttributeName': 'task_id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'timestamp',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'task_id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp',
                'AttributeType': 'N'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 123,
            'WriteCapacityUnits': 123
        },
    )


if __name__ == '__main__':
    exit(main())
