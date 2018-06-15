import threading

import mock
import pytest
from addict import Dict

from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def fake_task():
    return MesosTaskConfig(
        name='fake_name',
        cpus=10.0,
        mem=1024.0,
        disk=1000.0,
        gpus=1,
        image='fake_image',
        cmd='echo "fake"'
    )


@pytest.fixture
def fake_offer():
    return Dict(
        id=Dict(value='fake_offer_id'),
        agent_id=Dict(value='fake_agent_id'),
        hostname='fake_hostname',
        resources=[
            Dict(
                role='fake_role',
                name='cpus',
                scalar=Dict(value=10),
                type='SCALAR',
            ),
            Dict(
                role='other_fake_role',
                name='cpus',
                scalar=Dict(value=20),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='mem',
                scalar=Dict(value=1024),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='disk',
                scalar=Dict(value=1000),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='gpus',
                scalar=Dict(value=1),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='ports',
                ranges=Dict(range=[Dict(begin=31200, end=31500)]),
                type='RANGES',
            ),
        ],
        attributes=[
            Dict(
                name='pool',
                text=Dict(value='fake_pool_text')
            ),
            Dict(
                name='region',
                text=Dict(value='fake_region_text'),
            ),
        ]
    )


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def mock_fw_and_driver():
    with mock.patch(
        'task_processing.plugins.mesos.mesos_executor.ExecutionFramework'
    ) as mock_execution_framework, mock.patch(
        'task_processing.plugins.mesos.mesos_executor.MesosSchedulerDriver'
    ) as mock_scheduler_driver:
        mock_execution_framework.return_value.framework_info = mock.Mock()
        yield mock_execution_framework, mock_scheduler_driver
