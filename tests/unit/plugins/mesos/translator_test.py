import addict
import mock
import pytest
from pyrsistent import v

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.translator import make_mesos_task_info
from task_processing.plugins.mesos.translator import MESOS_STATUS_MAP
from task_processing.plugins.mesos.translator import mesos_status_to_event


@pytest.mark.parametrize('gpus_count,containerizer,container', [
    (1.0, 'MESOS', addict.Dict(
        type='MESOS',
        volumes=[addict.Dict(
            container_path='fake_container_path',
            host_path='fake_host_path',
            mode='RO'
        )],
        mesos=addict.Dict(
            image=addict.Dict(
                type='DOCKER',
                docker=addict.Dict(name='fake_image'),
                cached=True,
            ),
        ),
        network_infos=addict.Dict(
            port_mappings=[addict.Dict(host_port=31200, container_port=8888)],
        ),
    )),
    (0, 'DOCKER', addict.Dict(
        type='DOCKER',
        volumes=[addict.Dict(
            container_path='fake_container_path',
            host_path='fake_host_path',
            mode='RO'
        )],
        docker=addict.Dict(
            image='fake_image',
            network='BRIDGE',
            force_pull_image=False,
            port_mappings=[addict.Dict(host_port=31200, container_port=8888)],
            parameters=[],
        ),
    )),
])
def test_make_mesos_task_info(
    fake_task,
    fake_offer,
    gpus_count,
    containerizer,
    container,
):
    consumed_resources = ResourceSet(cpus=10, mem=1024, disk=1000, gpus=gpus_count, ports=v(31200))
    tid = fake_task.task_id
    fake_task = fake_task.set(
        volumes=v(
            addict.Dict(
                mode='RO',
                container_path='fake_container_path',
                host_path='fake_host_path'
            )
        ),
        gpus=gpus_count,
        containerizer=containerizer,
    )

    task_info = make_mesos_task_info(
        fake_task,
        consumed_resources,
        fake_offer,
        'fake_role',
    )

    expected_task_info = addict.Dict(
        task_id=addict.Dict(value=tid),
        agent_id=addict.Dict(value='fake_agent_id'),
        name='executor-{id}'.format(id=tid),
        resources=[
            addict.Dict(
                name='cpus',
                type='SCALAR',
                role='fake_role',
                scalar=addict.Dict(value=10.0)
            ),
            addict.Dict(
                name='mem',
                type='SCALAR',
                role='fake_role',
                scalar=addict.Dict(value=1024.0)
            ),
            addict.Dict(
                name='disk',
                type='SCALAR',
                role='fake_role',
                scalar=addict.Dict(value=1000.0)
            ),
            addict.Dict(
                name='gpus',
                type='SCALAR',
                role='fake_role',
                scalar=addict.Dict(value=gpus_count)
            ),
            addict.Dict(
                name='ports',
                type='RANGES',
                role='fake_role',
                ranges=addict.Dict(range=[addict.Dict(begin=31200, end=31200)]),
            ),
        ],
        command=addict.Dict(
            value='echo "fake"',
            uris=[],
            environment=addict.Dict(variables=[])
        ),
        container=container,
    )
    assert task_info == expected_task_info


def test_translator_maps_status_to_event():
    for k in MESOS_STATUS_MAP:
        mesos_status = mock.MagicMock()
        mesos_status.state = k
        assert isinstance(
            mesos_status_to_event(mesos_status, 123, task_config={}),
            Event,
        )
