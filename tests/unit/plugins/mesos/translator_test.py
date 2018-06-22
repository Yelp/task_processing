import addict
import mock
import pytest
from pyrsistent import v

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.translator import make_mesos_pod_operation
from task_processing.plugins.mesos.translator import make_mesos_task_operation
from task_processing.plugins.mesos.translator import MESOS_STATUS_MAP
from task_processing.plugins.mesos.translator import mesos_update_to_event


def expected_task_info(task_id, gpus_count, container, port):
    return addict.Dict(
        task_id=addict.Dict(value=task_id),
        agent_id=addict.Dict(value='fake_agent_id'),
        name=f'executor-{task_id}',
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
                ranges=addict.Dict(range=[addict.Dict(begin=port, end=port)]),
            ),
        ],
        command=addict.Dict(
            value='echo "fake"',
            uris=[],
            environment=addict.Dict(variables=[])
        ),
        container=container,
    )


def expected_mesos_container_info(port):
    return addict.Dict(
        type='MESOS',
        mesos=addict.Dict(
            image=addict.Dict(
                type='DOCKER',
                docker=addict.Dict(name='fake_image'),
                cached=True,
            ),
        ),
        network_infos=addict.Dict(
            port_mappings=[addict.Dict(host_port=port, container_port=8888)],
            name='fake_cni',
        ),
        volumes=[],
    )


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
def test_make_mesos_task_operation(
    fake_task,
    fake_offer,
    gpus_count,
    containerizer,
    container,
):
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

    operation = make_mesos_task_operation(
        fake_task,
        agent_id=fake_offer.agent_id.value,
        framework_id='fake_framework',
        role='fake_role',
    )

    assert operation == addict.Dict(
        type='LAUNCH',
        launch=addict.Dict(
            task_infos=expected_task_info(fake_task.task_id, gpus_count, container, 31200),
        )
    )


def test_make_mesos_pod_operation(
    fake_pod,
    fake_offer,
):
    operation = make_mesos_pod_operation(
        fake_pod,
        agent_id=fake_offer.agent_id.value,
        framework_id='fake_framework',
        role='fake_role',
    )

    first_port = 31200
    assert operation == addict.Dict(
        type='LAUNCH_GROUP',
        launch_group=addict.Dict(
            executor=addict.Dict(
                type='DEFAULT',
                executor_id=f'executor-{fake_pod.task_id}',
                framework_id='fake_framework',
                resources=[
                    addict.Dict(
                        name='cpus',
                        type='SCALAR',
                        role='fake_role',
                        scalar=addict.Dict(value=1.0),
                    ),
                    addict.Dict(
                        name='mem',
                        type='SCALAR',
                        role='fake_role',
                        scalar=addict.Dict(value=32.0),
                    ),
                    addict.Dict(
                        name='disk',
                        type='SCALAR',
                        role='fake_role',
                        scalar=addict.Dict(value=20.0),
                    ),
                    addict.Dict(
                        name='gpus',
                        type='SCALAR',
                        role='fake_role',
                        scalar=addict.Dict(value=0),
                    ),
                ],
                container=addict.Dict(type='MESOS'),
            ),
            task_group=[
                expected_task_info(
                    t.task_id,
                    gpus_count=0,
                    container=expected_mesos_container_info(first_port + i),
                    port=first_port + i,
                )
                for i, t in enumerate(fake_pod.tasks)
            ],
        ),
    )


@mock.patch('task_processing.plugins.mesos.translator.time')
def test_mesos_update_to_event(mock_time):
    mock_time.time.return_value = 12345678.0
    for key, val in MESOS_STATUS_MAP.items():
        mesos_status = mock.MagicMock()
        mesos_status.state = key
        assert mesos_update_to_event(mesos_status, addict.Dict(task_id='123')) == Event(
            kind='task',
            raw=mesos_status,
            task_id='123',
            task_config={'task_id': '123'},
            timestamp=12345678.0,
            terminal=val.terminal,
            platform_type=val.platform_type,
            success=val.get('success', None),
        )
