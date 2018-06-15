import time
from typing import List

import addict
from pyrsistent import thaw

from task_processing.interfaces.event import task_event
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.task_config import MesosTaskConfig

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto


def make_mesos_container_info(task_config: MesosTaskConfig, port: int) -> addict.Dict:
    container_info = addict.Dict(
        type=task_config.containerizer,
        volumes=thaw(task_config.volumes),
    )
    port_mappings = [addict.Dict(host_port=port, container_port=8888)]
    if container_info.type == 'DOCKER':
        container_info.docker = addict.Dict(
            image=task_config.image,
            network='BRIDGE',
            port_mappings=port_mappings,
            parameters=thaw(task_config.docker_parameters),
            force_pull_image=(not task_config.use_cached_image),
        )
    elif container_info.type == 'MESOS':
        container_info.network_infos = addict.Dict(port_mappings=port_mappings)
        # For this to work, image_providers needs to be set to 'docker' on mesos agents (as opposed
        # to 'appc' or 'oci'; we're still running docker images, we're just using the UCR to do it).
        if 'image' in task_config:
            container_info.mesos.image = addict.Dict(
                type='DOCKER',  # not 'APPC' or 'OCI'
                docker=addict.Dict(name=task_config.image),
                cached=task_config.use_cached_image,
            )
    return container_info


def make_mesos_resources(
    consumed_resources: ResourceSet,
    port: int,
    role: str,
) -> List[addict.Dict]:
    return [
        addict.Dict(
            name='cpus',
            type='SCALAR',
            role=role,
            scalar=addict.Dict(value=consumed_resources.cpus),
        ),
        addict.Dict(
            name='mem',
            type='SCALAR',
            role=role,
            scalar=addict.Dict(value=consumed_resources.mem)
        ),
        addict.Dict(
            name='disk',
            type='SCALAR',
            role=role,
            scalar=addict.Dict(value=consumed_resources.disk)
        ),
        addict.Dict(
            name='gpus',
            type='SCALAR',
            role=role,
            scalar=addict.Dict(value=consumed_resources.gpus)
        ),
        addict.Dict(
            name='ports',
            type='RANGES',
            role=role,
            ranges=addict.Dict(range=[addict.Dict(begin=port, end=port)])
        ),
    ]


def make_mesos_command_info(task_config: MesosTaskConfig) -> addict.Dict:
    return addict.Dict(
        value=task_config.cmd,
        uris=[addict.Dict(value=uri, extract=False) for uri in task_config.uris],
        environment=addict.Dict(
            variables=[addict.Dict(name=k, value=v) for k, v in task_config.environment.items()],
        )
    )


def make_mesos_task_info(
    task_config: MesosTaskConfig,
    consumed_resources: ResourceSet,
    offer: addict.Dict,
    role: str,
) -> addict.Dict:

    port = consumed_resources['ports'][0]
    container_info = make_mesos_container_info(task_config, port)
    resources = make_mesos_resources(consumed_resources, port, role)
    command_info = make_mesos_command_info(task_config)

    return addict.Dict(
        task_id=addict.Dict(value=task_config.task_id),
        agent_id=addict.Dict(value=offer.agent_id.value),
        name=f'executor-{task_config.task_id}',
        resources=resources,
        command=command_info,
        container=container_info
    )


MESOS_STATUS_MAP = {
    'TASK_STARTING':
    dict(platform_type='starting', terminal=False),
    'TASK_RUNNING':
    dict(platform_type='running', terminal=False),
    'TASK_FINISHED':
    dict(platform_type='finished', terminal=True, success=True),
    'TASK_FAILED':
    dict(platform_type='failed', terminal=True, success=False),
    'TASK_KILLED':
    dict(platform_type='killed', terminal=True, success=False),
    'TASK_LOST':
    dict(platform_type='lost', terminal=True, success=False),
    'TASK_STAGING':
    dict(platform_type='staging', terminal=False),
    'TASK_ERROR':
    dict(platform_type='error', terminal=True, success=False),
    'TASK_KILLING':
    dict(platform_type='killing', terminal=False),
    'TASK_DROPPED':
    dict(platform_type='dropped', terminal=True, success=False),
    'TASK_UNREACHABLE':
    dict(platform_type='unreachable', terminal=False),
    'TASK_GONE':
    dict(platform_type='gone', terminal=True, success=False),
    'TASK_GONE_BY_OPERATOR':
    dict(platform_type='gone_by_operator', terminal=True, success=False),
    'TASK_UNKNOWN':
    dict(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status, task_id, **kwargs):
    kwargs2 = dict(
        raw=mesos_status,
        task_id=str(task_id),
        timestamp=time.time(),
    )
    kwargs2.update(MESOS_STATUS_MAP[mesos_status.state])
    kwargs2.update(kwargs)
    return task_event(**kwargs2)
