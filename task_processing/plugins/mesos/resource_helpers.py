from typing import Tuple

import addict
from pyrsistent import field
from pyrsistent import m
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

from task_processing.plugins.mesos.task_config import MesosTaskConfig

NUMERIC_RESOURCE = field(
    type=float,
    initial=0.0,
    factory=float,
    invariant=lambda x: (x >= 0, 'resource < 0'),
)
_NUMERIC_RESOURCES = frozenset(['cpus', 'mem', 'disk', 'gpus'])


class ResourceSet(PRecord):
    cpus = NUMERIC_RESOURCE
    mem = NUMERIC_RESOURCE
    disk = NUMERIC_RESOURCE
    gpus = NUMERIC_RESOURCE
    ports = field(type=PVector, initial=v(), factory=pvector)


def get_offer_resources(offer: addict.Dict, role: str) -> ResourceSet:
    """ Get the resources from a Mesos offer

    :param offer: the payload from a Mesos resourceOffer call
    :param role: the Mesos role we want to get resources for
    :returns: a mapping from resource name -> available resources for the offer
    """
    res = ResourceSet()
    for resource in offer.resources:
        if resource.role != role:
            continue

        if resource.name in _NUMERIC_RESOURCES:
            res = res.set(resource.name, resource.scalar.value)
        elif resource.name == 'ports':
            res = res.set('ports', [pmap(r) for r in resource.ranges.range])
    return res


def allocate_task_resources(
    task_config: MesosTaskConfig,
    offer_resources: ResourceSet,
) -> Tuple[MesosTaskConfig, ResourceSet]:
    """ Allocate a task's resources to a Mesos offer

    :param task: the specification for the task to allocate
    :param offer_resources: a mapping of resource name -> available resources
        (should come from :func:`get_offer_resources`)
    :returns: a pair of (`prepared_task_config`, `remaining_resources`), where
        `prepared_task_config` is the task_config object modified with the
        actual resources consumed
    """
    for res, val in offer_resources.items():
        if res not in _NUMERIC_RESOURCES:
            continue
        offer_resources = offer_resources.set(res, val - task_config[res])

    port = offer_resources.ports[0].begin
    if offer_resources.ports[0].begin == offer_resources.ports[0].end:
        avail_ports = offer_resources.ports[1:]
    else:
        new_port_range = offer_resources.ports[0].set('begin', port + 1)
        avail_ports = offer_resources.ports.set(0, new_port_range)
    offer_resources = offer_resources.set('ports', avail_ports)
    task_config = task_config.set('ports', v(m(begin=port, end=port)))
    return task_config, offer_resources


def task_fits(task: MesosTaskConfig, offer_resources: ResourceSet) -> bool:
    """ Check to see if a task fits a given offer's resources

    :param task: the task specification to check
    :param offer_resources: a mapping of resource name -> available resources
        (should come from :func:`get_offer_resources`)
    :returns: True if the offer has enough resources for the task, False otherwise
    """
    for rname, value in offer_resources.items():
        if rname in _NUMERIC_RESOURCES and task[rname] > value:
            return False
        elif rname == 'ports' and len(value) == 0:  # TODO validate port ranges
            return False

    return True
