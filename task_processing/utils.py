from enum import Enum
from typing import Any
from typing import List

import yaml


class AutoEnum(Enum):
    """
    Enums in Python require that some methods be defined pre-construction if overrides are
    desired, so if we want to create an enum that sets the value of all members to the name
    of said members, we need to create an Enum class that overrides _generate_next_value_ such
    that when EnumBase does its magic, it sees this method.

    Based on the snippet in https://docs.python.org/3/library/enum.html#using-automatic-values
    """
    @staticmethod
    def _generate_next_value_(
        name: str,
        start: int,
        count: int,
        last_values: List[Any],
    ) -> str:
        """Override auto() to set all enum values to the name of the member"""
        return name


def get_cluster_master_by_proxy(
    proxy_prefix,
    cluster,
    services_file='/nail/etc/services/services.yaml',
):
    with open(services_file, 'r') as istream:
        services = yaml.load(istream)

    proxy_name = 'task_processing.' + proxy_prefix + '_' + cluster
    if proxy_name not in services:
        raise KeyError('Cluster master {} was not found'.format(proxy_name))
    else:
        addr_info = services[proxy_name]
        return addr_info['host'] + ':' + str(addr_info['port'])
