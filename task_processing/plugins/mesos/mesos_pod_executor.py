from typing import List
from typing import Tuple

from task_processing.plugins.mesos.config import MesosPodConfig
from task_processing.plugins.mesos.constraints import attributes_match_constraints
from task_processing.plugins.mesos.mesos_executor import ConfigType
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.plugins.mesos.mesos_executor import MesosExecutorCallbacks
from task_processing.plugins.mesos.resource_helpers import allocate_task_resources
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.resource_helpers import task_fits
from task_processing.plugins.mesos.translator import make_mesos_pod_operation
from task_processing.plugins.mesos.translator import mesos_update_to_event


def get_pods_for_offer(
    pod_configs: List[ConfigType],
    offer_resources: ResourceSet,
    offer_attributes: dict,
    role: str,
) -> Tuple[List[ConfigType], List[ConfigType]]:

    pods_to_launch, pods_to_defer = [], []

    for pod_config in pod_configs:
        prepared_tasks = []
        for task_config in pod_config.tasks:
            if (task_fits(task_config, offer_resources) and
                    attributes_match_constraints(offer_attributes, task_config.constraints)):
                prepared_task_config, offer_resources = allocate_task_resources(
                    task_config,
                    offer_resources,
                )
                prepared_tasks.append(prepared_task_config)
            else:
                prepared_tasks = []
                pods_to_defer.append(pod_config)
                break

        if prepared_tasks:
            prepared_pod_config = pod_config.set('tasks', prepared_tasks)
            pods_to_launch.append(prepared_pod_config)

    return pods_to_launch, pods_to_defer


class MesosPodExecutor(MesosExecutor):
    TASK_CONFIG_INTERFACE = MesosPodConfig

    def __init__(self, role, *args, **kwargs) -> None:
        super().__init__(
            role,
            MesosExecutorCallbacks(
                get_pods_for_offer,
                mesos_update_to_event,
                make_mesos_pod_operation,
            ),
            *args,
            **kwargs,
        )
