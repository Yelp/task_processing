from typing import List
from typing import Tuple

from task_processing.plugins.mesos.constraints import attributes_match_constraints
from task_processing.plugins.mesos.mesos_executor import MesosExecutor
from task_processing.plugins.mesos.mesos_executor import MesosExecutorCallbacks
from task_processing.plugins.mesos.resource_helpers import allocate_task_resources
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.resource_helpers import task_fits
from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.translator import make_mesos_task_info
from task_processing.plugins.mesos.translator import mesos_update_to_event


def get_tasks_for_offer(
    task_configs: List[MesosTaskConfig],
    offer_resources: ResourceSet,
    offer_attributes: dict,
    role: str,
) -> Tuple[List[MesosTaskConfig], List[MesosTaskConfig]]:

    tasks_to_launch, tasks_to_defer = [], []

    for task_config in task_configs:
        if (task_fits(task_config, offer_resources) and
                attributes_match_constraints(offer_attributes, task_config.constraints)):
            prepared_task_config, offer_resources = allocate_task_resources(
                task_config,
                offer_resources,
            )
            tasks_to_launch.append(prepared_task_config)
        else:
            tasks_to_defer.append(task_config)

    return tasks_to_launch, tasks_to_defer


class MesosTaskExecutor(MesosExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def __init__(self, role, *args, **kwargs) -> None:
        super().__init__(
            role,
            MesosExecutorCallbacks(
                get_tasks_for_offer,
                mesos_update_to_event,
                make_mesos_task_info,
            ),
            *args,
            **kwargs,
        )
