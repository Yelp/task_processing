import logging
from typing import List
from typing import Tuple

import addict

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.constraints import offer_matches_task_constraints
from task_processing.plugins.mesos.mesos_executor import AbstractMesosExecutor
from task_processing.plugins.mesos.resource_helpers import allocate_task_resources
from task_processing.plugins.mesos.resource_helpers import get_offer_resources
from task_processing.plugins.mesos.resource_helpers import task_fits
from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.translator import make_mesos_task_info
from task_processing.plugins.mesos.translator import mesos_update_to_event

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)


class MesosTaskExecutor(AbstractMesosExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def get_tasks_for_offer(
        self,
        task_configs: List[MesosTaskConfig],
        offer: addict.Dict,
    ) -> Tuple[List[addict.Dict], List[MesosTaskConfig]]:
        offer_resources = get_offer_resources(offer, self.role)
        log.info(f'Received offer {offer.id.value} for role {self.role}: {offer_resources}')
        tasks_to_launch, tasks_to_defer = [], []

        for task_config in task_configs:
            if (task_fits(task_config, offer_resources) and
                    offer_matches_task_constraints(offer, task_config)):
                consumed_resources, offer_resources = allocate_task_resources(
                    task_config,
                    offer_resources,
                )
                mesos_task_info = make_mesos_task_info(
                    task_config,
                    consumed_resources,
                    offer,
                    self.role,
                )
                tasks_to_launch.append(mesos_task_info)
            else:
                tasks_to_defer.append(task_config)

        return tasks_to_launch, tasks_to_defer

    def process_status_update(self, update: addict.Dict, task_config: MesosTaskConfig) -> Event:
        return mesos_update_to_event(update, task_config)
