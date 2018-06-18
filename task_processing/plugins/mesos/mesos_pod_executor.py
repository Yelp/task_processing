import logging
from typing import Any
from typing import List
from typing import Tuple

import addict

from task_processing.plugins.mesos.mesos_executor import AbstractMesosExecutor
from task_processing.plugins.mesos.task_config import MesosTaskConfig

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)


class MesosPodExecutor(AbstractMesosExecutor):

    def get_tasks_for_offer(
        self,
        task_configs: List[MesosTaskConfig],
        offer: addict.Dict,
    ) -> Tuple[List[Any], List[MesosTaskConfig]]:
        raise NotImplementedError

    def process_status_update(self):
        raise NotImplementedError
