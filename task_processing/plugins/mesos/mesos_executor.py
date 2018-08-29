import logging
import threading
from typing import Callable
from typing import List
from typing import NamedTuple
from typing import Tuple

import addict
from pymesos import MesosSchedulerDriver

from task_processing.interfaces.event import Event
from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.execution_framework import ExecutionFramework
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.task_config import MesosTaskConfig


class MesosExecutorCallbacks(NamedTuple):
    get_tasks_for_offer: Callable[
        [List[MesosTaskConfig], ResourceSet, dict, str],
        Tuple[List[addict.Dict], List[MesosTaskConfig]]
    ]
    handle_status_update: Callable[
        [addict.Dict, MesosTaskConfig],
        Event,
    ]
    make_mesos_protobuf: Callable[
        [MesosTaskConfig, str, str],
        addict.Dict,
    ]


class MesosExecutor(TaskExecutor):
    def __init__(
        self,
        role: str,
        callbacks: MesosExecutorCallbacks,
        pool=None,
        principal='taskproc',
        secret=None,
        mesos_address='127.0.0.1:5050',
        initial_decline_delay=1.0,
        framework_name='taskproc-default',
        framework_staging_timeout=240,
        framework_id=None,
        failover=False,
    ) -> None:
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        TODO param docstrings
        """

        self.logger = logging.getLogger(__name__)
        self.role = role
        self.failover = failover

        self.execution_framework = ExecutionFramework(
            role=role,
            pool=pool,
            name=framework_name,
            callbacks=callbacks,
            task_staging_timeout_s=framework_staging_timeout,
            initial_decline_delay=initial_decline_delay,
            framework_id=framework_id,
        )

        # TODO: Get mesos master ips from smartstack
        self.driver = MesosSchedulerDriver(
            sched=self.execution_framework,
            framework=self.execution_framework.framework_info,
            use_addict=True,
            master_uri=mesos_address,
            implicit_acknowledgements=False,
            principal=principal,
            secret=secret,
            failover=failover,
        )

        # start driver thread immediately
        self.driver_thread = threading.Thread(
            target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()

    def run(self, task_config):
        self.execution_framework.enqueue_task(task_config)

    def reconcile(self, task_config):
        self.execution_framework.reconcile_task(task_config)

    def kill(self, task_id):
        return self.execution_framework.kill_task(task_id)

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop(failover=self.failover)
        self.driver.join()

    def get_event_queue(self):
        return self.execution_framework.event_queue
