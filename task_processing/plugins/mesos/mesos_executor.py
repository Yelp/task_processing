import logging
import threading

from pymesos import MesosSchedulerDriver

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.execution_framework import (
    ExecutionFramework
)
from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.translator import mesos_status_to_event

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(format=FORMAT)


class MesosExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def __init__(
        self,
        role,
        pool=None,
        principal='taskproc',
        secret=None,
        mesos_address='127.0.0.1:5050',
        initial_decline_delay=1.0,
        framework_translator=mesos_status_to_event,
        framework_name='taskproc-default',
        framework_staging_timeout=240,
    ):
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        :param dict credentials: Mesos principal and secret.
        """

        self.logger = logging.getLogger(__name__)

        self.execution_framework = ExecutionFramework(
            role=role,
            pool=pool,
            name=framework_name,
            translator=framework_translator,
            task_staging_timeout_s=framework_staging_timeout,
            initial_decline_delay=initial_decline_delay
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
        )

        # start driver thread immediately
        self.driver_thread = threading.Thread(
            target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()

    def run(self, task_config):
        self.execution_framework.enqueue_task(task_config)

    def kill(self, task_id):
        self.execution_framework.kill_task(task_id)

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop()
        self.driver.join()

    def get_event_queue(self):
        return self.execution_framework.event_queue
