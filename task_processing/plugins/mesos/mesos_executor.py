import logging
import threading
import uuid

from pymesos import MesosSchedulerDriver
from pyrsistent import field
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.execution_framework import (
    ExecutionFramework
)
from task_processing.plugins.mesos.translator import mesos_status_to_event
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class MesosTaskConfig(PRecord):
    uuid = field(type=uuid.UUID, initial=uuid.uuid4)
    name = field(type=str, initial="default")
    image = field(type=str, initial="ubuntu:xenial")
    cmd = field(type=str, initial="/bin/true")
    cpus = field(type=float,
                 initial=0.1,
                 invariant=lambda c: (c > 0, 'cpus > 0'))
    mem = field(type=float,
                initial=32.0,
                invariant=lambda m: (m >= 32, 'mem is >= 32'))
    disk = field(type=float,
                 initial=10.0,
                 invariant=lambda d: (d > 0, 'disk > 0'))
    volumes = field(type=PVector, initial=v(), factory=pvector)
    ports = field(type=PVector, initial=v(), factory=pvector)
    cap_add = field(type=PVector, initial=v(), factory=pvector)
    ulimit = field(type=PVector, initial=v(), factory=pvector)
    # TODO: containerization + containerization_args ?
    docker_parameters = field(type=PVector, initial=v(), factory=pvector)

    @property
    def task_id(self):
        return "{}.{}".format(self.name, str(self.uuid))


class MesosExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def __init__(self,
                 authentication_principal='taskproc',
                 credential_secret_file=None,
                 mesos_address='127.0.0.1:5050',
                 translator=mesos_status_to_event):
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        :param dict credentials: Mesos principal and secret.
        """

        self.logger = logging.getLogger(__name__)

        principal = authentication_principal
        secret = None
        if credential_secret_file:
            with open(credential_secret_file) as f:
                secret = f.read().strip()

        self.execution_framework = ExecutionFramework(
            name="test",
            task_staging_timeout_s=60,
            translator=translator,
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
        self.driver_thread = threading.Thread(target=self.driver.run, args=())
        self.driver_thread.start()

    def run(self, task_config):
        self.execution_framework.enqueue_task(task_config)

    def kill(self, task_id):
        print("Killing")

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop()
        self.driver.join()

    def status(self):
        pass

    def get_event_queue(self):
        return self.execution_framework.task_update_queue
