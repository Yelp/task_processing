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
from task_processing.plugins.smartstack import get_mesos_master
FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


VOLUME_KEYS = set(['mode', 'container_path', 'host_path'])


def valid_volumes(volumes):
    for vol in volumes:
        if vol.keys() != VOLUME_KEYS:
            return (
                False,
                'Invalid volume format, must only contain following keys: '
                '{}, was: {}'.format(VOLUME_KEYS, vol.keys())
            )
    return (True, None)


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
    volumes = field(type=PVector,
                    initial=v(),
                    factory=pvector,
                    invariant=valid_volumes)
    ports = field(type=PVector, initial=v(), factory=pvector)
    cap_add = field(type=PVector, initial=v(), factory=pvector)
    ulimit = field(type=PVector, initial=v(), factory=pvector)
    uris = field(type=PVector, initial=v(), factory=pvector)
    # TODO: containerization + containerization_args ?
    docker_parameters = field(type=PVector, initial=v(), factory=pvector)

    @property
    def task_id(self):
        return "{}.{}".format(self.name, str(self.uuid))


class MesosExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def __init__(
        self,
        role,
        pool=None,
        principal='taskproc',
        secret=None,
        mesos_info=('norcal_devc', 'uswest1-devc'),
        framework_translator=mesos_status_to_event,
        framework_name='taskproc-default',
        framework_staging_timeout=60,
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
            task_staging_timeout_s=framework_staging_timeout
        )

        if isinstance(mesos_info, tuple):
            mesos_address = get_mesos_master(*mesos_info)
        elif isinstance(mesos_info, str):
            mesos_address = mesos_info

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
        print("Killing")

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop()
        self.driver.join()

    def get_event_queue(self):
        return self.execution_framework.event_queue
