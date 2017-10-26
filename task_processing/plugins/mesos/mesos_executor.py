import logging
import threading
import uuid

from pymesos import MesosSchedulerDriver
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
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
logging.basicConfig(format=FORMAT)


VOLUME_KEYS = set(['mode', 'container_path', 'host_path'])


def valid_volumes(volumes):
    for vol in volumes:
        if set(vol.keys()) != VOLUME_KEYS:
            return (
                False,
                'Invalid volume format, must only contain following keys: '
                '{}, was: {}'.format(VOLUME_KEYS, vol.keys())
            )
    return (True, None)


class MesosTaskConfig(PRecord):
    def __invariant__(conf):
        return ('image' in conf if conf.containerizer == 'DOCKER' else True,
                'Image required for chosen containerizer')

    uuid = field(type=(str, uuid.UUID), initial=uuid.uuid4)
    name = field(type=str, initial="default")
    # image is optional for the mesos containerizer
    image = field(type=str)
    cmd = field(type=str,
                mandatory=True,
                invariant=lambda cmd: (cmd.strip() != '', 'empty cmd'))
    cpus = field(type=float,
                 initial=0.1,
                 factory=float,
                 invariant=lambda c: (c > 0, 'cpus > 0'))
    mem = field(type=float,
                initial=32.0,
                factory=float,
                invariant=lambda m: (m >= 32, 'mem is >= 32'))
    disk = field(type=float,
                 initial=10.0,
                 factory=float,
                 invariant=lambda d: (d > 0, 'disk > 0'))
    gpus = field(type=int,
                 initial=0,
                 factory=int,
                 invariant=lambda g: (g >= 0, 'gpus >= 0'))
    timeout = field(type=float,
                    factory=float,
                    mandatory=False,
                    invariant=lambda t: (t > 0, 'timeout > 0'))
    # By default, the retrying executor retries 3 times. This task option
    # overrides the executor setting.
    retries = field(type=int,
                    factory=int,
                    mandatory=False,
                    invariant=lambda r: (r >= 0, 'retries >= 0'))
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
    containerizer = field(type=str,
                          initial='DOCKER',
                          invariant=lambda c:
                          (c == 'DOCKER' or c == 'MESOS',
                           'containerizer is docker or mesos'))
    environment = field(type=PMap, initial=m(), factory=pmap)

    @property
    def task_id(self):
        return "{}.{}".format(self.name, self.uuid)


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
