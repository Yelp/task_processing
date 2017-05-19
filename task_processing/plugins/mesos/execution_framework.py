import logging
import threading
import time

from addict import Dict
from pymesos.interface import Scheduler
from pyrsistent import field
from pyrsistent import PRecord
from six.moves.queue import Queue

from task_processing.plugins.mesos.translator import mesos_status_to_event


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class TaskMetadata(PRecord):
    slave_id = field(type=str, initial=None)
    retries = field(type=int, initial=0)
    task_config = field(type=PRecord, mandatory=True)
    task_state = field(type=str, initial='enqueued')
    time_launched = field(type=time.time, mandatory=True)


class ExecutionFramework(Scheduler):
    def __init__(
        self,
        name,
        task_staging_timeout_s=240,
        pool=None,
        max_task_queue_size=1000,
        translator=mesos_status_to_event,
        slave_blacklist_timeout_s=900,
        offer_backoff=240,
        task_retries=2,
    ):
        self.name = name
        # wait this long for a task to launch.
        self.task_staging_timeout_s = task_staging_timeout_s
        self.framework_info = self.build_framework_info()
        self.pool = pool
        self.translator = translator
        self.slave_blacklist_timeout_s = slave_blacklist_timeout_s
        self.offer_backoff = offer_backoff
        self.task_retries = task_retries

        self.task_queue = Queue(max_task_queue_size)
        self.task_update_queue = Queue(max_task_queue_size)
        self.driver = None
        self.are_offers_suppressed = False

        self.offer_decline_filter = self.build_decline_offer_filter()
        # TODO: These should be thread safe
        self.blacklisted_slaves = {}
        self.task_metadata = {}

        self.stopping = False
        threading.Thread(
            target=self.kill_tasks_stuck_in_staging, args=()).start()
        threading.Thread(
            target=self.unblacklist_slaves, args=()).start()

    def kill_tasks_stuck_in_staging(self):
        while True:
            if self.stopping:
                return

            time_now = time.time()
            for task_id in self.task_metadata.keys():
                if time_now > (
                    self.task_metadata[task_id].time_launched +
                    self.task_staging_timeout_s
                ):
                    log.warning('Killing stuck task {id}'.format(id=task_id))
                    self.kill_task(task_id)
                    self.blacklist_slave(
                        self.task_metadata[task_id].slave_id
                    )
            time.sleep(10)

    def offer_matches_pool(self, offer):
        if self.pool is None:
            # If pool is not specified, then we can accept offer from any agent
            return True
        for attribute in offer.attributes:
            if attribute.name == "pool":
                return attribute.text.value == self.pool
        return False

    def kill_task(self, task_id):
        self.driver.killTask(dict(value=task_id))

    def blacklist_slave(self, slave_id):
        if slave_id in self.blacklisted_slaves:
            # Punish this slave for more time.
            self.blacklisted_slaves.pop(slave_id, None)

        log.info('Blacklisting slave: {id} for {secs} seconds.'.format(
            id=slave_id,
            secs=self.slave_blacklist_timeout_s
        ))
        self.blacklisted_slaves[slave_id] = time.time()

    def unblacklist_slaves(self):
        while True:
            if self.stopping:
                return

            time_now = time.time()
            for slave_id in self.blacklisted_slaves.keys():
                if time_now < (
                    self.blacklisted_slaves[slave_id] +
                    self.slave_blacklist_timeout_s
                ):
                    log.info('Unblacklisting slave: {id}'.format(id=slave_id))
                    self.blacklisted_slaves.pop(slave_id, None)
            time.sleep(10)

    def enqueue_task(self, task_config):
        self.task_metadata[task_config.task_id] = TaskMetadata(
            task_config=task_config
        )
        self.task_queue.put(task_config)
        if self.are_offers_suppressed:
            self.driver.reviveOffers()
            self.are_offers_suppressed = False
            log.info('Reviving offers because we have tasks to run.')

    def build_framework_info(self):
        framework = Dict()
        framework.user = ""  # Have Mesos fill in the current user.
        framework.name = self.name
        framework.checkpoint = True
        return framework

    def build_decline_offer_filter(self):
        return dict(refuse_seconds=self.offer_backoff)

    def get_available_ports(self, resource):
        i = 0
        ports = []
        while True:
            try:
                ports = ports + range(
                    resource.ranges.range[i].begin,
                    resource.ranges.range[i].end
                )
                i += 1
            except Exception:
                break
        return ports

    def get_tasks_to_launch(self, offer):
        tasks_to_launch = []
        remaining_cpus = 0
        remaining_mem = 0
        remaining_disk = 0
        available_ports = []
        for resource in offer.resources:
            if resource.name == "cpus":
                remaining_cpus += resource.scalar.value
            elif resource.name == "mem":
                remaining_mem += resource.scalar.value
            elif resource.name == "disk":
                remaining_disk += resource.scalar.value
            elif resource.name == "ports":
                # TODO: Validate if the ports available > ports required
                available_ports = self.get_available_ports(resource)

        log.info(
            "Received offer {id} with cpus: {cpu}, mem: {mem}, \
            disk: {disk}".format(
                id=offer.id.value,
                cpu=remaining_cpus,
                mem=remaining_mem,
                disk=remaining_disk
            )
        )

        tasks_to_put_back_in_queue = []
        # Get all the tasks of the queue
        while not self.task_queue.empty():
            task = self.task_queue.get()

            if ((remaining_cpus >= task.cpus and
                 remaining_mem >= task.mem and
                 remaining_disk >= task.disk)):
                # This offer is sufficient for us to launch task
                tasks_to_launch.append(
                    self.create_new_docker_task(
                        offer,
                        task,
                        available_ports
                    )
                )

                # Deduct the resources taken by this task from the total
                # available resources.
                remaining_cpus -= task.cpus
                remaining_mem -= task.mem
                remaining_disk -= task.disk
            else:
                # This offer is insufficient for this task. We need to put it
                # back in the queue
                tasks_to_put_back_in_queue.append(task)

        # TODO: This needs to be thread safe. We can write a wrapper over queue
        # with a mutex.
        for task in tasks_to_put_back_in_queue:
            self.task_queue.put(task)

        return tasks_to_launch

    def create_new_docker_task(self, offer, task_config, available_ports):
        task = Dict()

        container = Dict()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER
        container.volumes = list()

        command = Dict()
        command.value = task_config.cmd

        task.command.update(command)
        task.task_id = dict(value=task_config.task_id)
        task.slave_id = dict(value=offer.slave_id.value)
        task.name = 'executor-{id}'.format(id=task_config.task_id)
        task.resources = list()

        md = self.task_metadata[task_config.task_id]
        self.task_metadata[task_config.task_id] = md.set(
            slave_id=task.slave_id.value)

        # CPUs
        cpus = dict(
            name='cpus',
            type='SCALAR',
            scalar=dict(value=task_config.cpus)
        )
        task.resources.append(cpus)

        # mem
        mem = dict(
            name='mem',
            type='SCALAR',
            scalar=dict(value=task_config.mem)
        )
        task.resources.append(mem)

        # disk
        disk = dict(
            name='disk',
            type='SCALAR',
            scalar=dict(value=task_config.disk)
        )
        task.resources.append(disk)

        # Volumes
        for mode in task_config.volumes:
            for container_path, host_path in task_config.volumes[mode]:
                volume = Dict()
                volume.container_path = container_path
                volume.host_path = host_path
                """
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                docker.port_mappings.append(docker_port)
                """
                volume.mode = 1 if mode == "RW" else 2
                container.volumes.append(volume)

        # Container info
        docker = Dict()
        docker.image = task_config.image
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = True
        docker.port_mappings = []

        # Handle the case of multiple port allocations
        port_to_use = available_ports[0]
        available_ports[:] = available_ports[1:]

        mesos_port = Dict(
            name='ports',
            type='RANGES',
            ranges=list(Dict(begin=port_to_use, end=port_to_use))
        )
        task.resources.append(mesos_port)

        docker_port = Dict(
            host_port=port_to_use,
            container_port=8888,
        )
        docker.port_mappings.append(docker_port)

        # Set docker info in container.docker
        container.docker = docker
        # Set docker container in task.container
        task.container = container

        return task

    def stop(self):
        self.stopping = True

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################

    def slaveLost(self, drive, slaveId):
        log.warning("Slave lost: {id}".format(id=str(slaveId)))

    def registered(self, driver, frameworkId, masterInfo):
        log.info("Registered with framework ID {id}".format(
            id=frameworkId.value
        ))

    def reregistered(self, driver, frameworkId, masterInfo):
        log.warning("Registered with framework ID {id}".format(
            id=frameworkId.value
        ))

    def resourceOffers(self, driver, offers):
        if self.driver is None:
            self.driver = driver

        if self.task_queue.empty():
            for offer in offers:
                log.info("Declining offer {id} because there are no more \
                    tasks to launch.".format(
                    id=offer.id.value
                ))
                driver.declineOffer(offer.id, self.offer_decline_filter)

            driver.suppressOffers()
            self.are_offers_suppressed = True
            log.info(
                'Supressing offers because we dont have any more tasks to run.'
            )
            return

        for offer in offers:
            if offer.slave_id.value in self.blacklisted_slaves:
                log.critical("Ignoring offer {offer_id} from blacklisted \
                    slave {slave_name}".format(
                    offer_id=offer.id.value,
                    slave_name=offer.slave_id.value
                ))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            if not self.offer_matches_pool(offer):
                log.info("Declining offer {id} because it is not for pool \
                    {pool}.".format(
                    id=offer.id.value,
                    pool=self.pool
                ))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            tasks_to_launch = self.get_tasks_to_launch(offer)

            if len(tasks_to_launch) == 0:
                log.info("Declining offer {id} because it does not match our \
                    requirements.".format(
                    id=offer.id.value
                ))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            log.info("Launching {number} new docker task(s) using offer {id} \
                on slave {slave}".format(
                number=len(tasks_to_launch),
                id=offer.id.value,
                slave=offer.slave_id.value
            ))
            driver.launchTasks(offer.id, tasks_to_launch)

            for task in tasks_to_launch:
                md = self.task_metadata[task.task_id.value]
                self.task_metadata[task.task_id.value] = md.set(
                    retries=md.retries + 1,
                    time_launched=time.time(),
                    task_state='launched'
                )

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        log.info("Task update {update} received for task {task}".format(
            update=update.state,
            task=task_id
        ))

        self.task_update_queue.put(self.translator(update))

        if update.state == 'TASK_FINISHED':
            self.task_metadata.pop(task_id, None)

        # TODO: move retries out of the framework
        if update.state in (
            'TASK_LOST',
            'TASK_KILLED',
            'TASK_FAILED',
            'TASK_ERROR'
        ):
            md = self.task_metadata[task_id]
            if md.retries >= self.task_retries:
                log.info(
                    'All the retries for task {task} are done.'.format(
                        task=task_id
                    )
                )
                self.task_update_queue.put(self.translator(update))
                self.task_metadata.pop(task_id, None)
            else:
                log.info('Re-enqueuing task {task_id}'.format(task_id=task_id))
                self.task_queue.put(md.task_config)
                self.task_metadata[task_id] = md.set(task_state='re-enqueued')

        # We have to do this because we are not using implicit
        # acknowledgements.
        driver.acknowledgeStatusUpdate(update)
