from __future__ import absolute_import
from __future__ import unicode_literals

import uuid
import mock
import logging
import threading
import time
from threading import Timer

from addict import Dict

# import mesos.interface
# import mesos.native
# from mesos.interface import mesos_pb2
from pymesos import MesosSchedulerDriver
from pymesos.interface import Scheduler

# from task_processing.plugins.mesos.translator import mesos_status_to_event

try:
    from Queue import Queue
except ImportError:
    from queue import Queue


log = logging.getLogger(__name__)


class ExecutionFramework(Scheduler):
    def __init__(
        self,
        name="remote_run",
        task_staging_timeout_s=240,
        pool=None,
        max_task_queue_size=1000,
        # translator=mesos_status_to_event,
        slave_blacklist_timeout_s=900,
        offer_backoff=240,
        task_retries=2,
    ):
        self.name = name
        # wait this long for a task to launch.
        self.task_staging_timeout_s = task_staging_timeout_s
        self.framework_info = self.build_framework_info()
        self.pool = pool
        # self.translator = translator
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

        self.start_new_thread(self.kill_tasks_stuck_in_staging)
        self.start_new_thread(self.unblacklist_slaves)

    def start_new_thread(self, func):
        t = threading.Thread(target=func, args=())
        # t.set_daemon = True
        t.start()

    def kill_tasks_stuck_in_staging(self):
        while True:
            time_now = time.time()
            for task_id in self.task_metadata.keys():
                if time_now > self.task_metadata[task_id]['time_launched'] + self.task_staging_timeout_s:
                    log.warning('Killing stuck task {id}'.format(id=task_id))
                    self.kill_task(task_id)
                    self.blacklist_slave(
                        self.task_metadata[task_id]['slave_id'])
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
        self.driver.killTask(dict(value=task_id)))

    def blacklist_slave(self, slave_id):
        if slave_id in self.blacklisted_slaves:
            # Punish this slave for more time.
            self.blacklisted_slaves.pop(slave_id, None)

        log.info('Blacklisting slave: {id} for {secs} seconds.'.format(
                id=slave_id,
                secs=self.slave_blacklist_timeout_s
            )
        )
        self.blacklisted_slaves[slave_id]=time.time()

    def unblacklist_slaves(self):
        while True:
            time_now=time.time()
            for slave_id in self.blacklisted_slaves.keys():
                if time_now < self.blacklisted_slaves[slave_id] + self.slave_blacklist_timeout_s:
                    log.info('Unblacklisting slave: {id}'.format(id=slave_id))
                    self.blacklisted_slaves.pop(slave_id, None)
            time.sleep(10)

    def enqueue_task(self, task):
        # TODO: Add a wrapper for this task
        self.task_metadata[task.task_id]={
            'slave_id': None,
            'retries': 0,
            'task_config': task,
            'task_state': 'enqueued'
        }
        self.task_queue.put(task)
        if self.are_offers_suppressed:
            driver.reviveOffers()
            self.are_offers_suppressed = False
            log.info('Reviving offers because we have tasks to run.')

    def build_framework_info(self):
        framework = Dict()
        framework.user = ""  # Have Mesos fill in the current user.
        framework.name = self.name
        framework.checkpoint = True
        return framework

    def build_decline_offer_filter(self):
        f = mesos_pb2.Filters()
        f.refuse_seconds = self.offer_backoff
        return f

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
            "Received offer {id} with cpus: {cpu}, mem: {mem}, disk: {disk}".format(
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
                tasks_to_launch.append(self.create_new_docker_task(offer, task, available_ports))

                # Deduct the resources taken by this task from the total available resources.
                remaining_cpus -= task.cpus
                remaining_mem -= task.mem
                remaining_disk -= task.disk
            else:
                # This offer is insufficient for this task. We need to put it back in the queue
                tasks_to_put_back_in_queue.append(task)

        # TODO: This needs to be thread safe. We can write a wrapper over queue with a mutux.
        for task in tasks_to_put_back_in_queue:
            self.task_queue.put(task)

        return tasks_to_launch

    def create_new_docker_task(self, offer, task_config, available_ports):
        task = Dict()

        container = Dict()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER

        command = Dict()
        command.value = task_config.cmd

        task.command.MergeFrom(command)
        task.task_id.value = task_config.task_id
        task.slave_id.value = offer.slave_id.value
        task.name = 'executor-{id}'.format(id=task_config.task_id)

        self.task_metadata[task_config.task_id]['slave_id'] = task.slave_id.value

        cpus = dict(
            name='cpus',
            type='SCALAR',
            scalar=dict(value=task_config.cpus)
        )

        # mem
        mem = dict(
            name='mem',
            type='SCALAR',
            scalar=dict(value=task_config.mem)
        )

        # disk
        disk = dict(
            name='disk',
            type='SCALAR',
            scalar=dict(value=task_config.disk)
        )

        # Volumes
        for mode in task_config.volumes:
            for container_path, host_path in task_config.volumes[mode]:
                disk = dict(
                    name='disk',
                    type='SCALAR',
                    scalar=dict(value=task_config.disk)
                )

                volume = container.volumes.add()
                volume.container_path = container_path
                volume.host_path = host_path
                """
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                """
                volume.mode = 1 if mode == "RW" else 2

        # Container info
        docker = Dict()
        docker.image = task_config.image
        docker.network = 2  # mesos_pb2.ContainerInfo.DockerInfo.Network.BRIDGE
        docker.force_pull_image = True

        # Handle the case of multiple port allocations
        port_to_use = available_ports[0]
        available_ports[:] = available_ports[1:]

        mesos_ports = task.resources.add()
        mesos_ports.name = "ports"
        mesos_ports.type = mesos_pb2.Value.RANGES
        port_range = mesos_ports.ranges.range.add()

        port_range.begin = port_to_use
        port_range.end = port_to_use
        docker_port = docker.port_mappings.add()
        docker_port.host_port = port_to_use
        docker_port.container_port = 8888

        # Set docker info in container.docker
        container.docker.MergeFrom(docker)
        # Set docker container in task.container
        task.container.MergeFrom(container)

        return task

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################

    def slaveLost(self, drive, slaveId):
        log.warning("Slave lost: {id}".format(id=str(slaveId)))

    def registered(self, driver, frameworkId, masterInfo):
        log.info("Registered with framework ID {id}".format(id=frameworkId.value))

    def reregistered(self, driver, frameworkId, masterInfo):
        log.warning("Registered with framework ID {id}".format(id=frameworkId.value))

    def resourceOffers(self, driver, offers):
        if self.driver is None:
            self.driver = driver

        if self.task_queue.empty():
            for offer in offers:
                log.info("Declining offer {id} because there are no more tasks to launch.".format(id=offer.id.value))
                driver.declineOffer(offer.id, self.offer_decline_filter)

            driver.suppressOffers()
            self.are_offers_suppressed = True
            log.info('Supressing offers because we dont have any more tasks to run.')
            return

        for offer in offers:
            if offer.slave_id.value in self.blacklisted_slaves:
                log.critical("Ignoring offer {offer_id} from blacklisted slave {slave_name}".format(
                        offer_id=offer.id.value,
                        slave_name=offer.slave_id.value
                    )
                )
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            if not self.offer_matches_pool(offer):
                log.info("Declining offer {id} because it is not for pool {pool}.".format(
                        id=offer.id.value,
                        pool=self.pool
                    )
                )
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            tasks_to_launch = self.get_tasks_to_launch(offer)

            if len(tasks_to_launch) == 0:
                log.info("Declining offer {id} because it does not match our requirements.".format(id=offer.id.value))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

            log.info("Launching {number} new docker task(s) using offer {id} on slave {slave}".format(
                    number=len(tasks_to_launch),
                    id=offer.id.value,
                    slave=offer.slave_id.value
                )
            )
            driver.launchTasks(offer.id, tasks_to_launch)

            for task in tasks_to_launch:
                self.task_metadata[task.task_id.value]['retries'] += 1
                self.task_metadata[task.task_id.value]['time_launched'] = time.time()
                self.task_metadata[task.task_id.value]['task_state'] = 'launched'

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        log.info("Task update {update} received for task {task}".format(
                update=update.state,
                task=task_id
            )
        )

        if update.state == 'TASK_RUNNING':
            self.task_metadata[task_id]['task_state'] = 'in-flight'

        if update.state == 'TASK_FINISHED':
            # self.task_update_queue.put(self.translator(update))
            self.task_update_queue.put(update)
            self.task_metadata.pop(task_id, None)

        if update.state in (
                'TASK_LOST',
                'TASK_KILLED',
                'TASK_FAILED',
                'TASK_ERROR'
            ):
            if self.task_metadata[task_id]['retries'] >= self.task_retries:
                log.info('All the retries for task {task} are done.'.format(task=task_id))
                # self.task_update_queue.put(self.translator(update))
                self.task_update_queue.put(update)
                self.task_metadata.pop(task_id, None)
            else:
                log.info('Re-enqueuing task {task_id}'.format(task_id=task_id))
                self.task_queue.put(self.task_metadata[task_id]['task_config'])
                self.task_metadata[task_id]['task_state'] = 're-enqueued'

        # We have to do this because we are not using implicit acknowledgements.
        driver.acknowledgeStatusUpdate(update)

mesos_scheduler = ExecutionFramework()

def run_mesos_driver():
    # Initialize mesos creds
    credential = Dict()
    credential.principal = "mesos_slave"
    credential.secret = "bee5aeJibee5aeJibee5aeJi"

    global mesos_scheduler
    driver = MesosSchedulerDriver(
        mesos_scheduler,
        mesos_scheduler.framework_info,
        "10.40.1.17:5050",
        False,
        credential
    )
    status = 0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1
    # Ensure that the driver process terminates.
    driver.stop()


if __name__ == "__main__":
    logging.info("Begin")

    d = mock.Mock(
        image="ubuntu:14.04",
        cmd="/bin/sleep 120",
        cpus=1,
        mem=10,
        disk=1000,
        volumes={
            "RO": [("/nail/etc/", "/nail/etc")],
            "RW": [("/tmp", "/nail/tmp")]
        },
        ports=[]
    )


    thread = threading.Thread(target=run_mesos_driver, args=())
    thread.daemon = True
    thread.start()
    global mesos_scheduler

    for i in range(200):
        d = mock.Mock(
            task_id=str(uuid.uuid4()),
            image="ubuntu:14.04",
            cmd="/bin/sleep 120",
            cpus=1,
            mem=10,
            disk=1000,
            volumes={
                "RO": [("/nail/etc/", "/nail/etc")],
                "RW": [("/tmp", "/nail/tmp")]
            },
            ports=[]
        )

        mesos_scheduler.enqueue_task(d)

    while True:
        time.sleep(20)
