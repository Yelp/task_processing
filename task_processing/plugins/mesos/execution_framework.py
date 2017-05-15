# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging
import threading
import time
from threading import Timer

import mesos.interface
import mesos.native
from mesos.interface import mesos_pb2

from task_processing.plugins.mesos.translator import mesos_status_to_event

try:
    from Queue import Queue
except ImportError:
    from queue import Queue


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class ExecutionFramework(mesos.interface.Scheduler):
    def __init__(
        self,
        name="remote_run",
        task_stagging_timeout_s=240,
        pool=None,
        max_task_queue_size=1000,
        translator=mesos_status_to_event,
        slave_blacklist_timeout_s=900,
        offer_backoff=240,
        task_retries=1,
    ):
        self.name = name
        # wait this long for a task to launch.
        self.task_stagging_timeout_s = task_stagging_timeout_s
        self.framework_info = self.build_framework_info()
        self.pool = pool
        self.translator = translator
        self.slave_blacklist_timeout_s = slave_blacklist_timeout_s
        self.offer_backoff = offer_backoff

        self.task_queue = Queue(max_task_queue_size)
        self.task_update_queue = Queue(max_task_queue_size)
        self.task_launch_counter = 1
        self.driver = None

        self.offer_decline_filter = self.build_decline_offer_filter()
        # TODO: These should be thread safe
        self.blacklisted_slaves = {}
        self.tasks_launched = {}
        self.tasks_inflight = {}
        self.task_to_slave_id_mappings = {}

        self.start_new_thread(self.kill_tasks_stuck_in_stagging)
        self.start_new_thread(self.unblacklist_slaves)

    def start_new_thread(self, func):
        t = threading.Thread(target=func, args=())
        t.start()

    def kill_tasks_stuck_in_stagging(self):
        import time
        while True:
            time_now = time.time()
            for task_id in self.tasks_launched.keys():
                if time_now > self.tasks_launched[task_id] + self.task_stagging_timeout_s:
                    log.warning('Killing stuck task {id}'.format(id=task_id))
                    self.kill_task(task_id)
                    self.tasks_launched.pop(task_id, None)
                    self.blacklist_slave(self.task_to_slave_id_mappings[task_id])
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
        tid = mesos_pb2.TaskID()
        tid.value = task_id
        self.driver.killTask(tid)

    def blacklist_slave(self, slave_id):
        if slave_id in self.blacklisted_slaves:
            self.blacklisted_slaves.pop(slave_id, None)

        log.info('Blacklisting slave: {id} for {secs} seconds.'.format(
                id=slave_id,
                secs=self.slave_blacklist_timeout_s
            )
        )
        self.blacklisted_slaves[slave_id] = time.time()

    def unblacklist_slaves(self):
        import time
        while True:
            time_now = time.time()
            for slave_id in self.blacklisted_slaves.keys():
                if time_now < self.blacklisted_slaves[slave_id] + self.slave_blacklist_timeout_s:
                    log.info('Unblacklisting slave: {id}'.format(id=slave_id))
                    self.blacklisted_slaves.pop(slave_id, None)
            time.sleep(10)

    def enqueue_task(self, task):
        # TODO: Add a wrapper for this task
        self.task_queue.put(task)

    def build_framework_info(self):
        framework = mesos_pb2.FrameworkInfo()
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

        # Iterate over all the tasks of the queue
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
        task = mesos_pb2.TaskInfo()

        container = mesos_pb2.ContainerInfo()
        container.type = 1  # mesos_pb2.ContainerInfo.Type.DOCKER

        command = mesos_pb2.CommandInfo()
        command.value = task_config.cmd

        task.command.MergeFrom(command)
        task.task_id.value = str(self.task_launch_counter)
        task.slave_id.value = offer.slave_id.value
        task.name = 'executor-{id}'.format(id=self.task_launch_counter)
        self.task_launch_counter += 1

        # CPUs
        cpus = task.resources.add()
        cpus.name = "cpus"
        cpus.type = mesos_pb2.Value.SCALAR
        cpus.scalar.value = task_config.cpus

        # mem
        mem = task.resources.add()
        mem.name = "mem"
        mem.type = mesos_pb2.Value.SCALAR
        mem.scalar.value = task_config.mem

        # disk
        disk = task.resources.add()
        disk.name = "disk"
        disk.type = mesos_pb2.Value.SCALAR
        disk.scalar.value = task_config.disk

        # Volumes
        for mode in task_config.volumes:
            for container_path, host_path in task_config.volumes[mode]:
                volume = container.volumes.add()
                volume.container_path = container_path
                volume.host_path = host_path
                """
                volume.mode = 1 # mesos_pb2.Volume.Mode.RW
                volume.mode = 2 # mesos_pb2.Volume.Mode.RO
                """
                volume.mode = 1 if mode == "RW" else 2

        # Container info
        docker = mesos_pb2.ContainerInfo.DockerInfo()
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

        for offer in offers:
            if self.task_queue.empty():
                log.info("Declining offer {id} because there are no more tasks to launch.".format(id=offer.id.value))
                driver.declineOffer(offer.id, self.offer_decline_filter)
                continue

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
                self.tasks_launched[task.task_id.value] = time.time()
                self.task_to_slave_id_mappings[task.task_id.value] = task.slave_id.value

    def statusUpdate(self, driver, update):
        log.info("Task update {update} received for task {task}".format(
                update=mesos_pb2.TaskState.Name(update.state),
                task=update.task_id.value
            )
        )

        if update.state == mesos_pb2.TASK_RUNNING:
            self.tasks_launched.pop(update.task_id.value, None)
            self.tasks_inflight[update.task_id.value] = 1

        if update.state == mesos_pb2.TASK_FINISHED:
            self.tasks_inflight.pop(update.task_id.value, None)
            self.task_update_queue.put(self.translator(update))

        if update.state in (
                mesos_pb2.TASK_LOST,
                mesos_pb2.TASK_KILLED,
                mesos_pb2.TASK_FAILED,
                mesos_pb2.TASK_ERROR
            ):
            if update.task_id.value in self.tasks_launched:
                # Garbage collection thread will take care of this case.
                pass
            elif self.tasks_inflight[update.task_id.value] < self.retries:
                self.tasks_inflight.pop(update.task_id.value, None)
            else:
                self.tasks_inflight[update.task_id.value] += 1
                self.task_update_queue.put(self.translator(update))

        # We have to do this because we are not using implicit acknowledgements.
        driver.acknowledgeStatusUpdate(update)
