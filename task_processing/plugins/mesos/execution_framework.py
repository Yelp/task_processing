import logging
import threading
import time

from addict import Dict
from pymesos.interface import Scheduler
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PRecord
from six.moves.queue import Queue

from task_processing.plugins.mesos.translator import mesos_status_to_event


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class TaskMetadata(PRecord):
    agent_id = field(type=str, initial='')
    retries = field(type=int, initial=0)
    task_config = field(type=PRecord, mandatory=True)
    task_state = field(type=str, initial='enqueued')
    time_launched = field(type=float, mandatory=True)


class ExecutionFramework(Scheduler):
    def __init__(
        self,
        name,
        role,
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
        self.pool = pool
        self.role = role
        self.translator = translator
        self.slave_blacklist_timeout_s = slave_blacklist_timeout_s
        self.offer_backoff = offer_backoff
        self.task_retries = task_retries

        # TODO: why does this need to be root, can it be "mesos plz figure out"
        self.framework_info = Dict(
            user='root',
            name=self.name,
            checkpoint=True,
            role=self.role
        )
        self.task_queue = Queue(max_task_queue_size)
        self.task_update_queue = Queue(max_task_queue_size)
        self.driver = None
        self.are_offers_suppressed = False

        self.offer_decline_filter = Dict(refuse_seconds=self.offer_backoff)
        self._lock = threading.RLock()
        self.blacklisted_slaves = m()
        self.task_metadata = m()

        self.stopping = False
        task_kill_thread = threading.Thread(
            target=self.kill_tasks_stuck_in_staging, args=())
        task_kill_thread.daemon = True
        task_kill_thread.start()
        blacklist_thread = threading.Thread(
            target=self.unblacklist_slaves, args=())
        blacklist_thread.daemon = True
        blacklist_thread.start()

    def kill_tasks_stuck_in_staging(self):
        while True:
            if self.stopping:
                return

            with self._lock:
                time_now = time.time()
                for task_id in self.task_metadata.keys():
                    if time_now > (
                        self.task_metadata[task_id].time_launched +
                        self.task_staging_timeout_s
                    ):
                        log.warning(
                            'Killing stuck task {id}'.format(id=task_id)
                        )
                        self.kill_task(task_id)
                        self.blacklist_slave(
                            self.task_metadata[task_id].agent_id
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
        self.driver.killTask(Dict(value=task_id))

    def blacklist_slave(self, agent_id):
        with self._lock:
            if agent_id in self.blacklisted_slaves:
                # Punish this slave for more time.
                self.blacklisted_slaves = \
                    self.blacklisted_slaves.discard(agent_id)

            log.info('Blacklisting slave: {id} for {secs} seconds.'.format(
                id=agent_id,
                secs=self.slave_blacklist_timeout_s
            ))
            self.blacklisted_slaves = \
                self.blacklisted_slaves.set(agent_id, time.time())

    def unblacklist_slaves(self):
        while True:
            if self.stopping:
                return

            with self._lock:
                time_now = time.time()
                for agent_id in self.blacklisted_slaves.keys():
                    if time_now < (
                        self.blacklisted_slaves[agent_id] +
                        self.slave_blacklist_timeout_s
                    ):
                        log.info(
                            'Unblacklisting slave: {id}'.format(id=agent_id)
                        )
                        self.blacklisted_slaves = \
                            self.blacklisted_slaves.discard(agent_id)
            time.sleep(10)

    def enqueue_task(self, task_config):
        with self._lock:
            self.task_metadata = self.task_metadata.set(
                task_config.task_id,
                TaskMetadata(
                    task_config=task_config,
                    time_launched=time.time(),
                )
            )

        self.task_queue.put(task_config)

        if self.are_offers_suppressed:
            self.driver.reviveOffers()
            self.are_offers_suppressed = False
            log.info('Reviving offers because we have tasks to run.')

    def get_available_ports(self, resource):
        i = 0
        ports = []
        while True:
            try:
                ports = ports + list(range(
                    resource.ranges.range[i].begin,
                    resource.ranges.range[i].end
                ))
                i += 1
            except Exception as e:
                break
        return ports

    def get_tasks_to_launch(self, offer):
        tasks_to_launch = []
        remaining_cpus = 0
        remaining_mem = 0
        remaining_disk = 0
        available_ports = []
        for resource in offer.resources:
            if resource.name == "cpus" and resource.role == self.role:
                remaining_cpus += resource.scalar.value
            elif resource.name == "mem" and resource.role == self.role:
                remaining_mem += resource.scalar.value
            elif resource.name == "disk" and resource.role == self.role:
                remaining_disk += resource.scalar.value
            elif resource.name == "ports" and resource.role == self.role:
                # TODO: Validate if the ports available > ports required
                available_ports = self.get_available_ports(resource)

        log.info(
            'Received offer {id} with cpus: {cpu}, mem: {mem},\
            disk: {disk} role: {role}'.format(
                id=offer.id.value,
                cpu=remaining_cpus,
                mem=remaining_mem,
                disk=remaining_disk,
                role=self.role
            )
        )

        tasks_to_put_back_in_queue = []

        # Need to lock here even though we working on the task_queue, because
        # we are predicating on the queues emptiness
        with self._lock:
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
                    # This offer is insufficient for this task. We need to put
                    # it back in the queue
                    tasks_to_put_back_in_queue.append(task)

        # TODO: This needs to be thread safe. We can write a wrapper over queue
        # with a mutex.
        for task in tasks_to_put_back_in_queue:
            self.task_queue.put(task)

        return tasks_to_launch

    def create_new_docker_task(self, offer, task_config, available_ports):
        # Handle the case of multiple port allocations
        port_to_use = available_ports[0]
        available_ports[:] = available_ports[1:]

        with self._lock:
            md = self.task_metadata[task_config.task_id]
            self.task_metadata = self.task_metadata.set(
                task_config.task_id,
                md.set(agent_id=str(offer.agent_id.value))
            )

        return Dict(
            task_id=Dict(value=task_config.task_id),
            agent_id=Dict(value=offer.agent_id.value),
            name='executor-{id}'.format(id=task_config.task_id),
            resources=[
                Dict(name='cpus',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=task_config.cpus)),
                Dict(name='mem',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=task_config.mem)),
                Dict(name='disk',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=task_config.disk)),
                Dict(name='ports',
                     type='RANGES',
                     role=self.role,
                     ranges=Dict(
                         range=[Dict(begin=port_to_use, end=port_to_use)]))
            ],
            command=Dict(
                value=task_config.cmd,
                uris=[Dict(value=uri, extract=False)
                      for uri in task_config.uris]
            ),
            container=Dict(
                type='DOCKER',
                docker=Dict(image=task_config.image,
                            network='BRIDGE',
                            force_pull_image=True,
                            port_mappings=[Dict(host_port=port_to_use,
                                                container_port=8888)]),
                volumes=[
                    Dict(container_path=container_path,
                         host_path=host_path,
                         mode=1 if mode == "RW" else 2)
                    for mode, paths in task_config.volumes
                    for container_path, host_path in paths]))

    def stop(self):
        self.stopping = True

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################

    def slaveLost(self, drive, slaveId):
        log.warning("Slave lost: {id}".format(id=str(slaveId)))

    def registered(self, driver, frameworkId, masterInfo):
        if self.driver is None:
            self.driver = driver
        log.info("Registered with framework ID {id} and role {role}".format(
            id=frameworkId.value,
            role=self.role
        ))

    def reregistered(self, driver, frameworkId, masterInfo):
        log.warning("Registered with framework ID {id} and role {role}".format(
            id=frameworkId.value,
            role=self.role
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
            with self._lock:
                if offer.agent_id.value in self.blacklisted_slaves:
                    log.critical("Ignoring offer {offer_id} from blacklisted \
                        slave {slave_name}".format(
                        offer_id=offer.id.value,
                        slave_name=offer.agent_id.value
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
                slave=offer.agent_id.value
            ))
            driver.launchTasks(offer.id, tasks_to_launch)

            with self._lock:
                for task in tasks_to_launch:
                    md = self.task_metadata[task.task_id.value]
                    self.task_metadata = self.task_metadata.set(
                        task.task_id.value,
                        md.set(
                            retries=md.retries + 1,
                            time_launched=time.time(),
                            task_state='launched'
                        )
                    )

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        log.info("Task update {update} received for task {task}".format(
            update=update.state,
            task=task_id
        ))

        md = self.task_metadata[task_id]
        self.task_update_queue.put(
            self.translator(update, task_id).set(task_config=md.task_config)
        )

        if update.state == 'TASK_FINISHED':
            with self._lock:
                self.task_metadata = self.task_metadata.discard(task_id)

        # TODO: move retries out of the framework
        if update.state in (
            'TASK_LOST',
            'TASK_KILLED',
            'TASK_FAILED',
            'TASK_ERROR'
        ):
            with self._lock:
                if md.retries >= self.task_retries:
                    log.info(
                        'All the retries for task {task} are done.'.format(
                            task=task_id
                        )
                    )
                    self.task_update_queue.put(
                        self.translator(update, task_id))
                    self.task_metadata = self.task_metadata.discard(task_id)
                else:
                    log.info(
                        'Re-enqueuing task {task_id}'.format(task_id=task_id)
                    )
                    self.task_queue.put(md.task_config)
                    self.task_metadata = self.task_metadata.set(
                        task_id,
                        md.set(
                            task_state='re-enqueued'
                        )
                    )

        # We have to do this because we are not using implicit
        # acknowledgements.
        driver.acknowledgeStatusUpdate(update)
