import logging
import socket
import threading
import time

from addict import Dict
from pymesos.interface import Scheduler
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import thaw
from pyrsistent import v
from six.moves.queue import Queue

from task_processing.interfaces.event import control_event
from task_processing.metrics import create_counter
from task_processing.metrics import create_timer
from task_processing.metrics import get_metric
from task_processing.plugins.mesos.translator import mesos_status_to_event


TASK_LAUNCHED_COUNT = 'taskproc.mesos.task_launched_count'
TASK_FAILED_TO_LAUNCH_COUNT = 'taskproc.mesos.tasks_failed_to_launch_count'
TASK_LAUNCH_FAILED_COUNT = 'taskproc.mesos.task_launch_failed_count'
TASK_FINISHED_COUNT = 'taskproc.mesos.task_finished_count'
TASK_FAILED_COUNT = 'taskproc.mesos.task_failure_count'
TASK_KILLED_COUNT = 'taskproc.mesos.task_killed_count'
TASK_LOST_COUNT = 'taskproc.mesos.task_lost_count'
TASK_LOST_DUE_TO_INVALID_OFFER_COUNT = \
    'taskproc.mesos.task_lost_due_to_invalid_offer_count'
TASK_ERROR_COUNT = 'taskproc.mesos.task_error_count'

TASK_ENQUEUED_COUNT = 'taskproc.mesos.task_enqueued_count'
TASK_QUEUED_TIME_TIMER = 'taskproc.mesos.task_queued_time'
TASK_INSUFFICIENT_OFFER_COUNT = 'taskproc.mesos.task_insufficient_offer_count'
TASK_STUCK_COUNT = 'taskproc.mesos.task_stuck_count'

OFFER_DELAY_TIMER = 'taskproc.mesos.offer_delay'
BLACKLISTED_AGENTS_COUNT = 'taskproc.mesos.blacklisted_agents_count'


FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(format=FORMAT)
log = logging.getLogger(__name__)


class TaskMetadata(PRecord):
    agent_id = field(type=str, initial='')
    task_config = field(type=PRecord, mandatory=True)
    task_state = field(type=str, mandatory=True)
    task_state_history = field(type=PMap, factory=pmap, mandatory=True)


class ExecutionFramework(Scheduler):
    def __init__(
        self,
        name,
        role,
        task_staging_timeout_s=240,
        pool=None,
        translator=mesos_status_to_event,
        slave_blacklist_timeout_s=900,
        offer_backoff=10,
        suppress_delay=10,
        initial_decline_delay=1,
        task_reconciliation_delay=300,
    ):
        self.name = name
        # wait this long for a task to launch.
        self.task_staging_timeout_s = task_staging_timeout_s
        self.pool = pool
        self.role = role
        self.translator = translator
        self.slave_blacklist_timeout_s = slave_blacklist_timeout_s
        self.offer_backoff = offer_backoff

        # TODO: why does this need to be root, can it be "mesos plz figure out"
        self.framework_info = Dict(
            user='root',
            name=self.name,
            checkpoint=True,
            role=self.role
        )

        self.task_queue = Queue()
        self.event_queue = Queue()
        self.driver = None
        self.are_offers_suppressed = False
        self.suppress_after = int(time.time()) + suppress_delay
        self.decline_after = time.time() + initial_decline_delay
        self._task_reconciliation_delay = task_reconciliation_delay
        self._reconcile_tasks_at = time.time() + \
            self._task_reconciliation_delay

        self.offer_decline_filter = Dict(refuse_seconds=self.offer_backoff)
        self._lock = threading.RLock()
        self.blacklisted_slaves = v()
        self.task_metadata = m()

        self._initialize_metrics()
        self._last_offer_time = None
        self._terminal_task_counts = {
            'TASK_FINISHED': TASK_FINISHED_COUNT,
            'TASK_LOST': TASK_LOST_COUNT,
            'TASK_KILLED': TASK_KILLED_COUNT,
            'TASK_FAILED': TASK_FAILED_COUNT,
            'TASK_ERROR': TASK_ERROR_COUNT,
        }

        self.stopping = False
        task_kill_thread = threading.Thread(
            target=self._background_check, args=())
        task_kill_thread.daemon = True
        task_kill_thread.start()

    def _background_check(self):
        while True:
            if self.stopping:
                return

            time_now = time.time()
            with self._lock:
                for task_id in self.task_metadata.keys():
                    md = self.task_metadata[task_id]

                    if md.task_state not in (
                        'TASK_STAGING',
                        'UNKNOWN'
                    ):
                        continue

                    # Task is not eligible for killing or reenqueuing
                    if time_now < (md.task_state_history[md.task_state] +
                                   self.task_staging_timeout_s):
                        continue

                    if md.task_state == 'UNKNOWN':
                        log.warning('Task {id} has been in unknown state for '
                                    'longer than {timeout}. Re-enqueuing it.'
                                    .format(
                                        id=task_id,
                                        timeout=self.task_staging_timeout_s
                                    ))
                        # Re-enqueue task
                        self.enqueue_task(md.task_config)
                        get_metric(TASK_FAILED_TO_LAUNCH_COUNT).count(1)
                        continue

                    else:
                        log.warning(
                            'Killing stuck task {id}'.format(id=task_id)
                        )
                        self.kill_task(task_id)
                        self.blacklist_slave(
                            agent_id=self.task_metadata[task_id].agent_id,
                            timeout=self.slave_blacklist_timeout_s,
                        )
                        get_metric(TASK_STUCK_COUNT).count(1)

            self._reconcile_tasks(
                [Dict({'task_id': Dict({'value': task_id})}) for
                    task_id in self.task_metadata
                 if self.task_metadata[task_id].task_state is not
                 'TASK_INITED']
            )
            time.sleep(10)

    def _reconcile_tasks(self, tasks_to_reconcile):
        if time.time() < self._reconcile_tasks_at:
            return

        log.info('Reconciling following tasks {tasks}'.format(
            tasks=tasks_to_reconcile
        ))

        if len(tasks_to_reconcile) > 0:
            try:
                self.driver.reconcileTasks(tasks_to_reconcile)
            except (socket.timeout, Exception) as e:
                log.warning(
                    'Failed to reconcile task status: {}'.format(str(e)))

        self._reconcile_tasks_at += self._task_reconciliation_delay

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

    def blacklist_slave(self, agent_id, timeout):
        with self._lock:
            # A new entry is appended even if the agent is being blacklisted.
            # This is equivalent to restarting the blacklist timer.
            log.info('Blacklisting slave: {id} for {secs} seconds.'.format(
                id=agent_id,
                secs=timeout
            ))
            self.blacklisted_slaves = self.blacklisted_slaves.append(agent_id)
            get_metric(BLACKLISTED_AGENTS_COUNT).count(1)
        unblacklist_thread = threading.Thread(
            target=self.unblacklist_slave,
            kwargs={'timeout': timeout, 'agent_id': agent_id},
        )
        unblacklist_thread.daemon = True
        unblacklist_thread.start()

    def unblacklist_slave(self, agent_id, timeout):
        time.sleep(timeout)
        log.info(
            'Unblacklisting slave: {id}'.format(id=agent_id)
        )
        with self._lock:
            self.blacklisted_slaves = \
                self.blacklisted_slaves.remove(agent_id)

    def enqueue_task(self, task_config):
        with self._lock:
            # task_state and task_state_history get reset every time
            # a task is enqueued.
            self.task_metadata = self.task_metadata.set(
                task_config.task_id,
                TaskMetadata(
                    task_config=task_config,
                    task_state='TASK_INITED',
                    task_state_history=m(TASK_INITED=time.time()),
                )
            )
            # Need to lock on task_queue to prevent enqueues when getting
            # tasks to launch
            self.task_queue.put(task_config)

            if self.are_offers_suppressed:
                self.driver.reviveOffers()
                self.are_offers_suppressed = False
                log.info('Reviving offers because we have tasks to run.')

        get_metric(TASK_ENQUEUED_COUNT).count(1)

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
        remaining_gpus = 0
        available_ports = []
        for resource in offer.resources:
            if resource.name == "cpus" and resource.role == self.role:
                remaining_cpus += resource.scalar.value
            elif resource.name == "mem" and resource.role == self.role:
                remaining_mem += resource.scalar.value
            elif resource.name == "disk" and resource.role == self.role:
                remaining_disk += resource.scalar.value
            elif resource.name == "gpus" and resource.role == self.role:
                remaining_gpus += resource.scalar.value
            elif resource.name == "ports" and resource.role == self.role:
                # TODO: Validate if the ports available > ports required
                available_ports = self.get_available_ports(resource)

        log.info(
            "Received offer {id} with cpus: {cpu}, mem: {mem}, "
            "disk: {disk} gpus: {gpu} role: {role}".format(
                id=offer.id.value,
                cpu=remaining_cpus,
                mem=remaining_mem,
                disk=remaining_disk,
                gpu=remaining_gpus,
                role=self.role
            )
        )

        tasks_to_put_back_in_queue = []

        # Need to lock here even though we are working on the task_queue, since
        # we are predicating on the queue's emptiness. If not locked, other
        # threads can continue enqueueing, and we never terminate the loop.
        with self._lock:
            # Get all the tasks of the queue
            while not self.task_queue.empty():
                task = self.task_queue.get()

                if ((remaining_cpus >= task.cpus and
                     remaining_mem >= task.mem and
                     remaining_disk >= task.disk and
                     remaining_gpus >= task.gpus and
                     len(available_ports) > 0)):
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
                    remaining_gpus -= task.gpus

                    md = self.task_metadata[task.task_id]
                    get_metric(TASK_QUEUED_TIME_TIMER).record(
                        time.time() - md.task_state_history['TASK_INITED']
                    )
                else:
                    # This offer is insufficient for this task. We need to put
                    # it back in the queue
                    tasks_to_put_back_in_queue.append(task)

        for task in tasks_to_put_back_in_queue:
            self.task_queue.put(task)
            get_metric(TASK_INSUFFICIENT_OFFER_COUNT).count(1)

        return tasks_to_launch

    def create_new_docker_task(self, offer, task_config, available_ports):
        # Handle the case of multiple port allocations
        port_to_use = available_ports[0]
        available_ports[:] = available_ports[1:]

        # TODO: this probably belongs in the caller
        with self._lock:
            md = self.task_metadata[task_config.task_id]
            self.task_metadata = self.task_metadata.set(
                task_config.task_id,
                md.set(agent_id=str(offer.agent_id.value))
            )

        if task_config.containerizer == 'DOCKER':
            container = Dict(
                type='DOCKER',
                volumes=thaw(task_config.volumes),
                docker=Dict(
                    image=task_config.image,
                    network='BRIDGE',
                    port_mappings=[Dict(host_port=port_to_use,
                                        container_port=8888)],
                    parameters=thaw(task_config.docker_parameters),
                    force_pull_image=True,
                ),
            )
        elif task_config.containerizer == 'MESOS':
            container = Dict(
                type='MESOS',
                # for docker, volumes should include parameters
                volumes=thaw(task_config.volumes),
                network_infos=Dict(
                    port_mappings=[Dict(host_port=port_to_use,
                                        container_port=8888)],
                ),
            )
            # For this to work, image_providers needs to be set to 'docker'
            # on mesos agents
            if 'image' in task_config:
                container.mesos = Dict(
                    image=Dict(
                        type='DOCKER',
                        docker=Dict(name=task_config.image),
                    )
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
                Dict(name='gpus',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=task_config.gpus)),
                Dict(name='ports',
                     type='RANGES',
                     role=self.role,
                     ranges=Dict(
                         range=[Dict(begin=port_to_use, end=port_to_use)]))
            ],
            command=Dict(
                value=task_config.cmd,
                uris=[
                    Dict(value=uri, extract=False)
                    for uri in task_config.uris
                ],
                environment=Dict(variables=[
                    Dict(name=k, value=v) for k, v in
                    task_config.environment.items()
                ])
            ),
            container=container
        )

    def stop(self):
        self.stopping = True

    # TODO: add mesos cluster dimension when available
    def _initialize_metrics(self):
        default_dimensions = {
            'framework_name': '.'.join(self.name.split()[:2]),
            'framework_role': self.role
        }

        counters = [
            TASK_LAUNCHED_COUNT,                 TASK_FINISHED_COUNT,
            TASK_FAILED_COUNT,                   TASK_KILLED_COUNT,
            TASK_LOST_COUNT,                     TASK_ERROR_COUNT,
            TASK_ENQUEUED_COUNT,                 TASK_INSUFFICIENT_OFFER_COUNT,
            TASK_STUCK_COUNT,                    BLACKLISTED_AGENTS_COUNT,
            TASK_LOST_DUE_TO_INVALID_OFFER_COUNT,
            TASK_LAUNCH_FAILED_COUNT,            TASK_FAILED_TO_LAUNCH_COUNT
        ]
        for cnt in counters:
            create_counter(cnt, default_dimensions)

        timers = [OFFER_DELAY_TIMER, TASK_QUEUED_TIME_TIMER]
        for tmr in timers:
            create_timer(tmr, default_dimensions)

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################
    def offerRescinded(self, driver, offerId):
        # TODO(sagarp): Executor should be able to deal with this.
        log.warning('Offer {offer} rescinded'.format(offer=offerId))

    def error(self, driver, message):
        event = control_event(raw=message)

        # TODO: have a mapper function similar to translator of task events
        if message == 'Framework has been removed':
            event = event.set(message='stop')
        else:
            event = event.set(message='unknown')

        self.event_queue.put(event)

    def slaveLost(self, drive, slaveId):
        log.warning("Slave lost: {id}".format(id=str(slaveId)))

    def registered(self, driver, frameworkId, masterInfo):
        if self.driver is None:
            self.driver = driver
        log.info("Registered with framework ID {id} and role {role}".format(
            id=frameworkId.value,
            role=self.role
        ))

    def reregistered(self, driver, masterInfo):
        log.warning("Re-registered to {master} with role {role}".format(
            master=masterInfo,
            role=self.role
        ))

    def resourceOffers(self, driver, offers):
        if self.driver is None:
            self.driver = driver

        current_offer_time = time.time()
        if self._last_offer_time is not None:
            get_metric(OFFER_DELAY_TIMER).record(
                current_offer_time - self._last_offer_time
            )
        self._last_offer_time = current_offer_time

        # Give user some time to enqueue tasks
        if self.task_queue.empty() and current_offer_time < self.decline_after:
            time.sleep(self.decline_after - current_offer_time)

        declined = {'blacklisted': [],
                    'bad pool': [],
                    'bad resources': [],
                    'no tasks': []}
        declined_offer_ids = []
        accepted = []

        with self._lock:
            if self.task_queue.empty():
                if not self.are_offers_suppressed:
                    driver.suppressOffers()
                    self.are_offers_suppressed = True
                    log.info("Suppressing offers, no more tasks to run.")

                for offer in offers:
                    declined['no tasks'].append(offer.id.value)
                    declined_offer_ids.append(offer.id)

                driver.declineOffer(
                    declined_offer_ids,
                    self.offer_decline_filter
                )
                log.info("Offers declined because of no tasks: {}".format(
                    ','.join(declined['no tasks'])
                ))
                return

        with_maintenance_window = [
            offer for offer in offers if offer.unavailability
        ]

        for offer in with_maintenance_window:
            start_time = offer.unavailability.start['nanoseconds']
            completion_time = int(
                (start_time + offer.unavailability.duration['nanoseconds'])
                / 1000000000
            )
            now = int(time.time())
            duration = completion_time - now
            if duration > 0:
                self.blacklist_slave(
                    agent_id=offer.agent_id.value,
                    timeout=duration,
                )

        without_maintenance_window = [
            offer for offer in offers if offer not in with_maintenance_window
        ]
        for offer in without_maintenance_window:
            with self._lock:
                if offer.agent_id.value in self.blacklisted_slaves:
                    declined['blacklisted'].append('offer {} agent {}'.format(
                        offer.id.value, offer.agent_id.value
                    ))
                    declined_offer_ids.append(offer.id)
                    continue

            if not self.offer_matches_pool(offer):
                log.info("Declining offer {id} because it is not for pool "
                         "{pool}.".format(
                             id=offer.id.value,
                             pool=self.pool
                         ))
                declined['bad pool'].append(offer.id.value)
                declined_offer_ids.append(offer.id)
                continue

            tasks_to_launch = self.get_tasks_to_launch(offer)

            if len(tasks_to_launch) == 0:
                if self.task_queue.empty():
                    if offer.id.value not in declined['no tasks']:
                        declined['no tasks'].append(offer.id.value)
                else:
                    declined['bad resources'].append(offer.id.value)
                declined_offer_ids.append(offer.id)
                continue

            accepted.append('offer: {} agent: {} tasks: {}'.format(
                offer.id.value, offer.agent_id.value, len(tasks_to_launch)))

            task_launch_failed = False
            try:
                driver.launchTasks(offer.id, tasks_to_launch)
            except (socket.timeout, Exception):
                log.warning('Failed to launch following tasks {tasks}.'
                            'Thus, moving them to UNKNOWN state'.format(
                                tasks=', '.join([
                                    task.task_id.value for task in
                                    tasks_to_launch
                                ]),
                            )
                            )
                task_launch_failed = True
                get_metric(TASK_LAUNCH_FAILED_COUNT).count(1)

            # 'UNKNOWN' state is for internal tracking. It will not be
            # propogated to users.
            current_task_state = 'UNKNOWN' if task_launch_failed else \
                'TASK_STAGING'
            with self._lock:
                for task in tasks_to_launch:
                    md = self.task_metadata[task.task_id.value]
                    self.task_metadata = self.task_metadata.set(
                        task.task_id.value,
                        md.set(
                            task_state=current_task_state,
                            task_state_history=md.task_state_history.set(
                                current_task_state, time.time()),

                        )
                    )
                    # Emit the staging event for successful launches
                    if not task_launch_failed:
                        update = Dict(
                            state='TASK_STAGING',
                            offer=offer,
                        )
                        self.event_queue.put(
                            self.translator(update, task.task_id.value).set(
                                task_config=md.task_config
                            )
                        )
                        get_metric(TASK_LAUNCHED_COUNT).count(1)

        if len(declined_offer_ids) > 0:
            driver.declineOffer(declined_offer_ids, self.offer_decline_filter)
        for reason, items in declined.items():
            if items:
                log.info("Offers declined because of {}: {}".format(
                    reason, ', '.join(items)))
        if accepted:
            log.info("Offers accepted: {}".format(', '.join(accepted)))

    def statusUpdate(self, driver, update):
        task_id = update.task_id.value
        task_state = str(update.state)

        log.info("Task update {update} received for task {task}".format(
            update=task_state,
            task=task_id
        ))

        if task_id not in self.task_metadata:
            # We assume that a terminal status update has been
            # received for this task already.
            log.info('Ignoring this status update because a terminal status '
                     'update has been received for this task already.')
            driver.acknowledgeStatusUpdate(update)
            return

        md = self.task_metadata[task_id]

        # If we attempt to accept an offer that has been invalidated by
        # master for some reason such as offer has been rescinded or we
        # have exceeded offer_timeout, then we will get TASK_LOST status
        # update back from mesos master.
        if task_state == 'TASK_LOST' and str(update.reason) == \
                'REASON_INVALID_OFFERS':
            # This task has not been launched. Therefore, we are going to
            # reenqueue it. We are not propogating any event up to the
            # application.
            log.warning('Received TASK_LOST from mesos master because we '
                        'attempted to accept an invalid offer. Going to '
                        're-enqueue this task {id}'.format(id=task_id))
            # Re-enqueue task
            self.enqueue_task(md.task_config)
            get_metric(TASK_LOST_DUE_TO_INVALID_OFFER_COUNT).count(1)
            driver.acknowledgeStatusUpdate(update)
            return

        # Record state changes, send a new event and emit metrics only if the
        # task state has actually changed.
        if md.task_state != task_state:
            with self._lock:
                self.task_metadata = self.task_metadata.set(
                    task_id,
                    md.set(
                        task_state=task_state,
                        task_state_history=md.task_state_history.set(
                            task_state, time.time()),
                    )
                )

            self.event_queue.put(
                self.translator(update, task_id).set(
                    task_config=md.task_config)
            )

            if task_state in self._terminal_task_counts:
                with self._lock:
                    self.task_metadata = self.task_metadata.discard(task_id)
                get_metric(self._terminal_task_counts[task_state]).count(1)

        # We have to do this because we are not using implicit
        # acknowledgements.
        driver.acknowledgeStatusUpdate(update)
