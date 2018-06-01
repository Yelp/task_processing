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
from task_processing.interfaces.event import task_event
from task_processing.metrics import create_counter
from task_processing.metrics import create_timer
from task_processing.metrics import get_metric
from task_processing.plugins.mesos.translator import mesos_status_to_pod_event
from task_processing.plugins.mesos.translator import mesos_status_to_task_event
from task_processing.plugins.mesos.utils import is_pod


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
TASK_OFFER_TIMEOUT = 'taskproc.mesos.task_offer_timeout'

TASK_ENQUEUED_COUNT = 'taskproc.mesos.task_enqueued_count'
POD_ENQUEUED_COUNT = 'taskproc.mesos.pod_enqueued_count'
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
    is_pod = field(type=bool, initial=False)
    pod_id = field(type=(str, type(None)), initial=None)
    terminated = field(type=bool, initial=False)
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
        slave_blacklist_timeout_s=900,
        offer_backoff=10,
        suppress_delay=10,
        initial_decline_delay=1,
        task_reconciliation_delay=300
    ):
        self.name = name
        self._framework_id = None
        # wait this long for a task to launch.
        self.task_staging_timeout_s = task_staging_timeout_s
        self.pool = pool
        self.role = role
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
        self._pod_to_task_mappings = m()

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
                    if md.is_pod is True:
                        continue

                    if md.task_state == 'TASK_INITED':
                        # give up if the task hasn't launched after
                        # offer_timeout
                        if time_now >= (
                            md.task_state_history[md.task_state]['time_stamp'] +
                            md.task_config.offer_timeout
                        ):
                            log.warning((
                                'Task {id} has been waiting for offers for '
                                'longer than configured timeout '
                                '{offer_timeout}. Giving up and removing the '
                                'task from the task queue.'
                            ).format(
                                id=task_id,
                                offer_timeout=md.task_config.offer_timeout
                            ))
                            # killing the task will also dequeue
                            self.kill_task(task_id)
                            self.task_metadata = self.task_metadata.discard(
                                task_id
                            )
                            self.event_queue.put(
                                task_event(
                                    task_id=task_id,
                                    terminal=True,
                                    timestamp=time.time(),
                                    success=False,
                                    message='stop'
                                ))
                            get_metric(TASK_OFFER_TIMEOUT).count(1)

                    # Task is not eligible for killing or reenqueuing
                    if time_now < (md.task_state_history[md.task_state]
                                   ['time_stamp'] + self.task_staging_timeout_s):
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

                    if md.task_state == 'TASK_STAGING':
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
            return True, None

        for attribute in offer.attributes:
            if attribute.name == "pool":
                return attribute.text.value == self.pool, attribute.text.value

        return False, None

    def kill_task(self, task_id):
        tmp_list = []
        flag = False
        with self._lock:
            while not self.task_queue.empty():
                task = self.task_queue.get()
                if task_id == task.task_id:
                    flag = True
                    if task in self._pod_to_task_mappings:
                        for t in self._pod_to_task_mappings[task_id]:
                            self.task_metadata = self.task_metadata.discard(t)

                        self._pod_to_task_mappings = \
                            self._pod_to_task_mappings.discard(task_id)

                    self.task_metadata = self.task_metadata.discard(task_id)
                else:
                    tmp_list.append(task)

            for t in tmp_list:
                self.task_queue.put(t)

        if flag is False:
            if task_id in self._pod_to_task_mappings:
                for t in self._pod_to_task_mappings[task_id]:
                    self.driver.killTask(Dict(value=t))
            else:
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

    def _initialize_metadata(self, task_config):
        # task_state and task_state_history get reset every time
        # a task is enqueued.
        with self._lock:
            self.task_metadata = self.task_metadata.set(
                task_config.task_id,
                TaskMetadata(
                    task_config=task_config,
                    pod_id=None,
                    is_pod=is_pod(task_config),
                    task_state='TASK_INITED',
                    task_state_history=m(
                        TASK_INITED=m(
                            time_stamp=time.time(),
                            status_update=Dict()
                        )
                    ),
                )
            )

            if is_pod(task_config):
                pod_id = task_config.task_id
                nested_container_ids = []

                # We receive individual task updates for each task in
                # task_groups. So, we are gonna keep track of all the
                # individual task in task_metadata.
                for task in task_config.tasks:
                    # task_state and task_state_history get reset every time
                    # a task is enqueued.
                    self.task_metadata = self.task_metadata.set(
                        task.task_id,
                        TaskMetadata(
                            task_config=task,
                            pod_id=pod_id,
                            is_pod=False,
                            task_state='TASK_INITED',
                            task_state_history=m(
                                TASK_INITED=m(
                                    time_stamp=time.time(),
                                    status_update=Dict()
                                )
                            ),
                        )
                    )
                    nested_container_ids.append(task.task_id)

                self._pod_to_task_mappings = self._pod_to_task_mappings.set(
                    pod_id,
                    nested_container_ids
                )
                get_metric(POD_ENQUEUED_COUNT).count(1)

            else:
                get_metric(TASK_ENQUEUED_COUNT).count(1)

    def enqueue_task(self, task_config):
        log.warning('Calling enqueue task again for {}'.format(
            task_config.task_id))
        self._initialize_metadata(task_config)
        with self._lock:
            # Need to lock on task_queue to prevent enqueues when getting
            # tasks to launch
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
        task_group_operations = []
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
                available_ports = self.get_available_ports(resource)

        log.info(
            "Received offer {id} with cpus: {cpu}, mem: {mem}, "
            "disk: {disk} gpus: {gpu} role: {role} ports: {ports}".format(
                id=offer.id.value,
                cpu=remaining_cpus,
                mem=remaining_mem,
                disk=remaining_disk,
                gpu=remaining_gpus,
                role=self.role,
                ports=len(available_ports)
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
                cpus = 0
                mem = 0
                ports = 0
                gpus = 0
                disk = 0

                if is_pod(task):
                    # Aggregate all the resources for pods
                    for t in task.tasks:
                        cpus += t.cpus
                        mem += t.mem
                        disk += t.disk
                        ports += len(t.ports)
                        gpus += t.gpus
                else:
                    cpus = task.cpus
                    mem = task.mem
                    disk = task.disk
                    ports = task.ports
                    gpus = task.gpus
                    ports = len(task.ports)

                if ((remaining_cpus >= cpus and
                     remaining_mem >= mem and
                     remaining_disk >= disk and
                     remaining_gpus >= gpus and
                     len(available_ports) >= ports)):

                    if is_pod(task):
                        task_group_operations.append(
                            self._create_new_mesos_pod(
                                offer,
                                task,
                                available_ports
                            )
                        )

                    else:
                        tasks_to_launch.append(
                            self._create_new_task_info(
                                offer,
                                task,
                                available_ports
                            )
                        )

                    # Deduct the resources taken by this task from the total
                    # available resources.
                    remaining_cpus -= cpus
                    remaining_mem -= mem
                    remaining_disk -= disk
                    remaining_gpus -= gpus

                    if is_pod(task):
                        for t in task.tasks:
                            md = self.task_metadata[t.task_id]
                            get_metric(TASK_QUEUED_TIME_TIMER).record(
                                time.time() - md.task_state_history
                                ['TASK_INITED']['time_stamp']
                            )
                    else:
                        md = self.task_metadata[task.task_id]
                        get_metric(TASK_QUEUED_TIME_TIMER).record(
                            time.time() - md.task_state_history
                            ['TASK_INITED']['time_stamp']
                        )
                else:
                    # This offer is insufficient for this task. We need to put
                    # it back in the queue
                    tasks_to_put_back_in_queue.append(task)

        for task in tasks_to_put_back_in_queue:
            self.task_queue.put(task)
            get_metric(TASK_INSUFFICIENT_OFFER_COUNT).count(1)

        task_operation = None
        if len(tasks_to_launch) > 0:
            task_operation = Dict(
                type='LAUNCH',
                launch=Dict(
                    task_infos=tasks_to_launch
                )
            )
        return task_operation, task_group_operations

    def _create_new_mesos_pod(self, offer, pod_config, available_ports):
        # First create task_infos for the nested containers
        task_infos = []
        for task_config in pod_config.tasks:
            task_infos.append(
                self._create_new_task_info(
                    offer,
                    task_config,
                    available_ports
                )
            )

        # The executor does not need a separate network namespace.
        task_info_for_parent = Dict(
            type='MESOS'
        )

        executor_info = Dict(
            type='DEFAULT',
            container=task_info_for_parent,
            executor_id=Dict(
                value='executor-{id}'.format(id=pod_config.task_id),
            ),
            framework_id=Dict(
                value=self._framework_id
            ),
            resources=[
                # There is no need to allocate a separate port to the executor
                Dict(name='cpus',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=0.1)),
                Dict(name='mem',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=100)),
                Dict(name='disk',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=200)),
                Dict(name='gpus',
                     type='SCALAR',
                     role=self.role,
                     scalar=Dict(value=0)),
            ],
        )
        task_group_info = Dict(
            tasks=task_infos
        )
        launch_group = Dict(
            executor=executor_info,
            task_group=task_group_info
        )
        operation = Dict(
            type='LAUNCH_GROUP',
            launch_group=launch_group,
            executor_ids=[],
        )
        return operation

    def _create_new_task_info(self, offer, task_config, available_ports):
        port_mappings = []
        counter = 0
        for p in task_config.ports:
            port_mappings.append(
                Dict(
                    host_port=available_ports[counter],
                    container_port=p
                )
            )
            counter += 1
        available_ports[:] = available_ports[len(task_config.ports):]

        # TODO: this probably belongs in the caller
        with self._lock:
            if task_config.task_id in self.task_metadata:
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
                    port_mappings=port_mappings,
                    parameters=thaw(task_config.docker_parameters),
                    force_pull_image=True,
                ),
            )
        elif task_config.containerizer == 'MESOS':
            container = Dict(
                type='MESOS',
                # for docker, volumes should include parameters
                volumes=thaw(task_config.volumes),
                network='BRIDGE',
            )
            # For this to work, image_providers needs to be set to 'docker'
            # on mesos agents
            if 'image' in task_config:
                container.mesos = Dict(
                    image=Dict(
                        type='DOCKER',
                        docker=Dict(name=task_config.image),
                    ),
                )

            if task_config.cni_network is not None:
                container.network_infos = [Dict(
                    protocol='IPv4',
                    port_mappings=port_mappings,
                    name=task_config.cni_network,
                )]

        task_info = Dict(
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

        if len(port_mappings) > 0:
            task_info.append(
                Dict(name='ports',
                     type='RANGES',
                     role=self.role,
                     ranges=Dict(
                         range=[Dict(
                             begin=port_mappings[0]['host_port'],
                             end=port_mappings[0]['host_port']
                         )]
                     )
                     )
            )

        if task_config.http_health_check_port is not None:
            task_info.health_check = Dict(
                type='COMMAND',
                command=Dict(
                    value='curl 127.0.0.1:{port}'.join(
                        port=task_config.http_health_check_port
                    ),
                )
            )
        return task_info

    def _extract_task_infos(self, task_operation, task_group_operations):
        task_infos = []
        if task_operation is not None:
            task_infos = task_operation.launch.task_infos
        if len(task_group_operations) > 0:
            for task in task_group_operations:
                task_infos = task_infos + task.launch_group.task_group.tasks

        return task_infos

    def _aggregate_status_updates_for_pod(self, pod_id, task_state):
        sm = m()
        for task_id in self._pod_to_task_mappings[pod_id]:
            sm = sm.set(
                task_id,
                self.task_metadata[task_id].task_state_history
                [task_state]['status_update']
            )

        return sm

    def _aggregate_task_configs_for_pod(self, pod_id):
        tm = m()
        for task_id in self._pod_to_task_mappings[pod_id]:
            tm = tm.set(
                task_id,
                self.task_metadata[task_id].task_config
            )

        return tm

    def _update_internal_state(self, task_id, task_state, update):
        is_terminal = task_state in self._terminal_task_counts
        metadata = self.task_metadata[task_id]

        with self._lock:
            self.task_metadata = self.task_metadata.set(
                task_id,
                metadata.set(
                    task_state=task_state,
                    task_state_history=metadata.task_state_history.set(
                        task_state,
                        m(
                            time_stamp=time.time(),
                            status_update=update
                        )
                    ),
                    terminated=is_terminal
                )
            )

        if metadata.pod_id is not None:
            # This container belongs to a pod. Iterate over all the nested
            # tasks of this pod and check the status of those tasks.
            all_tasks_terminated = True
            state_changed = True
            for t in self._pod_to_task_mappings[metadata.pod_id]:
                with self._lock:
                    md = self.task_metadata[t]
                # Check if all the tasks in the pod have received a
                # terminal status update or not.
                if md.terminated is False:
                    all_tasks_terminated = False

                if task_state not in md.task_state_history:
                    state_changed = False

            if all_tasks_terminated is True or state_changed is True:
                # This means that all the nested containers in the pod have
                # received this status update. So, it is okay for us to send
                # an event for the whole pod.
                self.event_queue.put(
                    mesos_status_to_pod_event(
                        task_state,
                        self._aggregate_status_updates_for_pod(
                            md.pod_id,
                            task_state
                        ),
                        md.pod_id
                    ).set(
                        task_config_per_task_id=self._aggregate_task_configs_for_pod(
                            md.pod_id
                        ),
                        task_config=self.task_metadata[metadata.pod_id].task_config
                    )
                )

            if all_tasks_terminated is True:
                # Discard the pod since it has reached a terminal state
                with self._lock:
                    for t in self._pod_to_task_mappings[metadata.pod_id]:
                        self.task_metadata = \
                            self.task_metadata.discard(t)

                    self._pod_to_task_mappings = \
                        self._pod_to_task_mappings.discard(metadata.pod_id)

                get_metric(self._terminal_task_counts[task_state]).count(
                    1
                )
        else:
            self.event_queue.put(
                mesos_status_to_task_event(update, task_id).set(
                    task_config=metadata.task_config)
            )

            if task_state in self._terminal_task_counts:
                self.task_metadata = self.task_metadata.discard(task_id)

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
            TASK_LAUNCH_FAILED_COUNT,            TASK_FAILED_TO_LAUNCH_COUNT,
            POD_ENQUEUED_COUNT,                  TASK_OFFER_TIMEOUT
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
        self._framework_id = frameworkId.value

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

        declined = {
            'blacklisted': [],
            'bad pool': [],
            'bad resources': [],
            'no tasks': []
        }
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

            offer_pool_match, offer_pool = self.offer_matches_pool(offer)
            if not offer_pool_match:
                log.info(
                    "Declining offer {id}, required pool {sp} doesn't match "
                    "offered pool {op}".format(
                        id=offer.id.value,
                        sp=self.pool,
                        op=offer_pool
                    ))
                declined['bad pool'].append(offer.id.value)
                declined_offer_ids.append(offer.id)
                continue

            tasks_to_launch, task_groups_to_launch = self.get_tasks_to_launch(
                offer)

            operations = task_groups_to_launch[:]
            if tasks_to_launch is not None:
                operations.append(tasks_to_launch)

            if len(operations) == 0:
                if self.task_queue.empty():
                    if offer.id.value not in declined['no tasks']:
                        declined['no tasks'].append(offer.id.value)
                else:
                    declined['bad resources'].append(offer.id.value)
                declined_offer_ids.append(offer.id)
                continue

            accepted.append('offer: {} agent: {} tasks: {}'.format(
                offer.id.value, offer.agent_id.value, len(operations)))

            task_launch_failed = False
            try:
                driver.acceptOffers([offer.id], operations)
            except (socket.timeout, Exception):
                task_launch_failed = True
                get_metric(TASK_LAUNCH_FAILED_COUNT).count(1)

            task_infos = self._extract_task_infos(
                tasks_to_launch, task_groups_to_launch)

            if task_launch_failed is True:
                log.warning('Failed to launch following tasks {tasks}.'
                            'Thus, moving them to UNKNOWN state'.format(
                                tasks=', '.join([
                                    task.task_id.value for task in
                                    task_infos
                                ]),
                            )
                            )

            # 'UNKNOWN' state is for internal tracking. It will not be
            # propogated to users.
            current_task_state = 'UNKNOWN' if task_launch_failed else \
                'TASK_STAGING'

            with self._lock:
                for task in task_infos:
                    md = self.task_metadata[task.task_id.value]
                    self.task_metadata = self.task_metadata.set(
                        task.task_id.value,
                        md.set(
                            task_state=current_task_state,
                            task_state_history=md.task_state_history.set(
                                current_task_state,
                                m(
                                    time_stamp=time.time(),
                                    status_update=Dict()
                                )
                            ),

                        )
                    )
                    # Emit the staging event for successful launches
                    if not task_launch_failed:
                        update = Dict(
                            state='TASK_STAGING',
                            offer=offer,
                        )
                        self._update_internal_state(
                            task.task_id.value,
                            'TASK_STAGING',
                            update
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
            self._update_internal_state(task_id, task_state, update)

        # We have to do this because we are not using implicit
        # acknowledgements.
        driver.acknowledgeStatusUpdate(update)
