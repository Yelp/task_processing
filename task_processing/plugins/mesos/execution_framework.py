import logging
import socket
import threading
import time
from collections import defaultdict
from queue import Queue
from typing import Optional  # noqa, flake8 issue
from typing import TYPE_CHECKING

from addict import Dict
from pymesos.interface import Scheduler
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import v

from task_processing.interfaces.event import control_event
from task_processing.interfaces.event import task_event
from task_processing.metrics import create_counter
from task_processing.metrics import create_timer
from task_processing.metrics import get_metric
from task_processing.plugins.mesos import metrics
from task_processing.plugins.mesos.resource_helpers import get_offer_resources


if TYPE_CHECKING:
    from .mesos_executor import MesosExecutorCallbacks  # noqa


log = logging.getLogger(__name__)


class TaskMetadata(PRecord):
    agent_id = field(type=str, initial='')
    task_config = field(type=PRecord, mandatory=True)
    task_state = field(type=str, mandatory=True)
    task_state_history = field(type=PMap, factory=pmap, mandatory=True)


class ExecutionFramework(Scheduler):
    callbacks: 'MesosExecutorCallbacks'

    def __init__(
        self,
        name,
        role,
        callbacks: 'MesosExecutorCallbacks',
        task_staging_timeout_s,
        pool=None,
        slave_blacklist_timeout_s=900,
        offer_backoff=10,
        suppress_delay=10,
        initial_decline_delay=1,
        task_reconciliation_delay=300,
        framework_id=None,
        failover_timeout=604800,  # 1 week
    ) -> None:
        self.name = name
        # wait this long for a task to launch.
        self.task_staging_timeout_s = task_staging_timeout_s
        self.pool = pool
        self.role = role
        self.callbacks = callbacks
        self.slave_blacklist_timeout_s = slave_blacklist_timeout_s
        self.offer_backoff = offer_backoff

        # TODO: why does this need to be root, can it be "mesos plz figure out"
        self.framework_info = Dict(
            user='root',
            name=self.name,
            checkpoint=True,
            role=self.role,
            failover_timeout=failover_timeout,
        )
        if framework_id:
            self.framework_info['id'] = {'value': framework_id}

        self.task_queue: Queue = Queue()
        self.event_queue: Queue = Queue()
        self._driver: Optional[Scheduler] = None
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
        self._last_offer_time: Optional[float] = None
        self._terminal_task_counts = {
            'TASK_FINISHED': metrics.TASK_FINISHED_COUNT,
            'TASK_LOST': metrics.TASK_LOST_COUNT,
            'TASK_KILLED': metrics.TASK_KILLED_COUNT,
            'TASK_FAILED': metrics.TASK_FAILED_COUNT,
            'TASK_ERROR': metrics.TASK_ERROR_COUNT,
            'TASK_OFFER_TIMEOUT': metrics.TASK_OFFER_TIMEOUT,
        }

        self.driver_error = object()

        self.stopping = False
        task_kill_thread = threading.Thread(
            target=self._background_check, args=())
        task_kill_thread.daemon = True
        task_kill_thread.start()

    def call_driver(self, method, *args, **kwargs):
        if not self._driver:
            log.error(f'{method} failed: No driver')
            return self.driver_error

        try:
            return getattr(self._driver, method)(*args, **kwargs)
        except (socket.timeout, Exception) as e:
            log.warning(f'{method} failed: {str(e)}')
            return self.driver_error

    def _background_check_task(self, time_now, tasks_to_reconcile, task_id, md):
        if md.task_state != 'TASK_INITED':
            tasks_to_reconcile.append(task_id)

        if md.task_state == 'TASK_INITED':
            # give up if the task hasn't launched after
            # offer_timeout
            inited_at = md.task_state_history['TASK_INITED']
            offer_timeout = md.task_config.offer_timeout
            expires_at = inited_at + offer_timeout
            if time_now >= expires_at:
                log.warning(
                    f'Task {task_id} has been waiting for offers '
                    'for longer than configured timeout '
                    f'{offer_timeout}. Giving up and removing the '
                    'task from the task queue.'
                )
                # killing the task will also remove them from the queue
                self.kill_task(task_id)
                # we are not expecting mesos to send terminal update
                # for this task, so cleaning it up manually
                self.task_metadata = self.task_metadata.discard(
                    task_id
                )
                self.event_queue.put(
                    task_event(
                        task_id=task_id,
                        terminal=True,
                        timestamp=time_now,
                        success=False,
                        message='stop',
                        task_config=md.task_config,
                        raw='Failed due to offer timeout',
                    )
                )
                get_metric(metrics.TASK_OFFER_TIMEOUT).count(1)

        # Task is not eligible for killing or reenqueuing
        in_current_state_since = md.task_state_history[md.task_state]
        if time_now < in_current_state_since + self.task_staging_timeout_s:
            return

        if md.task_state == 'UNKNOWN':
            log.warning(
                f'Re-enqueuing task {task_id} in unknown state for '
                f'longer than {self.task_staging_timeout_s}'
            )
            # Re-enqueue task
            self.enqueue_task(md.task_config)
            get_metric(
                metrics.TASK_FAILED_TO_LAUNCH_COUNT).count(1)
        elif md.task_state == 'TASK_STAGING':
            log.warning(f'Killing stuck task {task_id}')
            self.kill_task(task_id)
            self.task_metadata = self.task_metadata.set(
                task_id,
                md.set(
                    task_state='TASK_STUCK',
                    task_state_history=md.task_state_history.set(
                        'TASK_STUCK', time_now),
                )
            )
            self.blacklist_slave(
                agent_id=self.task_metadata[task_id].agent_id,
                timeout=self.slave_blacklist_timeout_s,
            )
            get_metric(metrics.TASK_STUCK_COUNT).count(1)
        elif md.task_state == 'TASK_STUCK':
            t = time.time()
            # 10s since last iteration + time we spent in current one
            time_delta = 10 + t - time_now
            # seconds since task was put in TASK_STUCK state
            time_stuck = t - md.task_state_history['TASK_STUCK']
            # seconds since `time_stuck` crossed another hour
            # boundary
            hour_rolled = time_stuck % 3600

            # if `time_stuck` crossed hour boundary since last
            # background check - lets re-send kill request
            if hour_rolled < time_delta:
                hours_stuck = time_stuck // 3600
                log.warning(
                    f'Task {task_id} is stuck, waiting for terminal '
                    f'state for {hours_stuck}h, sending another kill'
                )
                self.kill_task(task_id)

    def _background_check(self):
        while True:
            if self.stopping:
                return

            time_now = time.time()
            tasks_to_reconcile = []
            with self._lock:
                for task_id, md in self.task_metadata.items():
                    self._background_check_task(
                        time_now,
                        tasks_to_reconcile,
                        task_id,
                        md,
                    )

            self._reconcile_tasks(
                [Dict({'task_id': Dict({'value': task_id})}) for
                    task_id in tasks_to_reconcile]
            )
            elapsed = time.time() - time_now
            log.info(f'background check done in {elapsed}s')
            get_metric(metrics.BGCHECK_TIME_TIMER).record(elapsed)
            time.sleep(10)

    def reconcile_task(self, task_config):
        task_id = task_config.task_id
        with self._lock:
            if task_id in self.task_metadata:
                md = self.task_metadata[task_id]
                self.task_metadata = self.task_metadata.set(
                    task_id,
                    md.set(
                        task_state='TASK_RECONCILING',
                        task_state_history=md.task_state_history.set(
                            'TASK_RECONCILING', time.time()),
                    )
                )
            else:
                log.info(f'Adding {task_id} to metadata for reconciliation')
                self.task_metadata = self.task_metadata.set(
                    task_id,
                    TaskMetadata(
                        task_config=task_config,
                        task_state='TASK_RECONCILING',
                        task_state_history=m(TASK_RECONCILING=time.time()),
                    ),
                )
        self._reconcile_tasks([
            Dict({'task_id': Dict({'value': task_id})})
        ])

    def _reconcile_tasks(self, tasks_to_reconcile):
        if time.time() < self._reconcile_tasks_at:
            return

        log.info(f'Reconciling following tasks {tasks_to_reconcile}')

        if len(tasks_to_reconcile) > 0:
            self.call_driver('reconcileTasks', tasks_to_reconcile)

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
                t = self.task_queue.get()
                if task_id == t.task_id:
                    flag = True
                    self.task_metadata = self.task_metadata.discard(task_id)
                else:
                    tmp_list.append(t)

            for t in tmp_list:
                self.task_queue.put(t)

        if flag is False:
            if self.call_driver('killTask', Dict(value=task_id)) is self.driver_error:
                return False

        return True

    def blacklist_slave(self, agent_id, timeout):
        with self._lock:
            # A new entry is appended even if the agent is being blacklisted.
            # This is equivalent to restarting the blacklist timer.
            log.info(f'Blacklisting slave: {agent_id} for {timeout} seconds.')
            self.blacklisted_slaves = self.blacklisted_slaves.append(agent_id)
            get_metric(metrics.BLACKLISTED_AGENTS_COUNT).count(1)

        unblacklist_thread = threading.Thread(
            target=self.unblacklist_slave,
            kwargs={'timeout': timeout, 'agent_id': agent_id},
        )
        unblacklist_thread.daemon = True
        unblacklist_thread.start()

    def unblacklist_slave(self, agent_id, timeout):
        time.sleep(timeout)
        log.info(
            f'Unblacklisting slave: {agent_id}'
        )
        with self._lock:
            self.blacklisted_slaves = self.blacklisted_slaves.remove(agent_id)

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
                if self.call_driver('reviveOffers') is not self.driver_error:
                    self.are_offers_suppressed = False
                    log.info('Reviving offers because we have tasks to run.')

        get_metric(metrics.TASK_ENQUEUED_COUNT).count(1)

    def launch_tasks_for_offer(self, offer, tasks_to_launch) -> bool:
        mesos_protobuf_tasks = [
            self.callbacks.make_mesos_protobuf(
                task_config, offer.agent_id.value, self.role)
            for task_config in tasks_to_launch
            if task_config.task_id in self.task_metadata
        ]
        if not mesos_protobuf_tasks:
            return False

        launched = True
        launch_time = time.time()
        if self.call_driver('launchTasks', offer.id, mesos_protobuf_tasks) is self.driver_error:
            tasks = ', '.join(task.task_id for task in tasks_to_launch)
            log.warning(
                f'Failed to launch: {tasks}, moving them to UNKNOWN state')
            get_metric(metrics.TASK_LAUNCH_FAILED_COUNT).count(1)
            launched = False

        # 'UNKNOWN' state is for internal tracking. It will not be
        # propogated to users.
        current_task_state = 'TASK_STAGING' if launched else 'UNKNOWN'

        for task in tasks_to_launch:
            md = self.task_metadata.get(task.task_id)
            if not md:
                log.warning(
                    f'trying to launch task {task.task_id}, but it is not in task metadata.'
                    f'current keys in task_metadata: {self.task_metadata.keys()}'
                )
                continue
            self.task_metadata = self.task_metadata.set(
                task.task_id,
                md.set(
                    task_state=current_task_state,
                    task_state_history=md.task_state_history.set(
                        current_task_state, launch_time),
                    agent_id=str(offer.agent_id.value),
                )
            )

            get_metric(metrics.TASK_QUEUED_TIME_TIMER).record(
                launch_time - md.task_state_history['TASK_INITED']
            )

            # Emit the staging event for successful launches
            if launched:
                self.event_queue.put(
                    self.callbacks.handle_status_update(
                        Dict(state='TASK_STAGING', offer=offer),
                        md.task_config,
                    )
                )
                get_metric(metrics.TASK_LAUNCHED_COUNT).count(1)

        return launched

    def stop(self):
        self.stopping = True

    # TODO: add mesos cluster dimension when available
    def _initialize_metrics(self):
        default_dimensions = {
            'framework_name': '.'.join(self.name.split()[:2]),
            'framework_role': self.role
        }

        counters = [
            metrics.TASK_LAUNCHED_COUNT,                 metrics.TASK_FINISHED_COUNT,
            metrics.TASK_FAILED_COUNT,                   metrics.TASK_KILLED_COUNT,
            metrics.TASK_LOST_COUNT,                     metrics.TASK_ERROR_COUNT,
            metrics.TASK_ENQUEUED_COUNT,                 metrics.TASK_INSUFFICIENT_OFFER_COUNT,
            metrics.TASK_STUCK_COUNT,                    metrics.BLACKLISTED_AGENTS_COUNT,
            metrics.TASK_LOST_DUE_TO_INVALID_OFFER_COUNT,
            metrics.TASK_LAUNCH_FAILED_COUNT,            metrics.TASK_FAILED_TO_LAUNCH_COUNT,
            metrics.TASK_OFFER_TIMEOUT,
        ]
        for cnt in counters:
            create_counter(cnt, default_dimensions)

        timers = [
            metrics.OFFER_DELAY_TIMER, metrics.TASK_QUEUED_TIME_TIMER,
            metrics.BGCHECK_TIME_TIMER
        ]
        for tmr in timers:
            create_timer(tmr, default_dimensions)

    ####################################################################
    #                   Mesos driver hooks go here                     #
    ####################################################################
    def offerRescinded(self, driver, offerId):
        # TODO(sagarp): Executor should be able to deal with this.
        log.warning(f'Offer {offerId} rescinded')

    def error(self, driver, message):
        event = control_event(raw=message)

        # TODO: have a mapper function similar to translator of task events
        if message == 'Framework has been removed':
            event = event.set(message='stop')
        else:
            event = event.set(message='unknown')

        self.event_queue.put(event)

    def slaveLost(self, drive, slaveId):
        log.warning(f"Slave lost: {str(slaveId)}")

    def registered(self, driver, frameworkId, masterInfo):
        self._driver = driver
        event = control_event(
            raw={
                'master_info': masterInfo,
                'framework_id': frameworkId,
            },
            message='registered',
        )
        self.event_queue.put(event)
        log.info(
            f"Registered with framework ID {frameworkId.value} and role {self.role}"
        )

    def reregistered(self, driver, masterInfo):
        self._driver = driver
        log.warning(f"Re-registered to {masterInfo} with role {self.role}")

    def resourceOffers(self, driver, offers) -> None:
        self._driver = driver

        current_offer_time = time.time()
        if self._last_offer_time is not None:
            get_metric(metrics.OFFER_DELAY_TIMER).record(
                current_offer_time - self._last_offer_time
            )
        self._last_offer_time = current_offer_time

        # Give user some time to enqueue tasks
        if self.task_queue.empty() and current_offer_time < self.decline_after:
            time.sleep(self.decline_after - current_offer_time)

        declined: dict = defaultdict(list)
        declined_offer_ids = []
        accepted = []

        with self._lock:
            if self.task_queue.empty():
                # Always suppress offers when there is nothing to run
                if self.call_driver('suppressOffers') is not self.driver_error:
                    self.are_offers_suppressed = True
                    log.info("Suppressing offers, no more tasks to run.")

                for offer in offers:
                    declined['no tasks'].append(offer.id.value)
                    declined_offer_ids.append(offer.id)

                self.call_driver(
                    'declineOffer', declined_offer_ids, self.offer_decline_filter)
                log.info(
                    f"Offers declined because of no tasks: {','.join(declined['no tasks'])}")
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
                    declined['blacklisted'].append(
                        f'offer {offer.id.value} agent {offer.agent_id.value}'
                    )
                    declined_offer_ids.append(offer.id)
                    continue

            offer_pool_match, offer_pool = self.offer_matches_pool(offer)
            if not offer_pool_match:
                log.info(
                    f"Declining offer {offer.id.value}, required pool "
                    f"{self.pool} doesn't match offered pool {offer_pool}"
                )
                declined['bad pool'].append(offer.id.value)
                declined_offer_ids.append(offer.id)
                continue

            # Need to lock here even though we are only reading the task_queue, since
            # we are predicating on the queue's emptiness. If not locked, other
            # threads can continue enqueueing, and we never terminate the loop.
            task_configs = []
            with self._lock:
                while not self.task_queue.empty():
                    task_configs.append(self.task_queue.get())

                offer_resources = get_offer_resources(offer, self.role)
                offer_attributes = {
                    attribute.name: attribute.text.value
                    for attribute in offer.attributes
                }
                log.info(
                    f'Received offer {offer.id.value} for role {self.role}: {offer_resources}')
                tasks_to_launch, tasks_to_defer = self.callbacks.get_tasks_for_offer(
                    task_configs,
                    offer_resources,
                    offer_attributes,
                    self.role,
                )

                for task in tasks_to_defer:
                    self.task_queue.put(task)
                get_metric(metrics.TASK_INSUFFICIENT_OFFER_COUNT).count(len(tasks_to_defer))

                if len(tasks_to_launch) == 0:
                    declined['bad resources'].append(offer.id.value)
                    declined_offer_ids.append(offer.id)
                    continue

                ignored_tasks = ','.join(
                    task_config.task_id
                    for task_config in tasks_to_launch
                    if task_config.task_id not in self.task_metadata
                )
                if ignored_tasks:
                    log.warning(
                        f'ignoring tasks not in metadata: {ignored_tasks}')

                tasks_to_launch = [
                    task_config
                    for task_config in tasks_to_launch
                    if task_config.task_id in self.task_metadata
                ]

                if len(tasks_to_launch) == 0:
                    declined['nothing to launch'].append(offer.id.value)
                    declined_offer_ids.append(offer.id)
                elif not self.launch_tasks_for_offer(offer, tasks_to_launch):
                    declined['launch failed'].append(offer.id.value)
                    declined_offer_ids.append(offer.id)
                else:
                    accepted.append(
                        f'offer: {offer.id.value} '
                        f'agent: {offer.agent_id.value} '
                        f'tasks: {len(tasks_to_launch)}'
                    )

        if len(declined_offer_ids) > 0:
            self.call_driver(
                'declineOffer', declined_offer_ids, self.offer_decline_filter
            )
        for reason, items in declined.items():
            log.info(f"Offers declined because {reason}: {', '.join(items)}")
        if accepted:
            log.info(f"Offers accepted: {', '.join(accepted)}")

    def statusUpdate(self, driver, update) -> None:
        self._driver = driver

        task_id = update.task_id.value
        task_state = str(update.state)
        log.info(f"Task update {task_state} received for task {task_id}")

        if task_id not in self.task_metadata:
            # We assume that a terminal status update has been
            # received for this task already.
            log.info('Ignoring this status update because a terminal status '
                     'update has been received for this task already.')
            self.call_driver('acknowledgeStatusUpdate', update)
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
                        f're-enqueue this task {task_id}')
            # Re-enqueue task
            self.enqueue_task(md.task_config)
            get_metric(metrics.TASK_LOST_DUE_TO_INVALID_OFFER_COUNT).count(1)
            self.call_driver('acknowledgeStatusUpdate', update)
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
                self.callbacks.handle_status_update(update, md.task_config),
            )

            if task_state in self._terminal_task_counts:
                with self._lock:
                    self.task_metadata = self.task_metadata.discard(task_id)
                get_metric(self._terminal_task_counts[task_state]).count(1)

        # We have to do this because we are not using implicit
        # acknowledgements.
        self.call_driver('acknowledgeStatusUpdate', update)
