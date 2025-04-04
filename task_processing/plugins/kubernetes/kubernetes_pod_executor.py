import logging
import queue
import threading
import time
from queue import Queue
from typing import Collection
from typing import Optional

from kubernetes import watch as kube_watch
from kubernetes.client import V1Affinity
from kubernetes.client import V1Container
from kubernetes.client import V1ContainerPort
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSecurityContext
from kubernetes.client import V1PodSpec
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1SecurityContext
from kubernetes.client.exceptions import ApiException
from pyrsistent import pmap
from pyrsistent import v
from pyrsistent.typing import PMap

from task_processing.interfaces import TaskExecutor
from task_processing.interfaces.event import Event
from task_processing.interfaces.event import task_event
from task_processing.plugins.kubernetes.kube_client import KubeClient
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.plugins.kubernetes.task_metadata import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.task_metadata import KubernetesTaskState
from task_processing.plugins.kubernetes.types import PodEvent
from task_processing.plugins.kubernetes.utils import (
    get_capabilities_for_capability_changes,
)
from task_processing.plugins.kubernetes.utils import get_kubernetes_empty_volume_mounts
from task_processing.plugins.kubernetes.utils import get_kubernetes_env_vars
from task_processing.plugins.kubernetes.utils import get_kubernetes_secret_volume_mounts
from task_processing.plugins.kubernetes.utils import (
    get_kubernetes_service_account_token_volume_mounts,
)
from task_processing.plugins.kubernetes.utils import get_kubernetes_volume_mounts
from task_processing.plugins.kubernetes.utils import get_node_affinity
from task_processing.plugins.kubernetes.utils import get_pod_empty_volumes
from task_processing.plugins.kubernetes.utils import get_pod_secret_volumes
from task_processing.plugins.kubernetes.utils import (
    get_pod_service_account_token_volumes,
)
from task_processing.plugins.kubernetes.utils import get_pod_volumes
from task_processing.plugins.kubernetes.utils import get_sanitised_kubernetes_name
from task_processing.plugins.kubernetes.utils import get_topology_spread_constraints


logger = logging.getLogger(__name__)

POD_WATCH_THREAD_JOIN_TIMEOUT_S = 1.0
POD_EVENT_THREAD_JOIN_TIMEOUT_S = 1.0
QUEUE_GET_TIMEOUT_S = 0.5
SUPPORTED_POD_MODIFIED_EVENT_PHASES = {
    "Failed",
    "Running",
    "Succeeded",
    "Unknown",
}
# arbitrarily chosen - we may honestly want to consider failing loudly if we actually get to
# this amount of backoff since something is likely very wrong (this would require ~10 retries)
MAX_WATCH_BACKOFF_S = 30
# arbitrarily chosen - 1.5 seemed like a good compromise between multiple retries and giving the
# control plane some breathing room
RETRY_BACKOFF_EXPONENT = 1.5


class KubernetesPodExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = KubernetesTaskConfig

    def __init__(
        self,
        namespace: str,
        version: Optional[str] = None,
        kubeconfig_path: Optional[str] = None,
        task_configs: Optional[Collection[KubernetesTaskConfig]] = [],
        emit_events_without_state_transitions: bool = False,
        # kubeconfigs used to continue to watch other clusters
        # Used when transitioning to a new cluster in the primary kubeconfig_path to continue watching still-running pods on other clusters
        watcher_kubeconfig_paths: Collection[str] = (),
    ) -> None:
        if not version:
            version = "unknown_task_processing"
        user_agent = f"{namespace}/v{version}"
        self.kube_client = KubeClient(
            kubeconfig_path=kubeconfig_path, user_agent=user_agent
        )

        self.watcher_kube_clients = [
            KubeClient(kubeconfig_path=watcher_kubeconfig_path, user_agent=user_agent)
            for watcher_kubeconfig_path in watcher_kubeconfig_paths
        ]

        self.namespace = namespace

        # Pod modified events that did not result in a pod state transition are usually not
        # forwarded, but some consumers are interested in container state changes. This
        # variable controls whether such additional pod modified events are forwarded.
        self.emit_events_without_state_transitions = (
            emit_events_without_state_transitions
        )

        self.stopping = False
        self.task_metadata: PMap[str, KubernetesTaskMetadata] = pmap()

        self.task_metadata_lock = threading.RLock()
        if task_configs:
            for task_config in task_configs:
                self._initialize_existing_task(task_config)

        # we have two queues since we need to process Pod Events into something that we can place
        # onto the queue that any application (e.g., something like Tron or Jolt) can actually use
        # and we've opted to not do that processing in the Pod event watcher thread so as to keep
        # that logic for the threads that operate on them as simple as possible and to make it
        # possible to cleanly shutdown both of these.
        self.pending_events: "Queue[PodEvent]" = Queue()
        self.event_queue: "Queue[Event]" = Queue()

        # TODO(TASKPROC-243): keep track of resourceVersion so that we can continue event processing
        # from where we left off on restarts
        self.pod_event_watch_threads = []
        self.watches = []
        for kube_client in [self.kube_client] + self.watcher_kube_clients:
            watch = kube_watch.Watch()
            pod_event_watch_thread = threading.Thread(
                target=self._pod_event_watch_loop,
                args=(kube_client, watch),
                # ideally this wouldn't be a daemon thread, but a watch.Watch() only checks
                # if it should stop after receiving an event - and it's possible that we
                # have periods with no events so instead we'll attempt to stop the watch
                # and then join() with a small timeout to make sure that, if we shutdown
                # with the thread alive, we did not drop any events
                daemon=True,
            )
            pod_event_watch_thread.start()
            self.pod_event_watch_threads.append(pod_event_watch_thread)
            self.watches.append(watch)

        self.pending_event_processing_thread = threading.Thread(
            target=self._pending_event_processing_loop,
        )
        self.pending_event_processing_thread.start()

    def _initialize_existing_task(self, task_config: KubernetesTaskConfig) -> None:
        """Generates task_metadata in UNKNOWN state for an existing KubernetesTaskConfig.
        Used during initialization or recovery for a task"""
        pod_name = task_config.pod_name
        logger.debug(f"Initializing task metadata for known pod {pod_name}")
        # We are initiating with UNKNOWN state on initial load, then leave
        # matching tasks to running/completed pods in reconcile()
        with self.task_metadata_lock:
            self.task_metadata = self.task_metadata.set(
                pod_name,
                KubernetesTaskMetadata(
                    task_config=task_config,
                    node_name="UNKNOWN",
                    task_state=KubernetesTaskState.TASK_UNKNOWN,
                    task_state_history=v(
                        (KubernetesTaskState.TASK_UNKNOWN, time.time())
                    ),
                ),
            )

    def _pod_event_watch_loop(
        self, kube_client: KubeClient, watch: kube_watch.Watch
    ) -> None:
        # this will generally only be used for recovering from expired resourceVersions exceptions
        # we've seen the restart get stuck in a loop - even when we restart the watch anew - so
        # let's add a small backoff time to avoid hammering the API server and hopefully avoid this
        # since we're not sure if this restart loop is further hampering recovery
        retry_attempt = 1
        logger.debug(f"Starting watching Pod events for namespace={self.namespace}.")
        # TODO: Do LIST + WATCH if we're not starting from a known/good resourceVersion to
        # guarantee that we always get ordered events (starting from a resourceVersion of 0)
        # has no such guarantees that the initial events will be ordered in any meaningful way
        # see: https://github.com/kubernetes/kubernetes/issues/74022
        # NOTE: we'll also probably want to add some sort of reconciliation if we do ^
        # since it's possible that we'll have skipped some events
        while not self.stopping:
            try:
                for pod_event in watch.stream(
                    kube_client.core.list_namespaced_pod, self.namespace
                ):
                    # it's possible that we've received an event after we've already set the stop
                    # flag since Watch streams block forever, so re-check if we've stopped before
                    # queueing any pending events
                    if not self.stopping:
                        logger.debug("Adding Pod event to pending event queue.")
                        self.pending_events.put(pod_event)

                        # we reset the retry count unconditionally since this is simpler
                        # than checking if we had any exponential backoff
                        retry_attempt = 1
                    else:
                        break
            except ApiException as e:
                if not self.stopping:
                    if not kube_client.maybe_reload_on_exception(exception=e):
                        # we reset the resource version since it's possible that the last one is no
                        # longer able to be resumed from
                        # NOTE: this means that when we restart the stream, we'll get events for
                        # every pod in the namespace - so we *shouldn't* miss any events at the
                        # cost of likely re-processing events we've already handled (which should
                        # be fine)
                        logger.exception(
                            "Unhandled API exception while watching Pod events - restarting watch!"
                        )
                        watch.resource_version = None
                        # this should be safe since this function runs in its own thread so this
                        # sleep shouldn't block the workload using this plugin or the other
                        # task_proc threads - additionally, we don't take any locks in this thread
                        # so we shouldn't have to worry about any sort of deadlocks :)
                        backoff_time = min(
                            retry_attempt * RETRY_BACKOFF_EXPONENT,
                            MAX_WATCH_BACKOFF_S,
                        )
                        logger.info(
                            "Sleeping for %d seconds on attempt %d before retrying...",
                            backoff_time,
                            retry_attempt,
                        )
                        retry_attempt += 1
                        time.sleep(backoff_time)

            except Exception:
                # we want to avoid a potentially misleading log message should we encounter
                # an exception when we want to shutdown this thread since nothing of value
                # will be lost anyway (example scenario: we've called stop() on an idle cluster
                # but the Watch is still blocking and eventually gets disconnected)
                if not self.stopping:
                    logger.exception(
                        "Exception encountered while watching Pod events - restarting watch!"
                    )
        logger.debug("Exiting Pod event watcher - stop requested.")

    def __handle_deleted_pod_event(self, event: PodEvent) -> None:
        pod = event["object"]
        pod_name = pod.metadata.name
        task_metadata = self.task_metadata[pod_name]
        raw_event = event["raw_object"]

        logger.info(f"Removing {pod_name} from state and emitting 'killed' event.")

        self.task_metadata = self.task_metadata.discard(pod_name)
        self.event_queue.put(
            task_event(
                task_id=pod_name,
                terminal=True,
                success=False,
                timestamp=time.time(),
                raw=raw_event,
                task_config=task_metadata.task_config,
                platform_type="killed",
            )
        )

    def __handle_modified_pod_event(self, event: PodEvent) -> None:
        pod = event["object"]
        self.__update_modified_pod(pod=pod, event=event)

    def __update_modified_pod(self, pod: V1Pod, event: Optional[PodEvent]) -> None:
        """Called during reconciliation and normal event handling"""
        pod_name = pod.metadata.name
        task_metadata = self.task_metadata[pod_name]
        event_type = event["type"] if event else "null"

        raw_event = event["raw_object"] if event else None

        if pod.status.phase not in SUPPORTED_POD_MODIFIED_EVENT_PHASES:
            logger.debug(
                f"Got a {event_type} event for {pod_name} for unhandled phase: "
                f"{pod.status.phase} - ignoring."
            )
            return

        if (
            pod.status.phase in {"Succeeded", "Failed"}
            and task_metadata.task_state is KubernetesTaskState.TASK_PENDING
        ):
            logger.debug(
                f"Adding running event for {pod_name}, Kubernetes appears to have "
                "compacted the Running phase event."
            )
            self.task_metadata = self.task_metadata.set(
                pod_name,
                task_metadata.set(
                    node_name=pod.spec.node_name,
                    task_state=KubernetesTaskState.TASK_RUNNING,
                    task_state_history=task_metadata.task_state_history.append(
                        (KubernetesTaskState.TASK_RUNNING, time.time()),
                    ),
                ),
            )
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="running",
                )
            )

        if (
            pod.status.phase == "Succeeded"
            and task_metadata.task_state is not KubernetesTaskState.TASK_FINISHED
        ):
            logger.info(
                f"Removing {pod_name} from state and emitting 'finished' event.",
            )
            self.task_metadata = self.task_metadata.discard(pod_name)
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=True,
                    success=True,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="finished",
                )
            )
            return

        elif (
            pod.status.phase == "Failed"
            and task_metadata.task_state is not KubernetesTaskState.TASK_FAILED
        ):
            logger.info(f"Removing {pod_name} from state and emitting 'failed' event.")
            self.task_metadata = self.task_metadata.discard(pod_name)
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=True,
                    success=False,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="failed",
                )
            )
            return

        elif (
            pod.status.phase == "Running"
            and task_metadata.task_state is not KubernetesTaskState.TASK_RUNNING
        ):
            logger.info(f"Successfully launched {pod_name}, emitting 'running' event.")
            self.task_metadata = self.task_metadata.set(
                pod_name,
                task_metadata.set(
                    node_name=pod.spec.node_name,
                    task_state=KubernetesTaskState.TASK_RUNNING,
                    task_state_history=task_metadata.task_state_history.append(
                        (KubernetesTaskState.TASK_RUNNING, time.time()),
                    ),
                ),
            )
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="running",
                )
            )
            return

        # XXX: figure out how to handle this correctly (and when this actually
        # happens - we were unable to cajole k8s into giving us an event with an Unknown
        # phase)
        elif (
            pod.status.phase == "Unknown"
            and task_metadata.task_state is not KubernetesTaskState.TASK_LOST
        ):
            logger.info(
                f"Got a {event_type} event for {pod_name} with unknown phase, host likely "
                "unexpectedly died"
            )
            self.task_metadata = self.task_metadata.set(
                pod_name,
                task_metadata.set(
                    node_name=pod.spec.node_name,
                    task_state=KubernetesTaskState.TASK_LOST,
                    task_state_history=task_metadata.task_state_history.append(
                        (KubernetesTaskState.TASK_LOST, time.time()),
                    ),
                ),
            )
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="lost",
                )
            )
            return

        if self.emit_events_without_state_transitions:
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=raw_event,
                    task_config=task_metadata.task_config,
                    platform_type="running",
                )
            )
        else:
            logger.info(
                f"Ignoring {event_type} event for {pod_name} as it did not result "
                "in a state transition",
            )

    def _process_pod_event(self, event: PodEvent) -> None:
        """
        Router for handling Pod events based on event type. Currently, we only look at
        MODIFIED and DELETED events (we ignore ADDED since we handle all processing for
        those when we create Pods in the first place).

        NOTE: if we get multiple pod events while a Pod is running, we ignore all
        but the first one to cause us to transition to a new state to make processing
        easier since our watch might result in events with no state transitions and these
        don't have any value for us.

        """
        pod = event["object"]
        pod_name = pod.metadata.name
        with self.task_metadata_lock:
            if pod_name not in self.task_metadata:
                logger.info(
                    f"Ignoring event for {pod_name} - Pod not tracked by task_processing."
                )
                return None

            # this should only ever be run for Pods that were killed either by operator
            # action (e.g., kubectl delete pod) or by calling kill() - completed/failed
            # Pods will be removed from task_processing's metadata store
            elif event["type"] == "DELETED":
                self.__handle_deleted_pod_event(event)

            elif event["type"] in {"MODIFIED", "ADDED"}:
                self.__handle_modified_pod_event(event)

            else:
                logger.warning(
                    f"Got unknown event type for {pod_name}: {event['type']}"
                )

    def _pending_event_processing_loop(self) -> None:
        """
        Run in a thread to process PodEvents enqueued by the k8s event watcher thread.
        """
        logger.debug("Starting Pod event processing.")
        event = None
        while not self.stopping or not self.pending_events.empty():
            try:
                event = self.pending_events.get(timeout=QUEUE_GET_TIMEOUT_S)
                self._process_pod_event(event)
            except queue.Empty:
                logger.debug(
                    f"Pending event queue remained empty after {QUEUE_GET_TIMEOUT_S} seconds.",
                )
                # we explicitly continue here as this is the only code path that doesn't
                # need to call task_done()
                continue
            except Exception:
                logger.exception(
                    f"Exception encountered proccessing Pod events - skipping event {event} and "
                    "restarting processing!",
                )

            try:
                self.pending_events.task_done()
            except ValueError:
                # this should never happen since the only codepath that should ever get
                # here is after a successful get() - but just in case, we don't want to
                # have this thread die because of this
                logger.error(
                    "task_done() called on pending events queue too many times!"
                )

        logger.debug("Exiting Pod event processing - stop requested.")

    def _create_container_definition(
        self,
        name: str,
        task_config: KubernetesTaskConfig,
    ) -> V1Container:
        volume_mounts = (
            get_kubernetes_volume_mounts(task_config.volumes)
            + get_kubernetes_empty_volume_mounts(task_config.empty_volumes)
            + get_kubernetes_secret_volume_mounts(task_config.secret_volumes)
            + get_kubernetes_service_account_token_volume_mounts(
                task_config.projected_sa_volumes
            )
        )

        capabilities = get_capabilities_for_capability_changes(
            cap_add=task_config.cap_add,
            cap_drop=task_config.cap_drop,
        )

        security_context = None
        if capabilities is not None or task_config.privileged is not None:
            security_context = V1SecurityContext(
                capabilities=capabilities,
                privileged=task_config.privileged,
            )

        requests = {}
        # we need to explictly omit these if not set as k8s
        # (or the python clientlib) will actually set the
        # request to None instead of equal to the limit
        if task_config.cpus_request is not None:
            requests["cpu"] = task_config.cpus_request
        if task_config.memory_request is not None:
            requests["memory"] = f"{task_config.memory_request}Mi"

        limits = {
            "cpu": task_config.cpus,
            "memory": f"{task_config.memory}Mi",
            "ephemeral-storage": f"{task_config.disk}Mi",
        }
        resources = V1ResourceRequirements(
            limits=limits,
            # None is the default for an empty V1ResourceRequirements
            requests=requests if requests else None,
        )

        return V1Container(
            image=task_config.image,
            name=name,
            command=["/bin/sh", "-c"],
            args=[task_config.command],
            security_context=security_context,
            resources=resources,
            env=get_kubernetes_env_vars(
                environment=task_config.environment,
                secret_environment=task_config.secret_environment,
                field_selector_environment=task_config.field_selector_environment,
            ),
            volume_mounts=volume_mounts,
            ports=[V1ContainerPort(container_port=port) for port in task_config.ports],
            stdin=task_config.stdin,
            stdin_once=task_config.stdin_once,
            tty=task_config.tty,
        )

    def run(self, task_config: KubernetesTaskConfig) -> Optional[str]:
        try:
            # XXX: we were initially planning on using the name from KubernetesTaskConfig here,
            # but its too easy to go over the length limit for container names (63 characters),
            # so we're just hardcoding something for now since container names aren't used for
            # anything at the moment
            containers = [self._create_container_definition("main", task_config)]

            for name, nested_config in task_config.extra_containers.items():
                containers.append(
                    self._create_container_definition(
                        get_sanitised_kubernetes_name(name, length_limit=63),
                        nested_config,
                    )
                )

            volumes = (
                get_pod_volumes(task_config.volumes)
                + get_pod_empty_volumes(task_config.empty_volumes)
                + get_pod_secret_volumes(task_config.secret_volumes)
                + get_pod_service_account_token_volumes(
                    task_config.projected_sa_volumes
                )
            )

            pod = V1Pod(
                metadata=V1ObjectMeta(
                    name=task_config.pod_name,
                    namespace=self.namespace,
                    labels=dict(task_config.labels),
                    annotations=dict(task_config.annotations),
                ),
                spec=V1PodSpec(
                    restart_policy=task_config.restart_policy,
                    containers=containers,
                    volumes=volumes,
                    node_selector=dict(task_config.node_selectors),
                    affinity=V1Affinity(
                        node_affinity=get_node_affinity(task_config.node_affinities),
                    ),
                    topology_spread_constraints=get_topology_spread_constraints(
                        task_config.topology_spread_constraints
                    ),
                    # we're hardcoding this as Default as this is what we generally use
                    # internally - until we have a usecase for something that needs one
                    # of the other DNS policies, we can probably punt on plumbing all the
                    # bits for making this configurable
                    dns_policy="Default",
                    share_process_namespace=True,
                    security_context=V1PodSecurityContext(
                        fs_group=task_config.fs_group,
                    ),
                    service_account_name=task_config.service_account_name,
                ),
            )
        except Exception:
            logger.exception(f"Unable to create PodSpec for {task_config.pod_name}")
            return None

        # we need to lock here since there will be other threads updating this metadata in response
        # to k8s events
        with self.task_metadata_lock:
            self.task_metadata = self.task_metadata.set(
                task_config.pod_name,
                KubernetesTaskMetadata(
                    task_config=task_config,
                    task_state=KubernetesTaskState.TASK_PENDING,
                    task_state_history=v(
                        (KubernetesTaskState.TASK_PENDING, time.time())
                    ),
                ),
            )

        if self.kube_client.create_pod(
            namespace=self.namespace,
            pod=pod,
        ):
            return task_config.pod_name

        with self.task_metadata_lock:
            # if we weren't able to create this pod, then we shouldn't keep it around in
            # task_metadata
            self.task_metadata = self.task_metadata.discard(task_config.pod_name)

        return None

    def reconcile(self, task_config: KubernetesTaskConfig) -> None:
        pod_name = task_config.pod_name
        pod = None
        for kube_client in [self.kube_client] + self.watcher_kube_clients:
            try:
                pod = kube_client.get_pod(namespace=self.namespace, pod_name=pod_name)
            except Exception:
                logger.exception(
                    f"Hit an exception attempting to fetch pod {pod_name} from {kube_client.kubeconfig_path}"
                )
            else:
                # kube_client.get_pod will return None with no exception if it sees a 404 from API
                if pod:
                    break

        if pod_name not in self.task_metadata:
            self._initialize_existing_task(task_config)

        with self.task_metadata_lock:
            task_metadata = self.task_metadata[pod_name]
            self.task_metadata = self.task_metadata.set(
                pod_name, task_metadata.set(task_config=task_config)
            )

            if not pod:
                # Pod has gone away while restarting
                logger.info(
                    f"Pod {pod_name} for task {task_config.name} was no longer found. "
                    "Marking as LOST"
                )
                self.task_metadata = self.task_metadata.set(
                    pod_name,
                    task_metadata.set(
                        task_state=KubernetesTaskState.TASK_LOST,
                        task_state_history=task_metadata.task_state_history.append(
                            (KubernetesTaskState.TASK_LOST, time.time()),
                        ),
                    ),
                )
                self.event_queue.put(
                    task_event(
                        task_id=pod_name,
                        terminal=False,
                        timestamp=time.time(),
                        raw=None,
                        task_config=task_metadata.task_config,
                        platform_type="lost",
                    )
                )
            else:
                # Treat like a modified pod
                self.__update_modified_pod(pod=pod, event=None)

    def kill(self, task_id: str) -> bool:
        """
        Terminate a Pod by name.

        This function will request that Kubernetes delete the named Pod and will return
        True if the Pod termination request was succesfully emitted or False otherwise.
        """
        terminated = any(
            kube_client.terminate_pod(
                namespace=self.namespace,
                pod_name=task_id,
            )
            for kube_client in [self.kube_client] + self.watcher_kube_clients
        )
        if terminated:
            logger.info(
                f"Successfully requested termination for {task_id}. "
                "Emitting synthetic 'killed' event in case of Kubernetes event coalescion."
            )
            # we need to lock here since there will be other threads updating this metadata in response
            # to k8s events
            with self.task_metadata_lock:
                # NOTE: it's possible that there'll also be a real DELETED event for this Pod
                # but it should be safe to do this as _process_pod_event() will just ignore
                # pods not in self.task_metadata
                self.task_metadata = self.task_metadata.discard(task_id)
                self.event_queue.put(
                    task_event(
                        task_id=task_id,
                        terminal=True,
                        success=False,
                        timestamp=time.time(),
                        raw=None,
                        task_config=None,
                        platform_type="killed",
                    )
                )
        else:
            logger.error(f"Failed to request termination for {task_id}.")
        return terminated

    def stop(self) -> None:
        logger.debug("Preparing to stop all KubernetesPodExecutor threads.")
        self.stopping = True

        logger.debug("Signaling Pod event Watch to stop streaming events...")
        # make sure that we've stopped watching for events before calling join() - otherwise,
        # join() will block until we hit the configured timeout (or forever with no timeout).
        for watch in self.watches:
            watch.stop()
        # timeout arbitrarily chosen - we mostly just want to make sure that we have a small
        # grace period to flush the current event to the pending_events queue as well as
        # any other clean-up  - it's possible that after this join() the thread is still alive
        # but in that case we can be reasonably sure that we're not dropping any data.
        for pod_event_watch_thread in self.pod_event_watch_threads:
            pod_event_watch_thread.join(timeout=POD_WATCH_THREAD_JOIN_TIMEOUT_S)

        logger.debug("Waiting for all pending PodEvents to be processed...")
        # once we've stopped updating the pending events queue, we then wait until we're done
        # processing any events we've received - this will wait until task_done() has been
        # called for every item placed in this queue
        self.pending_events.join()
        logger.debug("All pending PodEvents have been processed.")
        # and then give ourselves time to do any post-stop cleanup
        self.pending_event_processing_thread.join(
            timeout=POD_EVENT_THREAD_JOIN_TIMEOUT_S
        )

        logger.debug("Done stopping KubernetesPodExecutor!")

    def get_event_queue(self) -> "Queue[Event]":
        return self.event_queue
