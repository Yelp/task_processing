import logging
import queue
import threading
import time
from queue import Queue
from typing import Optional

from kubernetes import watch
from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client.exceptions import ApiException
from pyrsistent import pmap
from pyrsistent import v
from pyrsistent.typing import PMap

from task_processing.interfaces import TaskExecutor
from task_processing.interfaces.event import Event
from task_processing.interfaces.event import task_event
from task_processing.plugins.kubernetes.kube_client import KubeClient
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.plugins.kubernetes.types import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.types import KubernetesTaskState
from task_processing.plugins.kubernetes.types import PodEvent

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


class KubernetesPodExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = KubernetesTaskConfig

    def __init__(self, namespace: str, kubeconfig_path: Optional[str] = None) -> None:
        self.kube_client = KubeClient(kubeconfig_path=kubeconfig_path)
        self.namespace = namespace
        self.stopping = False
        self.task_metadata: PMap[str, KubernetesTaskMetadata] = pmap()
        self.task_metadata_lock = threading.RLock()

        # we have two queues since we need to process Pod Events into something that we can place
        # onto the queue that any application (e.g., something like Tron or Jolt) can actually use
        # and we've opted to not do that processing in the Pod event watcher thread so as to keep
        # that logic for the threads that operate on them as simple as possible and to make it
        # possible to cleanly shutdown both of these.
        self.pending_events: "Queue[PodEvent]" = Queue()
        self.event_queue: "Queue[Event]" = Queue()

        # TODO(TASKPROC-243): keep track of resourceVersion so that we can continue event processing
        # from where we left off on restarts
        self.watch = watch.Watch()
        self.pod_event_watch_thread = threading.Thread(
            target=self._pod_event_watch_loop,
            # ideally this wouldn't be a daemon thread, but a watch.Watch() only checks
            # if it should stop after receiving an event - and it's possible that we
            # have periods with no events so instead we'll attempt to stop the watch
            # and then join() with a small timeout to make sure that, if we shutdown
            # with the thread alive, we did not drop any events
            daemon=True,
        )
        self.pod_event_watch_thread.start()

        self.pending_event_processing_thread = threading.Thread(
            target=self._pending_event_processing_loop,
        )
        self.pending_event_processing_thread.start()

    def _pod_event_watch_loop(self) -> None:
        logger.debug(f"Starting watching Pod events for namespace={self.namespace}.")
        # TODO(TASKPROC-243): we'll need to correctly handle resourceVersion expiration for the case
        # where the gap between task_proc shutting down and coming back up is long enough for data
        # to have expired from etcd as well as actually restarting from the last resourceVersion in
        # case of an exception
        # TODO: Do LIST + WATCH if we're not starting from a known/good resourceVersion to
        # guarantee that we always get ordered events (starting from a resourceVersion of 0)
        # has no such guarantees that the initial events will be ordered in any meaningful way
        # see: https://github.com/kubernetes/kubernetes/issues/74022
        while not self.stopping:
            try:
                for pod_event in self.watch.stream(
                    self.kube_client.core.list_namespaced_pod,
                    self.namespace
                ):
                    # it's possible that we've received an event after we've already set the stop
                    # flag since Watch streams block forever, so re-check if we've stopped before
                    # queueing any pending events
                    if not self.stopping:
                        logger.debug("Adding Pod event to pending event queue.")
                        self.pending_events.put(pod_event)
                    else:
                        break
            except ApiException as e:
                if not self.stopping:
                    if not self.kube_client.maybe_reload_on_exception(exception=e):
                        logger.exception(
                            "Unhandled API exception while watching Pod events - restarting watch!"
                        )
            except Exception:
                # we want to avoid a potentially misleading log message should we encounter
                # an exception when we want to shutdown this thread since nothing of value
                # will be lost anyway (example scenario: we've called stop() on an idle cluster
                # but the Watch is still blocking and eventually gets disconnected)
                if not self.stopping:
                    logger.exception(
                        "Exception encountered while watching Pod events - restarting watch!")
        logger.debug("Exiting Pod event watcher - stop requested.")

    def __handle_deleted_pod_event(self, event: PodEvent) -> None:
        pod = event["object"]
        pod_name = pod.metadata.name
        task_metadata = self.task_metadata[pod_name]

        logger.info(f"Removing {pod_name} from state and emitting 'killed' event.")

        self.task_metadata = self.task_metadata.discard(pod_name)
        self.event_queue.put(
            task_event(
                task_id=pod_name,
                terminal=True,
                success=False,
                timestamp=time.time(),
                raw=event["raw_object"],
                task_config=task_metadata.task_config,
                platform_type="killed"
            )
        )

    def __handle_modified_pod_event(self, event: PodEvent) -> None:
        pod = event["object"]
        pod_name = pod.metadata.name
        task_metadata = self.task_metadata[pod_name]

        if pod.status.phase not in SUPPORTED_POD_MODIFIED_EVENT_PHASES:
            logger.debug(
                f"Got a MODIFIED event for {pod_name} for unhandled phase: "
                f"{pod.status.phase} - ignoring."
            )
            return

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
                    raw=event["raw_object"],
                    task_config=task_metadata.task_config,
                    platform_type="finished"
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
                    raw=event["raw_object"],
                    task_config=task_metadata.task_config,
                    platform_type="FAILED"
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
                    node_name=pod.status.host_ip,
                    task_state=KubernetesTaskState.TASK_RUNNING,
                    task_state_history=task_metadata.task_state_history.append(
                        (KubernetesTaskState.TASK_RUNNING, time.time()),
                    )
                )
            )
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=event["raw_object"],
                    task_config=task_metadata.task_config,
                    platform_type="running"
                )
            )
            return

        # XXX: figure out how to handle this correctly (and when this actually
        # happens - we were unable to cajole k8s into giving us an event with an Unknown
        # phase)
        elif (
            pod.status.phase == "Unknown"
            and task_metadata.task_state is not KubernetesTaskState.TASK_UNKNOWN
        ):
            logger.info(
                f"Got a MODIFIED event for {pod_name} with unknown phase, host likely "
                "unexpectedly died"
            )
            self.task_metadata = self.task_metadata.set(
                pod_name,
                task_metadata.set(
                    node_name=pod.spec.node_name,
                    task_state=KubernetesTaskState.TASK_UNKNOWN,
                    task_state_history=task_metadata.task_state_history.append(
                        (KubernetesTaskState.TASK_UNKNOWN, time.time()),
                    )
                )
            )
            self.event_queue.put(
                task_event(
                    task_id=pod_name,
                    terminal=False,
                    timestamp=time.time(),
                    raw=event["raw_object"],
                    task_config=task_metadata.task_config,
                    platform_type="unknown"
                )
            )
            return

        logger.info(
            f"Ignoring MODIFIED event for {pod_name} as it did not result "
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
                logger.info(f"Ignoring event for {pod_name} - Pod not tracked by task_processing.")
                return None

            # this should only ever be run for Pods that were killed either by operator
            # action (e.g., kubectl delete pod) or by calling kill() - completed/failed
            # Pods will be removed from task_processing's metadata store
            elif event["type"] == "DELETED":
                self.__handle_deleted_pod_event(event)

            elif event["type"] == "MODIFIED":
                self.__handle_modified_pod_event(event)

            else:
                logger.warning(f"Got unknown event type for {pod_name}: {event['type']}")

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
                logger.error("task_done() called on pending events queue too many times!")

        logger.debug("Exiting Pod event processing - stop requested.")

    def run(self, task_config: KubernetesTaskConfig) -> Optional[str]:
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
        # TODO (TASKPROC-238): Add volume, cpu, gpu, desk, mem, etc.
        container = V1Container(
            image=task_config.image,
            name=task_config.name,
            command=["/bin/sh", "-c"],
            args=[task_config.command],
        )
        pod = V1Pod(
            metadata=V1ObjectMeta(
                name=task_config.pod_name,
                namespace=self.namespace
            ),
            spec=V1PodSpec(
                restart_policy=task_config.restart_policy,
                containers=[container]
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
        # TASKPROC(244): we probably only want reconcile() to fill in the task_config
        # member of the relevant KubernetesTaskMetadata and do an unconditional state
        # reconciliation on startup
        pass

    def kill(self, task_id: str) -> bool:
        """
        Terminate a Pod by name.

        This function will request that Kubernetes delete the named Pod and will return
        True if the Pod termination request was succesfully emitted or False otherwise.
        """
        # NOTE: we're purposely not removing this task from `task_metadata` as we want
        # to handle that with the Watch that we'll set to monitor each Pod for events.
        # TODO(TASKPROC-242): actually handle termination events
        return self.kube_client.terminate_pod(
            namespace=self.namespace,
            pod_name=task_id,
        )

    def stop(self) -> None:
        logger.debug("Preparing to stop all KubernetesPodExecutor threads.")
        self.stopping = True

        logger.debug("Signaling Pod event Watch to stop streaming events...")
        # make sure that we've stopped watching for events before calling join() - otherwise,
        # join() will block until we hit the configured timeout (or forever with no timeout).
        self.watch.stop()
        # timeout arbitrarily chosen - we mostly just want to make sure that we have a small
        # grace period to flush the current event to the pending_events queue as well as
        # any other clean-up  - it's possible that after this join() the thread is still alive
        # but in that case we can be reasonably sure that we're not dropping any data.
        self.pod_event_watch_thread.join(timeout=POD_WATCH_THREAD_JOIN_TIMEOUT_S)

        logger.debug("Waiting for all pending PodEvents to be processed...")
        # once we've stopped updating the pending events queue, we then wait until we're done
        # processing any events we've received - this will wait until task_done() has been
        # called for every item placed in this queue
        self.pending_events.join()
        logger.debug("All pending PodEvents have been processed.")
        # and then give ourselves time to do any post-stop cleanup
        self.pending_event_processing_thread.join(timeout=POD_EVENT_THREAD_JOIN_TIMEOUT_S)

        logger.debug("Done stopping KubernetesPodExecutor!")

    def get_event_queue(self) -> "Queue[Event]":
        return self.event_queue
