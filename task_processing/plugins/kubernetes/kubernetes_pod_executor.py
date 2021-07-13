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
from kubernetes.client import V1Status
from pyrsistent import pmap
from pyrsistent import v
from pyrsistent.typing import PMap

from task_processing.interfaces import TaskExecutor
from task_processing.interfaces.event import Event
from task_processing.plugins.kubernetes.kube_client import KubeClient
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.plugins.kubernetes.types import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.types import KubernetesTaskState
from task_processing.plugins.kubernetes.types import PodEvent

logger = logging.getLogger(__name__)

POD_WATCH_THREAD_JOIN_TIMEOUT_S = 1.0
POD_EVENT_THREAD_JOIN_TIMEOUT_S = 1.0
QUEUE_GET_TIMEOUT_S = 0.5


class KubernetesPodExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = KubernetesTaskConfig

    def __init__(self, namespace: str, kube_config_path: Optional[str] = None) -> None:
        self.kube_client = KubeClient(kube_config_path=kube_config_path)
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
            except Exception:
                # we want to avoid a potentially misleading log message should we encounter
                # an exception when we want to shutdown this thread since nothing of value
                # will be lost anyway (example scenario: we've called stop() on an idle cluster
                # but the Watch is still blocking and eventually gets disconnected)
                if not self.stopping:
                    logger.exception(
                        "Exception encountered while watching Pod events - restarting watch!")
        logger.debug("Exiting Pod event watcher - stop requested.")

    def _process_pod_event(self, event: PodEvent) -> None:
        # XXX: ensure that anything here that updates self.task_metadata acquires a lock
        if event["type"] == "ADDED":
            pass
        elif event["type"] == "DELETED":
            pass
        elif event["type"] == "MODIFIED":
            pass
        else:
            logger.warning("Got unknown event with type: {event['type']}")

        # TODO(TASKPROC-245): actually create real events to send to Tron by placing them in
        # this plugin's event queue

    def _pending_event_processing_loop(self) -> None:
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
                        (KubernetesTaskState.TASK_PENDING, int(time.time()))
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

        try:
            self.kube_client.core.create_namespaced_pod(namespace=self.namespace, body=pod)
        except Exception:
            logger.exception(f"Failed to create pod {task_config.pod_name}")
            return None

        logger.debug(f"Successfully created pod {task_config.pod_name}")

        return task_config.pod_name

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
        logger.info(f"Attempting to terminate Pod: {task_id}")
        try:
            status: V1Status = self.kube_client.core.delete_namespaced_pod(
                name=task_id,
                namespace=self.namespace,
                # attempt to delete immediately - Pods launched by task_processing
                # shouldn't need time to clean-up/drain
                grace_period_seconds=0,
                # this is the default, but explcitly request background deletion of releated objects
                # see: https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
                propagation_policy="Background"
            )
        except Exception:
            logger.exception(f"Failed to request termination for Pod: {task_id}")
            return False

        # this is not ideal, but the k8s clientlib returns the status of the request as a string
        # that is either "Success" or "Failure" - we could potentially use `code` instead
        # but it's not exactly documented what HTTP return codes will be used
        return status.status == "Success"

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
