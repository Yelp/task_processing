import logging
import threading
import time
from enum import auto
from enum import unique
from typing import Tuple

from kubernetes.client import V1Status
from pyrsistent import field
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v
from pyrsistent.typing import PMap
from pyrsistent.typing import PVector as PVectorType

from task_processing.interfaces import TaskExecutor
from task_processing.plugins.kubernetes.kube_client import KubeClient
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.utils import AutoEnum

logger = logging.getLogger(__name__)


@unique
class KubernetesTaskState(AutoEnum):
    TASK_PENDING = auto()


class KubernetesTaskMetadata(PRecord):
    # what box this task/Pod was scheduled onto
    node_name: str = field(type=str, initial='')

    # the config used to launch this task/Pod
    task_config: KubernetesTaskConfig = field(type=KubernetesTaskConfig, mandatory=True)

    # TODO(TASKPROC-241): add current task state and task state history as we did for mesos
    task_state: KubernetesTaskState = field(type=KubernetesTaskState, mandatory=True)
    # Map of state to when that state was entered (stored as a timestamp)
    task_state_history: PVectorType[Tuple[KubernetesTaskState, int]] = field(
        type=PVector, factory=pvector, mandatory=True)


class KubernetesPodExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = KubernetesTaskConfig

    def __init__(self, namespace: str) -> None:
        self.kube_client = KubeClient()
        self.namespace = namespace

        self.task_metadata: PMap[str, KubernetesTaskMetadata] = pmap()
        self._lock = threading.RLock()

    def run(self, task_config: KubernetesTaskConfig) -> None:
        # we need to lock here since there will be other threads updating this metadata in response
        # to k8s events
        with self._lock:
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

        # TODO(TASKPROC-231): actually launch a Pod

    def reconcile(self, task_config: KubernetesTaskConfig) -> None:
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
        pass

    def get_event_queue(self):
        pass
