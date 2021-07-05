import logging
import threading
import time
from enum import auto
from enum import unique
from typing import Tuple

from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client.apis import CoreV1Api
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
from task_processing.plugins.kubernetes.utils import ensure_namespace
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

    def __init__(self) -> None:
        self.kube_client = KubeClient()
        ensure_namespace(self.kube_client, "tron")
        self.task_metadata: PMap[str, KubernetesTaskMetadata] = pmap()
        self.api = CoreV1Api()
        self._lock = threading.RLock()

    def run(self, task_config: KubernetesTaskConfig) -> str:
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
        # TODO Add volume_devices and volume_mounts
        container = V1Container(
            image=task_config.image,
            name=task_config.name,
            command=[task_config.command]
        )
        pod = V1Pod(
            metadata=V1ObjectMeta(
                name=task_config.pod_name,
                namespace="tron"
            ),
            spec=V1PodSpec(
                restart_policy=task_config.restart_policy,
                containers=[container]
            ),
        )
        self.api.create_namespaced_pod(namespace="tron", body=pod)
        logger.debug(f"Successfully launched pod {task_config.pod_name}")

        return task_config.pod_name

    def reconcile(self, task_config: KubernetesTaskConfig) -> None:
        pass

    def kill(self, task_id: str) -> bool:
        pass

    def stop(self) -> None:
        pass

    def get_event_queue(self):
        pass
