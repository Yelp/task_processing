import threading
import time
from enum import auto
from enum import unique
from typing import Tuple

from pyrsistent import field
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent.typing import PMap
from pyrsistent.typing import PVector as PVectorType

from task_processing.interfaces import TaskExecutor
from task_processing.plugins.kubernetes.kube_client import KubeClient
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.utils import AutoEnum


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
                    task_state_history=pvector(
                        (KubernetesTaskState.TASK_PENDING, int(time.time()))
                    ),
                ),
            )

        # TODO(TASKPROC-231): actually launch a Pod

    def reconcile(self, task_config: KubernetesTaskConfig) -> None:
        pass

    def kill(self, task_id: str) -> bool:
        pass

    def stop(self) -> None:
        pass

    def get_event_queue(self):
        pass
