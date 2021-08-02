from enum import auto
from enum import unique
from typing import Any
from typing import Dict
from typing import Tuple

from kubernetes.client import V1Pod
from pyrsistent import field
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent.typing import PVector as PVectorType
from typing_extensions import TypedDict

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.utils import AutoEnum


class DockerVolume(TypedDict):
    host_path: str
    container_path: str
    mode: str  # XXX: Literal["RO", "RW"] once we drop older Python support


class SecretEnvSource(TypedDict):
    secret_name: str  # full name of k8s secret resource
    key: str


class PodEvent(TypedDict):
    # there are only 3 possible types for Pod events: ADDED, DELETED, MODIFIED
    # XXX: this should be typed as Literal["ADDED", "DELETED", "MODIFIED"] once we drop support
    # for older Python versions
    type: str
    object: V1Pod
    # this is just the dict-ified version of object - but it's too big to type here
    raw_object: Dict[str, Any]


@unique
class KubernetesTaskState(AutoEnum):
    TASK_PENDING = auto()
    TASK_RUNNING = auto()
    TASK_FINISHED = auto()
    TASK_FAILED = auto()
    TASK_UNKNOWN = auto()
    TASK_LOST = auto()


class KubernetesTaskMetadata(PRecord):
    # what box this task/Pod was scheduled onto
    node_name: str = field(type=str, initial='')

    # the config used to launch this task/Pod
    task_config: KubernetesTaskConfig = field(type=KubernetesTaskConfig, mandatory=True)

    task_state: KubernetesTaskState = field(type=KubernetesTaskState, mandatory=True)
    # List of state to when that state was entered (stored as a timestamp)
    task_state_history: PVectorType[Tuple[KubernetesTaskState, float]] = field(
        type=PVector, factory=pvector, mandatory=True)
