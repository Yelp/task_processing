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
