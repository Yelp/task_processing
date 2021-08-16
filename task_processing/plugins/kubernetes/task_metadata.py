from enum import auto
from enum import unique
from typing import Tuple

from pyrsistent import field
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent.typing import PVector as PVectorType

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.utils import AutoEnum


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
