from typing import Any
from typing import Dict

from kubernetes.client import V1Pod
from typing_extensions import TypedDict


class DockerVolume(TypedDict):
    host_path: str
    container_path: str
    mode: str  # XXX: Literal["RO", "RW"] once we drop older Python support


class SecretEnvSource(TypedDict):
    secret_name: str  # full name of k8s secret resource
    key: str


class NodeAffinityOperator:
    IN = "In"
    NOT_IN = "NotIn"
    EXISTS = "Exists"
    DOES_NOT_EXIST = "DoesNotExist"
    GT = "Gt"
    LT = "Lt"
    ALL = {IN, NOT_IN, EXISTS, DOES_NOT_EXIST, GT, LT}


class NodeAffinity(TypedDict):
    key: str
    operator: str
    value: Any


class PodEvent(TypedDict):
    # there are only 3 possible types for Pod events: ADDED, DELETED, MODIFIED
    # XXX: this should be typed as Literal["ADDED", "DELETED", "MODIFIED"] once we drop support
    # for older Python versions
    type: str
    object: V1Pod
    # this is just the dict-ified version of object - but it's too big to type here
    raw_object: Dict[str, Any]
