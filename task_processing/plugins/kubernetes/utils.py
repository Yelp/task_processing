import hashlib
import logging
from typing import Dict
from typing import List
from typing import Optional
from typing import TYPE_CHECKING

from kubernetes.client import V1Capabilities
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1NodeAffinity
from kubernetes.client import V1NodeSelector
from kubernetes.client import V1NodeSelectorRequirement
from kubernetes.client import V1NodeSelectorTerm
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from pyrsistent.typing import PMap
from pyrsistent.typing import PVector

from task_processing.plugins.kubernetes.types import NodeAffinityOperator
if TYPE_CHECKING:
    from task_processing.plugins.kubernetes.types import DockerVolume
    from task_processing.plugins.kubernetes.types import NodeAffinity
    from task_processing.plugins.kubernetes.types import SecretEnvSource

logger = logging.getLogger(__name__)


def get_security_context_for_capabilities(
    cap_add: PVector[str],
    cap_drop: PVector[str],
) -> Optional[V1SecurityContext]:
    """
    Helper to take lists of capabilties to add/drop and turn them into the
    corresponding Kubernetes representation.
    """
    caps = {
        capability_type: capabilities
        for (capability_type, capabilities)
        in [("add", list(cap_add)), ("drop", list(cap_drop))]
        if capabilities
    }
    if caps:
        return V1SecurityContext(capabilities=V1Capabilities(**caps))

    logger.info(
        "No capabilities found, not creating a security context"
    )
    return None


def get_kubernetes_env_vars(
    environment: PMap[str, str], secret_environment: PMap[str, 'SecretEnvSource'],
) -> List[V1EnvVar]:
    """
    Given a dict of environment variables, transform them into the corresponding Kubernetes
    representation. This function will replace any secret placeholders with the value of
    the actual secret.
    """
    env_vars = [
        V1EnvVar(name=key, value=value) for key, value
        in environment.items() if key not in secret_environment.keys()
    ]

    secret_env_vars = [
        V1EnvVar(name=key, value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(
                name=value["secret_name"],
                key=value["key"],
                optional=False,
            ),
        ),
        )
        for key, value
        in secret_environment.items()
    ]

    return env_vars + secret_env_vars


def get_sanitised_kubernetes_name(
    name: str,
    replace_dots: bool = False,
    replace_forward_slash: bool = False,
    length_limit: int = 0,
) -> str:
    """
    Helper to ensure that any names given to Kubernetes objects follow our conventions

    replace_dots is an optional parameter for objects such as Containers that cannot contain `.`s in
    their names (in contrast to objects such as Pods that can)

    replace_forward_slash is an optional parameter for objects that may contain / in their "pretty"
    names, but that cannot contain / in their Kubernetes name

    NOTE: For names exceeding the length limit, we'll truncate the name and replace the
    truncated portion with a unique suffix.
    """
    name = name.replace("_", "--")
    if name.startswith("--"):
        name = name.replace("--", "underscore-", 1)
    if replace_dots:
        name = name.replace(".", "dot-")
    if replace_forward_slash:
        name = name.replace("/", "slash-")

    if length_limit and len(name) > length_limit:
        # for names that exceed the length limit, we'll remove 6 characters from
        # the part of the name that does fit in the limit in order to replace them with
        # -- (2 characters) and then a 4 character hash (which should help ensure uniqueness
        # after truncation).
        name = (
            name[0:length_limit - 6]
            + "--"
            + hashlib.md5(name.encode("ascii")).hexdigest()[:4]
        )
    return name.lower()


def get_sanitised_volume_name(volume_name: str, length_limit: int = 0) -> str:
    """
    Helper to ensure that volume names follow our conventions (and respect an optional length limit)

    NOTE: For volume names exceeding the length limit, we'll truncate the name and replace the
    truncated portion with a unique suffix.
    """
    volume_name = volume_name.rstrip("/")
    return get_sanitised_kubernetes_name(
        volume_name,
        replace_dots=True,
        replace_forward_slash=True,
        length_limit=length_limit,
    )


def get_kubernetes_volume_mounts(volumes: PVector['DockerVolume']) -> List[V1VolumeMount]:
    """
    Given a list of volume mounts, return a list corresponding to the Kubernetes objects
    representing these mounts.
    """
    return [
        V1VolumeMount(
            mount_path=volume["container_path"],
            name=get_sanitised_volume_name(
                f"host--{volume['host_path']}",
                length_limit=63
            ),
            read_only=volume.get("mode", "RO") == "RO",
        )
        for volume in volumes
    ]


def get_pod_volumes(volumes: PVector['DockerVolume']) -> List[V1Volume]:
    """
    Given a list of volume mounts, return a list corresponding to the Kubernetes objects needed to
    tie the mounts to a Pod.
    """
    unique_volumes: Dict[str, 'DockerVolume'] = {
        get_sanitised_volume_name(f"host--{volume['host_path']}", length_limit=63): volume
        for volume in volumes
    }

    return [
        V1Volume(
            host_path=V1HostPathVolumeSource(path=volume["host_path"]),
            name=name,
        )
        for name, volume in unique_volumes.items()
    ]


def get_node_affinity(affinities: PVector["NodeAffinity"]) -> Optional[V1NodeAffinity]:
    # convert NodeAffinity into V1NodeSelectorRequirement
    match_expressions = []
    for aff in affinities:
        op = aff["operator"]
        val = aff["value"]

        # operator and value are assumed to be validated
        if op in {NodeAffinityOperator.IN, NodeAffinityOperator.NOT_IN}:
            val = [str(v) for v in val]
        elif op in {NodeAffinityOperator.GT, NodeAffinityOperator.LT}:
            val = [str(val)]
        elif op in {NodeAffinityOperator.EXISTS, NodeAffinityOperator.DOES_NOT_EXIST}:
            val = []
        else:
            continue
        match_expressions.append(
            V1NodeSelectorRequirement(key=str(aff["key"]), operator=op, values=val)
        )

    # package into V1NodeAffinity
    if not match_expressions:
        return None
    return V1NodeAffinity(
        # this means that the selectors are only used during scheduling.
        # changing it while the pod is running will not cause an eviction.
        required_during_scheduling_ignored_during_execution=V1NodeSelector(
            node_selector_terms=[V1NodeSelectorTerm(match_expressions=match_expressions)],
        ),
    )
