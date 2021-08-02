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
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from pyrsistent.typing import PMap
from pyrsistent.typing import PVector

if TYPE_CHECKING:
    from task_processing.plugins.kubernetes.types import DockerVolume
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


def get_sanitised_kubernetes_name(name: str, replace_dots: bool = False) -> str:
    """
    Helper to ensure that any names given to Kubernetes objects follow our conventions

    replace_dots is an optional parameter for objects such as Containers that cannot contain `.`s in
    their names (in contrast to objects such as Pods that can)
    """
    name = name.replace("_", "--")
    if name.startswith("--"):
        name = name.replace("--", "underscore-", 1)
    if replace_dots:
        name = name.replace(".", "dot-")
    return name.lower()


def get_sanitised_volume_name(volume_name: str, length_limit: int = 0) -> str:
    """
    Helper to ensure that volume names follow our conventions (and respect an optional length limit)

    NOTE: For volume names exceeding the length limit, we'll truncate the name and replace the
    truncated portion with a unique suffix.
    """
    volume_name = volume_name.rstrip("/")
    sanitised = volume_name.replace("/", "slash-").replace(".", "dot-")
    sanitised_name = get_sanitised_kubernetes_name(sanitised)
    if length_limit and len(sanitised_name) > length_limit:
        sanitised_name = (
            sanitised_name[0:length_limit - 6]
            + "--"
            + hashlib.md5(sanitised_name.encode("ascii")).hexdigest()[:4]
        )
    return sanitised_name


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
