import hashlib
import logging
import re
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

SECRET_VALUE_REGEX = re.compile(r"^(SHARED_)?SECRET\([A-Za-z0-9_-]*\)$")
SHARED_SECRET_SERVICE = "_shared"

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


def is_secret_env_var(value: str) -> bool:
    """
    Given the value of an environment variable, return if that environment variable
    represents a secret.
    """
    return SECRET_VALUE_REGEX.match(value) is not None


def get_secret_name_from_ref(value: str) -> str:
    """
    Given a PaaSTA-style secret reference, return the name of the secret.

    Supported secret references types:
        * SECRET(value)
        * SHARED_SECRET(value)
    """
    return value.split("(")[1][:-1]


def is_shared_secret(value: str) -> bool:
    """
    In some cases, multiple services need access to the same secrets - rather than storing
    these N times internally, Yelp has a mechanism in which a secret can be tagged as
    "shared" and thus only need to be modified in one place should that secret need to be
    rotated/updated/etc.

    These "shared" secrets are referenced using the format SHARED_SECRET(secret_name)
    instead of the more tightly-scoped format of SECRET(secret_name)
    """
    return value.startswith("SHARED_")


def get_secret_kubernetes_env_var(
    key: str, value: str, task_name: str, namespace: str,
) -> V1EnvVar:
    """
    Returns a Kubernetes EnvVar object that will pull the plaintext of a secret from the
    Kubernetes Secrets store.

    Will attempt to use the task name to retrieve the corresponding secret unless the value
    referenced is a shared secret (in which case, a special name is used).

    This expects Kubernetes Secrets to have been created by an external process matching a
    specific naming convention based on Kubernetes namespace, task name, and the requested
    secret name.

    XXX:  document how these work internally for non-Yelpers?
    """
    task_prefix = task_name.split('.')[0] if not is_shared_secret(value) else SHARED_SECRET_SERVICE
    sanitised_task_prefix = get_sanitised_kubernetes_name(task_prefix)

    secret = get_secret_name_from_ref(value)
    sanitised_secret = get_sanitised_kubernetes_name(secret)

    return V1EnvVar(
        name=key,
        value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(
                name=f"{namespace}-secret-{sanitised_task_prefix}-{sanitised_secret}",
                key=secret,
                optional=False,
            )
        ),
    )


def get_kubernetes_env_vars(
    environment: PMap[str, str], task_name: str, namespace: str,
) -> List[V1EnvVar]:
    """
    Given a dict of environment variables, transform them into the corresponding Kubernetes
    representation. This function will replace any secret placeholders with the value of
    the actual secret.
    """
    env_vars = [
        V1EnvVar(name=key, value=value) for key, value
        in environment.items()
        if not is_secret_env_var(value)
    ]

    secret_env_vars = [
        get_secret_kubernetes_env_var(
            key=key, value=value, task_name=task_name, namespace=namespace
        )
        for key, value
        in environment.items()
        if is_secret_env_var(value)
    ]
    return env_vars + secret_env_vars


def get_sanitised_kubernetes_name(name: str) -> str:
    """
    Helper to ensure that any names given to Kubernetes objects follow our conventions
    """
    name = name.replace("_", "--")
    if name.startswith("--"):
        name = name.replace("--", "underscore-", 1)
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
