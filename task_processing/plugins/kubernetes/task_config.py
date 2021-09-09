import re
import secrets
import string
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TYPE_CHECKING

from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

from task_processing.plugins.kubernetes.types import NodeAffinity
from task_processing.plugins.kubernetes.types import NodeAffinityOperator
from task_processing.plugins.kubernetes.utils import get_sanitised_kubernetes_name
if TYPE_CHECKING:
    from task_processing.plugins.kubernetes.types import DockerVolume
    from task_processing.plugins.kubernetes.types import SecretEnvSource

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface

POD_SUFFIX_ALPHABET = string.ascii_lowercase + string.digits
POD_SUFFIX_LENGTH = 6
# The max length is actually 253, but https://github.com/kubernetes/kubernetes/issues/91410 means
# that the effective limit is actually:
# 255 - 63 - 37 - 2 = 153
# or (max filename length) - (max namespace length) -(pod UID length) - (separators)
# but let's give ourselves a little buffer so we'll round down a bit
MAX_POD_NAME_LENGTH = 150
VALID_POD_NAME_REGEX = '[a-z0-9]([.-a-z0-9]*[a-z0-9])?'
VALID_VOLUME_KEYS = {'mode', 'container_path', 'host_path'}
VALID_SECRET_ENV_KEYS = {'secret_name', 'key'}
VALID_CAPABILITIES = {
    "AUDIT_CONTROL",
    "AUDIT_READ",
    "AUDIT_WRITE",
    "BLOCK_SUSPEND",
    "CHOWN",
    "DAC_OVERRIDE",
    "DAC_READ_SEARCH",
    "FOWNER",
    "FSETID",
    "IPC_LOCK",
    "IPC_OWNER",
    "KILL",
    "LEASE",
    "LINUX_IMMUTABLE",
    "MAC_ADMIN",
    "MAC_OVERRIDE",
    "MKNOD",
    "NET_ADMIN",
    "NET_BIND_SERVICE",
    "NET_BROADCAST",
    "NET_RAW",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYSLOG",
    "SYS_ADMIN",
    "SYS_BOOT",
    "SYS_CHROOT",
    "SYS_MODULE",
    "SYS_NICE",
    "SYS_PACCT",
    "SYS_PTRACE",
    "SYS_RAWIO",
    "SYS_RESOURCE",
    "SYS_TIME",
    "SYS_TTY_CONFIG",
    "WAKE_ALARM",
}
DEFAULT_CAPS_DROP = {
    "AUDIT_WRITE",
    "CHOWN",
    "DAC_OVERRIDE",
    "FOWNER",
    "FSETID",
    "KILL",
    "MKNOD",
    "NET_BIND_SERVICE",
    "NET_RAW",
    "SETFCAP",
    "SETGID",
    "SETPCAP",
    "SETUID",
    "SYS_CHROOT",
}
VALID_DOCKER_VOLUME_MODES = {"RW", "RO"}
REQUIRED_NODE_AFFINITY_KEYS = set(NodeAffinity.__annotations__.keys())


def _generate_pod_suffix() -> str:
    return ''.join(secrets.choice(POD_SUFFIX_ALPHABET) for i in range(POD_SUFFIX_LENGTH))


def _valid_volumes(volumes: Sequence["DockerVolume"]) -> Tuple[bool, Optional[str]]:
    for volume in volumes:
        if set(volume.keys()) != VALID_VOLUME_KEYS:
            return (
                False,
                f'Invalid volume format, must only contain following keys: '
                f'{VALID_VOLUME_KEYS}, got: {volume.keys()}'
            )
        if volume["mode"] not in VALID_DOCKER_VOLUME_MODES:
            return (
                False,
                f"Invalid mode for volume, must be one of {VALID_DOCKER_VOLUME_MODES}",
            )
    return (True, None)


def _valid_secret_envs(secret_envs: Mapping[str, "SecretEnvSource"]) -> Tuple[bool, Optional[str]]:
    # Note we are not validating existence of secret in k8s here, leave that to creation of pod
    for key, value in secret_envs.items():
        if set(value.keys()) != VALID_SECRET_ENV_KEYS:
            return (
                False,
                f'Invalid secret environment variable {key}, must only contain following keys: '
                f'{VALID_SECRET_ENV_KEYS}, got: {value.keys()}'
            )
    return (True, None)


def _valid_capabilities(capabilities: Sequence[str]) -> Tuple[bool, Optional[str]]:
    if (set(capabilities) & VALID_CAPABILITIES) != set(capabilities):
        return (
            False,
            f"Invalid capabilities - got {capabilities} but expected only values from "
            f"{VALID_CAPABILITIES}",
        )
    return (True, None)


def _valid_node_affinities(affinities: Sequence["NodeAffinity"]) -> Tuple[bool, Optional[str]]:
    for aff in affinities:
        missing_keys = REQUIRED_NODE_AFFINITY_KEYS.difference(set(aff.keys()))
        if missing_keys:
            return (
                False,
                f"Invalid node affinity: got {aff} but missing keys {missing_keys}"
            )

        op, val = aff["operator"], aff["value"]
        if op not in NodeAffinityOperator:
            valid_operators = list(o.value for o in NodeAffinityOperator)
            return (
                False,
                f"Invalid node affinity operator: got '{op}', "
                f"but expected one of: {valid_operators}",
            )

        elif (
            op in {NodeAffinityOperator.IN, NodeAffinityOperator.NOT_IN} and
            type(val) not in {list, tuple}
        ):
            return (
                False,
                "Invalid node affinity value: "
                f"got non-list value '{val}' for affinity operator '{op}'"
            )

        elif (
            op in {NodeAffinityOperator.GT, NodeAffinityOperator.LT} and
            type(val) != int
        ):
            return (
                False,
                "Invalid node affinity value: "
                f"got non-int value '{val}' for affinity operator '{op}'",
            )

    return True, None


class KubernetesTaskConfig(DefaultTaskConfigInterface):
    def __invariant__(self) -> Tuple[Tuple[bool, str], ...]:
        valid_length = len(self.pod_name) <= MAX_POD_NAME_LENGTH
        valid_name = bool(re.match(VALID_POD_NAME_REGEX, self.pod_name))

        return (
            (valid_length, f'Pod name must have up to {MAX_POD_NAME_LENGTH} characters.'),
            (valid_name, 'Must comply with Kubernetes pod naming standards.'),
        )

    uuid = field(type=str, initial=_generate_pod_suffix)  # type: ignore
    name = field(type=str, initial="default")
    # Hardcoded for the time being
    restart_policy = "Never"
    # By default, the retrying executor retries 3 times. This task option
    # overrides the executor setting.
    retries = field(
        type=int,
        factory=int,
        mandatory=False,
        invariant=lambda r: (r >= 0, 'retries >= 0')
    )

    image = field(type=str, mandatory=True)
    command = field(
        type=str,
        mandatory=True,
        invariant=lambda cmd: (cmd.strip() != '', 'empty command is not allowed')
    )
    volumes = field(
        type=PVector if not TYPE_CHECKING else PVector["DockerVolume"],
        initial=v(),
        factory=pvector,
        invariant=_valid_volumes,
    )

    cpus = field(
        type=float,
        initial=0.1,
        factory=float,
        invariant=lambda c: (c > 0, 'cpus > 0'))
    memory = field(
        type=float,
        initial=128.0,
        factory=float,
        invariant=lambda m: (m >= 32, 'mem is >= 32'))
    disk = field(
        type=float,
        initial=10.0,
        factory=float,
        invariant=lambda d: (d > 0, 'disk > 0'))
    environment = field(
        type=PMap if not TYPE_CHECKING else PMap[str, str],
        initial=m(),
        factory=pmap,
    )
    secret_environment = field(
        type=PMap if not TYPE_CHECKING else PMap[str, 'SecretEnvSource'],
        initial=m(),
        factory=pmap,
        invariant=_valid_secret_envs,
    )
    cap_add = field(
        type=PVector if not TYPE_CHECKING else PVector[str],
        initial=v(),
        factory=pvector,
        invariant=_valid_capabilities,
    )
    cap_drop = field(
        type=PVector if not TYPE_CHECKING else PVector[str],
        initial=pvector(DEFAULT_CAPS_DROP),
        factory=pvector,
        invariant=_valid_capabilities,
    )
    node_selectors = field(
        type=PMap if not TYPE_CHECKING else PMap[str, str],
        initial=m(),
        factory=pmap,
    )
    node_affinities = field(
        type=PVector if not TYPE_CHECKING else PVector["NodeAffinity"],
        initial=v(),
        factory=pvector,
        invariant=_valid_node_affinities,
    )
    labels = field(
        type=PMap if not TYPE_CHECKING else PMap[str, str],
        initial=m(),
        factory=pmap,
    )
    fs_group = field(
        type=int,
        # this is the `nobody` user at Yelp, which is what we should always be using
        # and, as such, is probably the best default to add here.
        initial=65534,
        invariant=lambda group: (0 <= group <= 65534, 'fs_group must be >= 0 and <= 65,534'),
    )

    @property
    def pod_name(self) -> str:
        return get_sanitised_kubernetes_name(
            f'{self.name}.{self.uuid}',  # type: ignore
            length_limit=MAX_POD_NAME_LENGTH,
        )

    def set_pod_name(self, pod_name: str):
        try:
            name, uuid = pod_name.rsplit('.', maxsplit=1)
        except ValueError:
            raise ValueError(f'Invalid format for pod_name {pod_name}')

        return self.set(name=name, uuid=uuid)
