import re
import secrets
import string

from pyrsistent import field
from pyrsistent import PMap
from pyrsistent import PVector

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface

POD_SUFFIX_ALPHABET = string.ascii_lowercase + string.digits
POD_SUFFIX_LENGTH = 6
MAX_POD_NAME_LENGTH = 253
VALID_POD_NAME_REGEX = '[a-z0-9]([.-a-z0-9]*[a-z0-9])?'


def _generate_pod_suffix() -> str:
    return ''.join(secrets.choice(POD_SUFFIX_ALPHABET) for i in range(POD_SUFFIX_LENGTH))


class KubernetesTaskConfig(DefaultTaskConfigInterface):
    def __invariant__(conf):
        return (
            (
                len(conf.pod_name) < MAX_POD_NAME_LENGTH,
                (
                    f'Pod name must have up to {MAX_POD_NAME_LENGTH} characters.'
                )
            ),
            (
                re.match(VALID_POD_NAME_REGEX, conf.pod_name),
                (
                    'Must comply with Kubernetes pod naming standards.'
                )
            )
        )

    uuid = field(type=str, initial=_generate_pod_suffix)
    name = field(type=str, initial="default")
    node_selector = field(type=PMap)
    containers = field(type=PVector)
    # Hardcoded for the time being
    restart_policy = "Never"
    # By default, the retrying executor retries 3 times. This task option
    # overrides the executor setting.
    retries = field(type=int,
                    factory=int,
                    mandatory=False,
                    invariant=lambda r: (r >= 0, 'retries >= 0'))
    volumes = field(type=PVector)

    @property
    def pod_name(self) -> str:
        return f'{self.name}.{self.uuid}'

    def set_pod_name(self, pod_name: str):
        try:
            name, uuid = pod_name.rsplit('.', maxsplit=1)
        except ValueError:
            raise ValueError(f'Invalid format for pod_name {pod_name}')

        return self.set(name=name, uuid=uuid)
