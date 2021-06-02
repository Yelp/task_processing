import re
import secrets

from pyrsistent import field
from pyrsistent import PMap
from pyrsistent import PVector

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface

BASE_58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
UUID_BYTES = 6
MAX_POD_NAME_LENGTH = 253
REGEX_MATCH = '[a-z0-9]([.-a-z0-9]*[a-z0-9])?'


class KubernetesTaskConfig(DefaultTaskConfigInterface):
    def __invariant__(conf):
        return(
            (
                len(conf.pod_name) > MAX_POD_NAME_LENGTH,
                (
                    f'Invalid format for pod_name {conf.pod_name}. ',
                    f'Pod name must have up to {MAX_POD_NAME_LENGTH}.'
                )
            ),
            (
                re.match(REGEX_MATCH, conf.pod_name),
                (
                    f'Invalid format for pod_name {conf.pod_name}. ',
                    f'Must comply with Kubernetes pod naming standards.'
                )
            )
        )
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
            name, = pod_name.rsplit('.', maxsplit=1)
            uuid = ''.join(secrets.choice(BASE_58) for i in range(UUID_BYTES))
        except ValueError:
            raise ValueError(f'Invalid format for pod_name {pod_name}')

        return self.set(name=name, uuid=uuid)
