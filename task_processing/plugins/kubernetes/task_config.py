import re

from pyrsistent import field
from pyrsistent import PMap
from pyrsistent import PVector

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface


class KubernetesTaskConfig(DefaultTaskConfigInterface):
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
    volumes = field(type=PVector,)

    @property
    def pod_name(self) -> str:
        return self.name

    def set_pod_name(self, pod_name: str):
        try:
            name, uuid = pod_name.rsplit('.', maxsplit=1)
        except ValueError:
            raise ValueError(f'Invalid format for pod_name {pod_name}')

        if len(pod_name) > 253:
            print(
                f'Invalid format for pod_name {pod_name}.',
                f'Pod_name must have up to 253 characters only.')
            return False

        regex_passed = re.match('[a-z0-9]([.-a-z0-9]*[a-z0-9])?', pod_name)
        if not regex_passed:
            print(f'Invalid format for pod_name {pod_name}.')

        return self.set(name=name, uuid=uuid)
