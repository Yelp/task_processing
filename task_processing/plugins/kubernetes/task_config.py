import uuid
from typing import Sequence
from typing import TYPE_CHECKING

from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface

VOLUME_KEYS = set(['mode', 'container_path', 'host_path'])

def valid_volumes(volumes):
    for vol in volumes:
        if set(vol.keys()) != VOLUME_KEYS:
            return (
                False,
                'Invalid volume format, must only contain following keys: '
                '{}, was: {}'.format(VOLUME_KEYS, vol.keys())
            )
    return (True, None)

class KubernetesTaskConfig(DefaultTaskConfigInterface):
    uuid = field(type=(str, uuid.UUID), initial=uuid.uuid4)
    node_name = field(type=str)
    node_selector = field(type=(str,str))
    containers = field(type=PVector)
    restart_policy = field(type=str)
    # ToDO IAM stuff
    service_account_name = field(type=str)
    # By default, the retrying executor retries 3 times. This task option
    # overrides the executor setting.
    retries = field(type=int,
                    factory=int,
                    mandatory=False,
                    invariant=lambda r: (r >= 0, 'retries >= 0'))
    volumes = field(type=PVector,)

    @property
    def task_id(self):
        return "{}.{}".format(self.name, self.uuid)

    def set_task_id(self, task_id):
        try:
            name, uuid = task_id.rsplit('.', maxsplit=1)
        except ValueError:
            raise ValueError(f'Invalid format for task_id {task_id}')
        return self.set(name=name, uuid=uuid)