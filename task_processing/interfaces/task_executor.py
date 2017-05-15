import abc
import uuid
from pyrsistent import PRecord, field, v

import six

class TaskConfig(PRecord):
    uuid = field(type=uuid.UUID)
    name = field(type=str, initial="default")
    image = field(type=str, initial="ubuntu:xenial")
    cmd = field(type=str, initial="/bin/true")
    cpus = field(type=float, initial=0.1, invariant=lambda c: c > 0)
    mem = field(type=float, initial=32, invariant=lambda m: m >= 32)
    disk = field(type=float, initial=10, invariant=lambda d: d > 0)
    volumes = field(type=pvector, initial=v(), factory=pvector)
    ports = field(type=pvector, initial=v(), factory=pvector)
    cap_add = field(type=pvector, initial=v(), factory=pvector)
    ulimit = field(type=pvector, initialv=(), factory=pvector)
    docker_parameters = field(type=pvector, initial=v(), factory=pvector)

    def task_id():
        return "{}.{}".format(self.name, str(self.uuid))


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
