import uuid

from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import PVector
from pyrsistent import pvector
from pyrsistent import v

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


class MesosTaskConfig(PRecord):
    def __invariant__(conf):
        return ('image' in conf if conf.containerizer == 'DOCKER' else True,
                'Image required for chosen containerizer')

    uuid = field(type=(str, uuid.UUID), initial=uuid.uuid4)
    name = field(type=str, initial="default")
    # image is optional for the mesos containerizer
    image = field(type=str)
    cmd = field(type=str,
                mandatory=True,
                invariant=lambda cmd: (cmd.strip() != '', 'empty cmd'))
    cpus = field(type=float,
                 initial=0.1,
                 factory=float,
                 invariant=lambda c: (c > 0, 'cpus > 0'))
    mem = field(type=float,
                initial=32.0,
                factory=float,
                invariant=lambda m: (m >= 32, 'mem is >= 32'))
    disk = field(type=float,
                 initial=10.0,
                 factory=float,
                 invariant=lambda d: (d > 0, 'disk > 0'))
    gpus = field(type=int,
                 initial=0,
                 factory=int,
                 invariant=lambda g: (g >= 0, 'gpus >= 0'))
    timeout = field(type=float,
                    factory=float,
                    mandatory=False,
                    invariant=lambda t: (t > 0, 'timeout > 0'))
    volumes = field(type=PVector,
                    initial=v(),
                    factory=pvector,
                    invariant=valid_volumes)
    ports = field(type=PVector, initial=v(), factory=pvector)
    cap_add = field(type=PVector, initial=v(), factory=pvector)
    ulimit = field(type=PVector, initial=v(), factory=pvector)
    uris = field(type=PVector, initial=v(), factory=pvector)
    # TODO: containerization + containerization_args ?
    docker_parameters = field(type=PVector, initial=v(), factory=pvector)
    containerizer = field(type=str,
                          initial='DOCKER',
                          invariant=lambda c:
                          (c == 'DOCKER' or c == 'MESOS',
                           'containerizer is docker or mesos'))
    environment = field(type=PMap, initial=m(), factory=pmap)
    offer_timeout = field(
        type=float,
        factory=float,
        mandatory=False,
        invariant=lambda t: (t > 0, 'timeout > 0')
    )

    @property
    def task_id(self):
        return "{}.{}".format(self.name, self.uuid)