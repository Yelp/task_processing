import json
import uuid

from pyrsistent import field
from pyrsistent import freeze
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import PRecord


class Event(PRecord):
    # we store timestamps as seconds since epoch.
    # use time.time() to generate
    timestamp = field(type=float)
    # reference to platform-specific event object
    raw = field()
    # is this the last event for a task?
    terminal = field(type=bool, mandatory=True)
    success = field(type=(bool, type(None)), initial=None)
    # platform-specific event name
    platform_type = field(type=str)
    # task_id this event pertains to
    task_id = field(type=str)
    task_config = field()
    extensions = field(type=PMap, initial=m())


def json_serializer(o):
    if isinstance(o, uuid.UUID):
        return o.hex
    return json.JSONEncoder.default(o)


def json_deserializer(dct):
    for k, v in dct.items():
        if k == "uuid":
            try:
                dct[k] = uuid.UUID(hex=v)
            except ValueError:
                dct[k] = freeze(v)
        else:
            dct[k] = freeze(v)
    return dct
