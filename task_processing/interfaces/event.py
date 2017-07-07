import json
import uuid

from pyrsistent import field
from pyrsistent import freeze
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import PRecord


class Event(PRecord):
    type = field(type=str,
                 invariant=lambda x: x in ['task', 'control'])
    # we store timestamps as seconds since epoch.
    # use time.time() to generate
    timestamp = field(type=float)
    # reference to platform-specific event object
    raw = field()
    # free-form dictionary for stack-specific data
    extensions = field(type=PMap, initial=m())
    # is this the last event for a task?
    terminal = field(type=bool, mandatory=True)

    # task-specific fields
    # task_id this event pertains to
    task_id = field(type=str)
    # task config dict that sourced the task this event refers to
    task_config = field(type=PMap)
    # the task finished with exit code 0
    success = field(type=(bool, type(None)), initial=None)
    # platform-specific event type
    platform_type = field(type=str)

    # control events
    message = field(type=str)


class TaskEvent(Event):
    def __init__(self, **kwargs):
        kwargs.setdefault('type', 'task')
        super(Event, self).__init__(**kwargs)


class ControlEvent(Event):
    def __init__(self, **kwargs):
        kwargs.setdefault('type', 'control')
        super(Event, self).__init__(**kwargs)


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
