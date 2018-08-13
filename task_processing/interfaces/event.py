import json
import uuid

from pyrsistent import field
from pyrsistent import freeze
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord

EVENT_KINDS = {'task', 'control'}
EVENT_TASK_ATTRS = {'task_id', 'task_config'}


class Event(PRecord):
    def __invariant__(self):
        if self.kind != 'task':
            return True, 'Not a task event'

        missing_attrs = EVENT_TASK_ATTRS - set(self.keys())
        if missing_attrs:
            return False, 'Missing task attributes: {}'.format(missing_attrs)

        return True, 'Task is valid'

    kind = field(type=str,
                 mandatory=True,
                 invariant=lambda x: (x in EVENT_KINDS,
                                      'kind not in {}'.format(EVENT_KINDS)))
    # we store timestamps as seconds since epoch.
    # use time.time() to generate
    timestamp = field(type=float, initial=0.0)
    # reference to platform-specific event object
    raw = field(mandatory=True, initial=None)
    # free-form dictionary for stack-specific data
    extensions = field(type=PMap, initial=m(), factory=pmap)
    # is this the last event for a task?
    terminal = field(type=bool, initial=False)

    # task-specific fields
    # task_id this event pertains to
    task_id = field(type=(str, type(None)), initial=None)
    # task config dict that sourced the task this event refers to
    task_config = field(
        invariant=lambda x: (isinstance(x, PMap),
                             'task_config must inherit from PMap'),
        factory=lambda x: pmap(x) if not isinstance(x, PMap) else x,
        initial=m(),
    )
    # the task finished with exit code 0
    success = field(type=(bool, type(None)), initial=None)
    # platform-specific event type
    platform_type = field(type=(str, type(None)), initial=None)

    # control events
    message = field(type=(str, type(None)), initial=None)


def task_event(**kwargs):
    kwargs.setdefault('kind', 'task')
    return Event(**kwargs)


def control_event(**kwargs):
    kwargs.setdefault('kind', 'control')
    return Event(**kwargs)


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
