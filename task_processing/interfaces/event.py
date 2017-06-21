import datetime

from pyrsistent import field
from pyrsistent import freeze
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import PRecord


class Event(PRecord):
    timestamp = field(type=datetime.datetime)
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
    if isinstance(o, datetime.datetime):
        return o.strftime('%s')


def json_deserializer(dct):
    for k, v in dct.items():
        if k == 'timestamp':
            try:
                dct[k] = datetime.datetime.fromtimestamp(float(v))
            except:
                pass
        else:
            dct[k] = freeze(v)
    return dct
