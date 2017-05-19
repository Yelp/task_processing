from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import PRecord

from task_processing.interfaces.task_executor import TaskConfig


class Event(PRecord):
    # reference to platform-specific event object
    raw = field()
    # is this the last event for a task?
    terminal = field(type=bool, mandatory=True)
    success = field(type=(bool, type(None)), initial=None)
    # platform-specific event name
    platform_type = field(type=str)
    # task_id this event pertains to
    task_id = field(type=str)
    task_config = field(type=TaskConfig)
    middleware_data = field(type=PMap, initial=m())
