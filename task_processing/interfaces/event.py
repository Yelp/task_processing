from pyrsistent import field
from pyrsistent import PRecord

# TODO: organize and explain these


class Event(PRecord):
    # reference to platform-specific event object
    raw = field()
    # is this the last event for a task?
    terminal = field(type=bool, mandatory=True)
    # platform-specific event name
    platform_type = field(type=str)
    # task_id this event pertains to
    task_id = field(type=str)
