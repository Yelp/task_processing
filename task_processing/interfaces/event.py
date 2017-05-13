from __future__ import absolute_import
from __future__ import unicode_literals

from pyrsistent import PRecord
from pyrsistent import field

# TODO: organize and explain these

class Event(PRecord):
    # reference to platform-specific event object
    raw = field()
    # is this the last event for a task?
    terminal = field(type=bool)
    # platform-specific event name
    platform_type = field(type=str)

