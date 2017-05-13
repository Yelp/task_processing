from __future__ import absolute_import
from __future__ import unicode_literals

from task_processing.interfaces.event import Event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto
MESOS_TASK_STATUS_TO_EVENT = {
    0: Event(platform_type='starting'),
    1: Event(platform_type='running'),
    2: Event(platform_type='finished'),
    3: Event(platform_type='failed'),
    4: Event(platform_type='killed',
             terminal=True),
    5: Event(platform_type='lost',
             terminal=True),
    6: Event(platform_type='staging'),
    7: Event(platform_type='error'),
    8: Event(platform_type='killing'),
    9: Event(platform_type='dropped',
             terminal=True),
    10: Event(platform_type='unreachable'),
    11: Event(platform_type='gone',
              terminal=True),
    12: Event(platform_type='gone_by_operator',
              terminal=True),
    13: Event(platform_type='unknown')
}


def mesos_status_to_event(mesos_status):
    return MESOS_TASK_STATUS_TO_EVENT[mesos_status.state].set(raw=mesos_status)
