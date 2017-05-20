from task_processing.interfaces.event import Event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto
MESOS_TASK_STATUS_TO_EVENT = {
    0: Event(platform_type='starting', terminal=False),
    1: Event(platform_type='running', terminal=False),
    2: Event(platform_type='finished', terminal=True),
    3: Event(platform_type='failed', terminal=True),
    4: Event(platform_type='killed',
             terminal=True),
    5: Event(platform_type='lost',
             terminal=True),
    6: Event(platform_type='staging', terminal=False),
    7: Event(platform_type='error', terminal=True),
    8: Event(platform_type='killing', terminal=False),
    9: Event(platform_type='dropped',
             terminal=True),
    10: Event(platform_type='unreachable', terminal=False),
    11: Event(platform_type='gone',
              terminal=True),
    12: Event(platform_type='gone_by_operator',
              terminal=True),
    13: Event(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status):
    return MESOS_TASK_STATUS_TO_EVENT[mesos_status.state].set(raw=mesos_status)
