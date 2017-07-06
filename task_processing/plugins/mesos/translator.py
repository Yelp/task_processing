import time

from task_processing.interfaces.event import Event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto
MESOS_TASK_STATUS_TO_EVENT = {
    'TASK_STARTING': Event(platform_type='starting', terminal=False),
    'TASK_RUNNING': Event(platform_type='running', terminal=False),
    'TASK_FINISHED': Event(platform_type='finished',
                           terminal=True,
                           success=True),
    'TASK_FAILED': Event(platform_type='failed', terminal=True),
    'TASK_KILLED': Event(platform_type='killed',
                         terminal=True),
    'TASK_LOST': Event(platform_type='lost',
                       terminal=True),
    'TASK_STAGING': Event(platform_type='staging', terminal=False),
    'TASK_ERROR': Event(platform_type='error', terminal=True),
    'TASK_KILLING': Event(platform_type='killing', terminal=False),
    'TASK_DROPPED': Event(platform_type='dropped',
                          terminal=True),
    'TASK_UNREACHABLE': Event(platform_type='unreachable', terminal=False),
    'TASK_GONE': Event(platform_type='gone',
                       terminal=True),
    'TASK_GONE_BY_OPERATOR': Event(platform_type='gone_by_operator',
                                   terminal=True),
    'TASK_UNKNOWN': Event(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status, task_id):
    return MESOS_TASK_STATUS_TO_EVENT[mesos_status.state].set(
        raw=mesos_status,
        task_id=str(task_id),
        timestamp=time.time(),
    )
