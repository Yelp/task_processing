import time

from task_processing.interfaces.event import TaskEvent

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto

MESOS_STATUS_MAP = {
    'TASK_STARTING': TaskEvent(platform_type='starting', terminal=False),
    'TASK_RUNNING': TaskEvent(platform_type='running', terminal=False),
    'TASK_FINISHED': TaskEvent(platform_type='finished',
                               terminal=True,
                               success=True),
    'TASK_FAILED': TaskEvent(platform_type='failed', terminal=True),
    'TASK_KILLED': TaskEvent(platform_type='killed', terminal=True),
    'TASK_LOST': TaskEvent(platform_type='lost', terminal=True),
    'TASK_STAGING': TaskEvent(platform_type='staging', terminal=False),
    'TASK_ERROR': TaskEvent(platform_type='error', terminal=True),
    'TASK_KILLING': TaskEvent(platform_type='killing', terminal=False),
    'TASK_DROPPED': TaskEvent(platform_type='dropped', terminal=True),
    'TASK_UNREACHABLE': TaskEvent(platform_type='unreachable', terminal=False),
    'TASK_GONE': TaskEvent(platform_type='gone', terminal=True),
    'TASK_GONE_BY_OPERATOR': TaskEvent(platform_type='gone_by_operator',
                                       terminal=True),
    'TASK_UNKNOWN': TaskEvent(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status, task_id):
    return MESOS_STATUS_MAP[mesos_status.state].set(
        raw=mesos_status,
        task_id=str(task_id),
        timestamp=time.time(),
    )
