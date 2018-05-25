import time

from task_processing.interfaces.event import task_event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto

MESOS_STATUS_MAP = {
    'TASK_STARTING':
    dict(platform_type='starting', terminal=False),
    'TASK_RUNNING':
    dict(platform_type='running', terminal=False),
    'TASK_FINISHED':
    dict(platform_type='finished', terminal=True, success=True),
    'TASK_FAILED':
    dict(platform_type='failed', terminal=True, success=False),
    'TASK_KILLED':
    dict(platform_type='killed', terminal=True, success=False),
    'TASK_LOST':
    dict(platform_type='lost', terminal=True, success=False),
    'TASK_STAGING':
    dict(platform_type='staging', terminal=False),
    'TASK_ERROR':
    dict(platform_type='error', terminal=True, success=False),
    'TASK_KILLING':
    dict(platform_type='killing', terminal=False),
    'TASK_DROPPED':
    dict(platform_type='dropped', terminal=True, success=False),
    'TASK_UNREACHABLE':
    dict(platform_type='unreachable', terminal=False),
    'TASK_GONE':
    dict(platform_type='gone', terminal=True, success=False),
    'TASK_GONE_BY_OPERATOR':
    dict(platform_type='gone_by_operator', terminal=True, success=False),
    'TASK_UNKNOWN':
    dict(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status, task_id, **kwargs):
    kwargs2 = dict(
        raw=mesos_status,
        task_id=str(task_id),
        timestamp=time.time(),
    )
    kwargs2.update(MESOS_STATUS_MAP[mesos_status.state])
    kwargs2.update(kwargs)
    return task_event(**kwargs2)
