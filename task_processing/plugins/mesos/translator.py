import time

from task_processing.interfaces.event import task_event

# https://github.com/apache/mesos/blob/master/include/mesos/mesos.proto

MESOS_STATUS_MAP = {
    'TASK_STARTING': task_event(platform_type='starting', terminal=False),
    'TASK_RUNNING': task_event(platform_type='running', terminal=False),
    'TASK_FINISHED': task_event(platform_type='finished',
                                terminal=True,
                                success=True),
    'TASK_FAILED': task_event(platform_type='failed',
                              terminal=True,
                              success=False),
    'TASK_KILLED': task_event(platform_type='killed',
                              terminal=True,
                              success=False),
    'TASK_LOST': task_event(platform_type='lost',
                            terminal=True,
                            success=False),
    'TASK_STAGING': task_event(platform_type='staging', terminal=False),
    'TASK_ERROR': task_event(platform_type='error',
                             terminal=True,
                             success=False),
    'TASK_KILLING': task_event(platform_type='killing', terminal=False),
    'TASK_DROPPED': task_event(platform_type='dropped',
                               terminal=True,
                               success=False),
    'TASK_UNREACHABLE': task_event(platform_type='unreachable',
                                   terminal=False),
    'TASK_GONE': task_event(platform_type='gone',
                            terminal=True,
                            success=False),
    'TASK_GONE_BY_OPERATOR': task_event(platform_type='gone_by_operator',
                                        terminal=True,
                                        success=False),
    'TASK_UNKNOWN': task_event(platform_type='unknown', terminal=False)
}


def mesos_status_to_event(mesos_status, task_id):
    return MESOS_STATUS_MAP[mesos_status.state].set(
        raw=mesos_status,
        task_id=str(task_id),
        timestamp=time.time(),
    )
