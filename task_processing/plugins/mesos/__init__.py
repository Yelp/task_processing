from .logging_executor import MesosLoggingExecutor
from .mesos_executor import MesosExecutor
from .retrying_executor import RetryingExecutor
from .timeout_executor import TimeoutExecutor


TASK_PROCESSING_PLUGIN = 'mesos_plugin'


def register_plugin(registry):
    return registry \
        .register_task_executor('logging', MesosLoggingExecutor) \
        .register_task_executor('mesos', MesosExecutor) \
        .register_task_executor('retrying', RetryingExecutor) \
        .register_task_executor('timeout', TimeoutExecutor)
