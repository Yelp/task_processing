from .logging_executor import MesosLoggingExecutor
from .mesos_pod_executor import MesosPodExecutor
from .mesos_task_executor import MesosTaskExecutor
from .retrying_executor import RetryingExecutor
from .timeout_executor import TimeoutExecutor


TASK_PROCESSING_PLUGIN = 'mesos_plugin'


def register_plugin(registry):
    return registry \
        .register_task_executor('logging', MesosLoggingExecutor) \
        .register_deprecated_task_executor('mesos', MesosTaskExecutor) \
        .register_task_executor('mesos_task', MesosTaskExecutor) \
        .register_task_executor('mesos_pod', MesosPodExecutor) \
        .register_task_executor('retrying', RetryingExecutor) \
        .register_task_executor('timeout', TimeoutExecutor)
