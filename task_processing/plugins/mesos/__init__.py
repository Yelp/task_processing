from .mesos_executor import MesosExecutor
from .retrying_executor import RetryingExecutor


TASK_PROCESSING_PLUGIN = 'mesos_plugin'


def register_plugin(registry):
    return registry \
        .register_task_executor('mesos', MesosExecutor) \
        .register_task_executor('retrying', RetryingExecutor)
