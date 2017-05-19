from .mesos_executor import MesosExecutor


TASK_PROCESSING_PLUGIN = 'mesos_plugin'


def register_plugin(registry):
    return registry.register_task_executor('mesos', MesosExecutor)
