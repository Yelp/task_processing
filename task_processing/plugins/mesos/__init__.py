from .mesos_executor import MesosExecutor


def register_plugin(registry):
    registry.register_task_executor('mesos', MesosExecutor)
    return 'mesos'
