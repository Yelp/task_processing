from task_processing.plugins.mesos.mesos_executor import MesosExecutor


class MesosPodExecutor(MesosExecutor):
    def __init__(self, role, *args, **kwargs) -> None:
        raise NotImplementedError
