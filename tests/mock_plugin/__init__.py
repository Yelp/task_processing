from task_processing.interfaces import TaskExecutor


class DummyTaskExecutor(TaskExecutor):
    def __init__(self, arg):
        self.arg = arg

    def run(self, task_config):
        pass

    def reconcile(self, task_config):
        pass

    def kill(self, task_id):
        pass

    def stop(self):
        pass

    def get_event_queue(self, task_id):
        pass


class DummyTaskExecutor2(TaskExecutor):
    def run(self, task_config):
        pass

    def reconcile(self, task_config):
        pass

    def kill(self, task_id):
        pass

    def stop(self):
        pass

    def get_event_queue(self, task_id):
        pass


TASK_PROCESSING_PLUGIN = 'mock_plugin'


def register_plugin(registry):
    registration = registry.register_task_executor(
        'dummy', DummyTaskExecutor
    ).register_task_executor(
        'dummy2', DummyTaskExecutor2
    )

    return registration
