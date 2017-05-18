from task_processing.interfaces import TaskExecutor


class DummyTaskExecutor(TaskExecutor):
    def __init__(self, arg):
        self.arg = arg

    def run(self, task_config):
        pass

    def kill(self, task_id):
        pass


def register_plugin(registry):
    registry.register_task_executor('dummy', DummyTaskExecutor)
    return 'mock_plugin'
