from .stateful_executor import StatefulTaskExecutor


TASK_PROCESSING_PLUGIN = 'stateful_plugin'


def register_plugin(registry):
    return registry.register_task_executor('stateful', StatefulTaskExecutor)
