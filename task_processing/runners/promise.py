from task_processing.interfaces.runner import Runner
from task_processing.runners.sync import Sync


class Promise(Runner):
    def __init__(self, executor, futures_executor):
        self.TASK_CONFIG_INTERFACE = executor.TASK_CONFIG_INTERFACE
        self._futures_executor = futures_executor
        self._runner = Sync(executor)

    def run(self, task_config):
        """Schedules execution of the supplied task

        :param task_config: An object satisfying the TASK_CONFIG_INTERFACE

        :return: A Future object representing the execution of the task.
        """
        return self._futures_executor.submit(self._runner.run, task_config)

    def kill(self, task_id):
        return self._runner.kill(task_id)

    def reconcile(self, task_config):
        self._runner.reconcile(task_config)

    def stop(self):
        self._runner.stop()
