from task_processing.interfaces import TaskExecutor
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


class KubernetesPodExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = KubernetesTaskConfig

    def run(self, task_config):
        pass

    def reconcile(self, task_config):
        pass

    def kill(self, task_id):
        pass

    def stop(self):
        pass

    def get_event_queue(self):
        pass
