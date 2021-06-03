from task_processing.interfaces import TaskExecutor
from task_processing.task_processor import TaskProcessor


class KubernetesPodExecutor(TaskExecutor):
    # super().load_plugin(provider_module='task_processing.plugins.kubernetes')
    # executor = super().registry.register_task_executor(TaskExecutor)

    tp = TaskProcessor()
    tp.load_plugin(provider_module='task_processing.plugins.kubernetes')
    # Implementing this after TaskProc-229 has been merged
    # super().TASK_CONFIG_INTERFACE = KubernetesTaskConfigInterface

    def run(self, task_config):
        return super().run(task_config)

    def reconcile(self, task_config):
        return super().reconcile(task_config)

    def kill(self, task_id):
        return super().kill(task_id)

    def stop(self):
        return super().stop()

    def get_event_queue(self):
        return super().get_event_queue()
