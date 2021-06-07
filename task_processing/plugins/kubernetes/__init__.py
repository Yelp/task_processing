from .kubernetes_pod_executor import KubernetesPodExecutor


TASK_PROCESSING_PLUGIN = 'kubernetes_plugin'


def register_plugin(registry):
    return registry \
        .register_task_executor('kubernetes', KubernetesPodExecutor)
