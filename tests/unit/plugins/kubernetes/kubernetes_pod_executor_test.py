from unittest import mock

import pytest
from pyrsistent import pmap
from pyrsistent import v

from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesPodExecutor
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskState
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


@pytest.fixture
def k8s_executor():
    executor = KubernetesPodExecutor()
    yield executor
    executor.stop()


def test_run_updates_task_metadata(k8s_executor):
    task_config = KubernetesTaskConfig(name="name", uuid="uuid")
    k8s_executor.run(task_config=task_config)

    assert k8s_executor.task_metadata == pmap(
        {
            task_config.pod_name: KubernetesTaskMetadata(
                task_state_history=v((KubernetesTaskState.TASK_PENDING, mock.ANY)),
                task_config=task_config,
                node_name='',
                task_state=KubernetesTaskState.TASK_PENDING,
            ),
        },
    )
