import os
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
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ), mock.patch.dict(os.environ, {"KUBECONFIG": "/this/doesnt/exist.conf"}):
        executor = KubernetesPodExecutor(namespace="task_processing_tests")
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


def test_kill_removes_task_metadata(k8s_executor):
    task_config = KubernetesTaskConfig(name="name", uuid="uuid")
    k8s_executor.task_metadata = pmap(
        {
            task_config.pod_name: KubernetesTaskMetadata(
                task_state_history=v((KubernetesTaskState.TASK_PENDING, 0)),
                task_config=task_config,
                node_name='',
                task_state=KubernetesTaskState.TASK_PENDING,
            ),
        },
    )

    k8s_executor.kill(task_id=task_config.pod_name)

    assert k8s_executor.task_metadata == pmap({})
