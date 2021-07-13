import os
from unittest import mock

import pytest
from kubernetes.client import ApiException
from kubernetes.client import V1Container
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from pyrsistent import pmap
from pyrsistent import v

from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesPodExecutor
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskState
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.plugins.kubernetes.types import PodEvent


@pytest.fixture
def k8s_executor(mock_Thread):
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
    task_config = KubernetesTaskConfig(
        name="name",
        uuid="uuid",
        image="fake_image",
        command="fake_command"
    )
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


def test_run(k8s_executor):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )
    fake_container = V1Container(
        image=task_config.image,
        name=task_config.name,
        command=["/bin/sh", "-c"],
        args=[task_config.command],
    )
    fake_pod = V1Pod(
        metadata=V1ObjectMeta(
            name=task_config.pod_name,
            namespace="task_processing_tests"
        ),
        spec=V1PodSpec(
            restart_policy=task_config.restart_policy,
            containers=[fake_container]
        ),
    )

    assert k8s_executor.run(task_config) == task_config.pod_name
    assert k8s_executor.kube_client.core.create_namespaced_pod.call_args_list == [
        mock.call(body=fake_pod, namespace='task_processing_tests')
    ]


def test_run_failed_exception(k8s_executor):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )
    k8s_executor.kube_client.core.create_namespaced_pod.side_effect = ApiException(
        status=403, reason="Fake unauthorized message")
    assert k8s_executor.run(task_config) is None


@pytest.mark.xfail(reason="_process_pod_event is still a stub function")
def test_process_event_enqueues_task_processing_events(k8s_executor):
    event = PodEvent(
        type="ADDED",
        object=mock.Mock(),
        raw_object=mock.Mock(),
    )

    k8s_executor._process_pod_event(event)

    assert k8s_executor.event_queue.qsize() == 1


def test_pending_event_processing_loop_processes_remaining_events_after_stop(k8s_executor):
    k8s_executor.pending_events.put(
        PodEvent(
            type="ADDED",
            object=mock.Mock(),
            raw_object=mock.Mock(),
        )
    )
    k8s_executor.stopping = True

    with mock.patch.object(
        k8s_executor,
        "_process_pod_event",
        autospec=True,
    ) as mock_process_event:
        k8s_executor._pending_event_processing_loop()

    mock_process_event.assert_called_once()
    assert k8s_executor.pending_events.qsize() == 0
