import os
from unittest import mock

import pytest
from pyrsistent import pmap
from pyrsistent import v

from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesPodExecutor
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskMetadata
from task_processing.plugins.kubernetes.kubernetes_pod_executor import KubernetesTaskState
from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig
from task_processing.plugins.kubernetes.types import PodEvent
from task_processing.plugins.kubernetes.types import Sentinel


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


def test_pending_event_processing_loop_exits_on_poison_pill(k8s_executor):
    k8s_executor.pending_events.put(Sentinel.POISON_PILL)
    k8s_executor._pending_event_processing_loop()
    assert k8s_executor.event_queue.qsize() == 0
    assert k8s_executor.pending_events.qsize() == 0


def test_pending_event_processing_loop_processes_events_in_order(k8s_executor):
    k8s_executor.pending_events.put(
        PodEvent(
            type="ADDED",
            object=mock.Mock(),
            raw_object=mock.Mock(),
        )
    )

    k8s_executor.pending_events.put(Sentinel.POISON_PILL)
    with mock.patch.object(
        k8s_executor,
        "_process_pod_event",
        autospec=True,
    ) as mock_process_event:
        k8s_executor._pending_event_processing_loop()

    mock_process_event.assert_called_once()
    assert k8s_executor.pending_events.qsize() == 0


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
    k8s_executor.pending_events.put(Sentinel.POISON_PILL)

    with mock.patch.object(
        k8s_executor,
        "_process_pod_event",
        autospec=True,
    ) as mock_process_event:
        k8s_executor._pending_event_processing_loop()

    mock_process_event.assert_called_once()
    assert k8s_executor.pending_events.qsize() == 0
