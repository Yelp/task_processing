import os
from unittest import mock

import pytest
from kubernetes.client import ApiException
from kubernetes.client import V1Capabilities
from kubernetes.client import V1Container
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1ObjectMeta
from kubernetes.client import V1Pod
from kubernetes.client import V1PodSpec
from kubernetes.client import V1ResourceRequirements
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
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


@pytest.fixture
def mock_task_configs():
    test_task_names = ['job1.action1', 'job1.action2', 'job2.action1', 'job3.action2']
    task_configs = []
    for task in test_task_names:
        taskconf = KubernetesTaskConfig(
            name=task,
            uuid='fake_id',
            image='fake_docker_image',
            command='fake_command',
        )
        task_configs.append(taskconf)

    yield task_configs


@pytest.fixture
def k8s_executor_with_tasks(mock_Thread, mock_task_configs):
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ), mock.patch.dict(os.environ, {"KUBECONFIG": "/this/doesnt/exist.conf"}):
        executor = KubernetesPodExecutor(
            namespace="task_processing_tests",
            task_configs=mock_task_configs,
        )
        yield executor, [md.task_config for md in executor.task_metadata.values()]
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
        command="fake_command",
        cpus=1,
        memory=1024,
        disk=1024,
        volumes=[{"host_path": "/a", "container_path": "/b", "mode": "RO"}]
    )
    expected_container = V1Container(
        image=task_config.image,
        name="main",
        command=["/bin/sh", "-c"],
        args=[task_config.command],
        security_context=V1SecurityContext(
            capabilities=V1Capabilities(drop=list(task_config.cap_drop)),
        ),
        resources=V1ResourceRequirements(
            limits={
                "cpu": 1.0,
                "memory": "1024.0Mi",
                "ephemeral-storage": "1024.0Mi",
            }
        ),
        env=[],
        volume_mounts=[V1VolumeMount(
            mount_path="/b",
            name="host--slash-a",
            read_only=True,
        )],
    )
    expected_pod = V1Pod(
        metadata=V1ObjectMeta(
            name=task_config.pod_name,
            namespace="task_processing_tests"
        ),
        spec=V1PodSpec(
            restart_policy=task_config.restart_policy,
            containers=[expected_container],
            volumes=[V1Volume(
                host_path=V1HostPathVolumeSource(path="/a"),
                name="host--slash-a",
            )],
        ),
    )

    assert k8s_executor.run(task_config) == task_config.pod_name
    assert k8s_executor.kube_client.core.create_namespaced_pod.call_args_list == [
        mock.call(body=expected_pod, namespace='task_processing_tests')
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


def test_process_event_enqueues_task_processing_events_pending_to_running(k8s_executor):
    mock_pod = mock.Mock(spec=V1Pod)
    mock_pod.metadata.name = "test.1234"
    mock_pod.status.phase = "Running"
    mock_pod.spec.node_name = "node-1-2-3-4"
    mock_event = PodEvent(
        type="MODIFIED",
        object=mock_pod,
        raw_object=mock.Mock(),
    )
    k8s_executor.task_metadata = pmap({
        mock_pod.metadata.name: KubernetesTaskMetadata(
            task_config=mock.Mock(spec=KubernetesTaskConfig),
            task_state=KubernetesTaskState.TASK_PENDING,
            task_state_history=v(),
        )
    })

    k8s_executor._process_pod_event(mock_event)

    assert k8s_executor.event_queue.qsize() == 1
    # in normal usage this would actually have 2 items, but we're obiviating the inital PENDING
    # state for this test
    assert len(k8s_executor.task_metadata[mock_pod.metadata.name].task_state_history) == 1


@pytest.mark.parametrize(
    "phase", (
        "Succeeded",
        "Failed",
    )
)
def test_process_event_enqueues_task_processing_events_running_to_terminal(k8s_executor, phase):
    mock_pod = mock.Mock(spec=V1Pod)
    mock_pod.metadata.name = "test.1234"
    mock_pod.status.phase = phase
    mock_pod.spec.node_name = "node-1-2-3-4"
    mock_event = PodEvent(
        type="MODIFIED",
        object=mock_pod,
        raw_object=mock.Mock(),
    )
    k8s_executor.task_metadata = pmap({
        mock_pod.metadata.name: KubernetesTaskMetadata(
            task_config=mock.Mock(spec=KubernetesTaskConfig),
            task_state=KubernetesTaskState.TASK_RUNNING,
            task_state_history=v(),
        )
    })

    k8s_executor._process_pod_event(mock_event)

    assert k8s_executor.event_queue.qsize() == 1
    assert len(k8s_executor.task_metadata) == 0


@pytest.mark.parametrize(
    "phase,task_state", (
        ("Succeeded", KubernetesTaskState.TASK_FINISHED),
        ("Failed", KubernetesTaskState.TASK_FAILED),
        ("Running", KubernetesTaskState.TASK_RUNNING),
        ("Pending", KubernetesTaskState.TASK_PENDING),
    )
)
def test_process_event_enqueues_task_processing_events_no_state_transition(
    k8s_executor,
    phase,
    task_state,
):
    mock_pod = mock.Mock(spec=V1Pod)
    mock_pod.metadata.name = "test.1234"
    mock_pod.status.phase = phase
    mock_pod.status.host_ip = "1.2.3.4"
    mock_pod.spec.node_name = 'kubenode'
    mock_event = PodEvent(
        type="MODIFIED",
        object=mock_pod,
        raw_object=mock.Mock(),
    )
    k8s_executor.task_metadata = pmap({
        mock_pod.metadata.name: KubernetesTaskMetadata(
            task_config=mock.Mock(spec=KubernetesTaskConfig),
            task_state=task_state,
            task_state_history=v(),
        )
    })

    k8s_executor._process_pod_event(mock_event)

    assert k8s_executor.event_queue.qsize() == 0
    assert len(k8s_executor.task_metadata) == 1
    assert k8s_executor.task_metadata[mock_pod.metadata.name].task_state == task_state
    # in reality, this would have some entries, but we're not filling out task_state_history
    # for tests, so checking that the size is 0 is the same as checking that we didn't transition
    # to a new state
    assert len(k8s_executor.task_metadata[mock_pod.metadata.name].task_state_history) == 0


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


def test_process_event_enqueues_task_processing_events_deleted(
    k8s_executor,
):
    mock_pod = mock.Mock(spec=V1Pod)
    mock_pod.metadata.name = "test.1234"
    mock_pod.status.phase = "Running"
    mock_pod.status.host_ip = "1.2.3.4"
    mock_pod.spec.node_name = 'kubenode'
    mock_event = PodEvent(
        type="DELETED",
        object=mock_pod,
        raw_object=mock.Mock(),
    )
    k8s_executor.task_metadata = pmap({
        mock_pod.metadata.name: KubernetesTaskMetadata(
            task_config=mock.Mock(spec=KubernetesTaskConfig),
            task_state=KubernetesTaskState.TASK_RUNNING,
            task_state_history=v(),
        )
    })

    k8s_executor._process_pod_event(mock_event)

    assert k8s_executor.event_queue.qsize() == 1
    assert len(k8s_executor.task_metadata) == 0


def test_initial_task_metadata(
    k8s_executor_with_tasks
):
    executor, task_configs = k8s_executor_with_tasks
    assert all([tc.pod_name in executor.task_metadata for tc in task_configs])
    assert all(
        [
            tm.task_state == KubernetesTaskState.TASK_UNKNOWN
            for tm in executor.task_metadata.values()
        ]
    )


def test_reconcile_missing_pod(
    k8s_executor,
):
    task_config = mock.Mock(spec=KubernetesTaskConfig)
    task_config.pod_name = 'pod--name.uuid'
    task_config.name = 'job-name'

    k8s_executor.task_metadata = pmap({
        task_config.pod_name: KubernetesTaskMetadata(
            task_config=mock.Mock(spec=KubernetesTaskConfig),
            task_state=KubernetesTaskState.TASK_UNKNOWN,
            task_state_history=v(),
        )
    })

    with mock.patch.object(k8s_executor, "kube_client", autospec=True) as mock_kube_client:
        mock_kube_client.get_pod.return_value = None
        k8s_executor.reconcile(task_config)

    assert k8s_executor.event_queue.qsize() == 1
    assert len(k8s_executor.task_metadata) == 1
    tm = k8s_executor.task_metadata['pod--name.uuid']
    assert tm.task_state == KubernetesTaskState.TASK_LOST


def test_reconcile_existing_pods(
    k8s_executor, mock_task_configs
):

    mock_pods = []
    test_phases = ['Running', 'Succeeded', 'Failed', 'Unknown']
    for i in range(len(mock_task_configs)):
        taskconf = mock_task_configs[i]
        phase = test_phases[i]
        mock_pod = mock.Mock(spec=V1Pod)
        mock_pod.metadata.name = taskconf.pod_name
        mock_pod.status.phase = phase
        mock_pod.status.host_ip = '1.2.3.4'
        mock_pod.spec.node_name = 'kubenode'
        mock_pods.append(mock_pod)

    with mock.patch.object(
        k8s_executor,
        "kube_client",
        autospec=True
    ) as mock_kube_client:
        mock_kube_client.get_pod.side_effect = mock_pods
        for taskconf in mock_task_configs:
            k8s_executor.reconcile(taskconf)

    assert k8s_executor.event_queue.qsize() == 4
    assert len(k8s_executor.task_metadata) == 2

    running_pod_metadata = k8s_executor.task_metadata[mock_pods[0].metadata.name]
    assert running_pod_metadata.task_state == KubernetesTaskState.TASK_RUNNING
