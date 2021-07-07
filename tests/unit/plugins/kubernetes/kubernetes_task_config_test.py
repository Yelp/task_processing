import pytest
from pyrsistent import InvariantException

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


def test_kubernetes_task_config_set_pod_name():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )
    result = task_config.set_pod_name(pod_name="mock_pod.mock_uuid")

    assert result.pod_name == "mock_pod.mock_uuid"


def test_kubernetes_task_config_set_pod_name_rejects_long_name():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )

    with pytest.raises(InvariantException):
        task_config.set(name='a' * 254)


def test_kubernetes_task_config_enforces_kubernetes_name_requirements():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )

    with pytest.raises(InvariantException):
        task_config.set(name=f"INVALID{task_config.name}")
