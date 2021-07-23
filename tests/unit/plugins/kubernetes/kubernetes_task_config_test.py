import pytest
from pyrsistent import InvariantException
from pyrsistent import pmap

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


def test_kubernetes_task_config_set_pod_name():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )
    result = task_config.set_pod_name(pod_name="mock_pod.mock_uuid")

    assert result.pod_name == "mock--pod.mock--uuid"


def test_kubernetes_task_config_set_pod_name_rejects_long_name():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )

    with pytest.raises(InvariantException):
        task_config.set(name='a' * 254)


def test_kubernetes_task_config_enforces_command_requirmenets():
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command"
    )
    with pytest.raises(InvariantException):
        task_config.set(command="")


@pytest.mark.parametrize(
    "capabilties", (
        ("NOT_A_CAP",),
        ("MKNOD", "NOT_A_CAP",),
    )
)
def test_cap_add_capabilities_rejects_invalid_capabilites(capabilties):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake_task_name",
            uuid="fake_id",
            image="fake_docker_image",
            command="fake_command",
            cap_add=capabilties,
        )


@pytest.mark.parametrize(
    "capabilties", (
        ("CHOWN",),
        ("MKNOD", "CHOWN",),
    )
)
def test_cap_add_capabilities_valid_capabilites(capabilties):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command",
        cap_add=capabilties,
    )
    assert tuple(task_config.cap_add) == capabilties


@pytest.mark.parametrize(
    "capabilties", (
        ("NOT_A_CAP",),
        ("MKNOD", "NOT_A_CAP",),
    )
)
def test_cap_drop_capabilities_rejects_invalid_capabilites(capabilties):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake_task_name",
            uuid="fake_id",
            image="fake_docker_image",
            command="fake_command",
            cap_drop=capabilties,
        )


@pytest.mark.parametrize(
    "capabilties", (
        ("CHOWN",),
        ("MKNOD", "CHOWN",),
    )
)
def test_cap_drop_capabilities_valid_capabilites(capabilties):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command",
        cap_drop=capabilties,
    )
    assert tuple(task_config.cap_drop) == capabilties


@pytest.mark.parametrize(
    "volumes", (
        [{"host_path": "/a"}],
        [{"host_path": "/a", "containerPath": "/b"}],
        [{"host_path": "/a", "containerPath": "/b", "mode": "RO"}],
        [{"host_path": "/a", "container_path": "/b", "mode": "LOL"}],
        [
            {"host_path": "/c", "container_path": "/d", "mode": "RO"},
            {"host_path": "/e", "containerPath": "/f", "mode": "LOL"}
        ],
    )
)
def test_volume_rejects_invalid_specification(volumes):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake_task_name",
            uuid="fake_id",
            image="fake_docker_image",
            command="fake_command",
            volumes=volumes
        )


@pytest.mark.parametrize(
    "volumes", (
        ({"host_path": "/a", "container_path": "/b", "mode": "RO"},),
        (
            {"host_path": "/a", "container_path": "/b", "mode": "RO"},
            {"host_path": "/c", "container_path": "/d", "mode": "RW"}
        ),
    )
)
def test_volume_valid_specification(volumes):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command",
        volumes=volumes
    )

    assert tuple(task_config.volumes) == volumes


@pytest.mark.parametrize(
    "secret_environment", (
        pmap({'SECRET1': {'secret': 'taskprocns-secret-secret1', 'key': 'secret_1'}}),
        pmap({
            'SECRET_A': {'secret': 'taskprocns-secret-secret--a', 'key': 'secreta'},
            'SECRET_B': {'secret': 'taskprocns-secret-secret--b', 'key': 'secretb'},
        }),
    )
)
def test_secret_env_valid_specification(secret_environment):
    task_config = KubernetesTaskConfig(
        name="fake_task_name",
        uuid="fake_id",
        image="fake_docker_image",
        command="fake_command",
        secret_environment=secret_environment
    )

    assert task_config.secret_environment == secret_environment


@pytest.mark.parametrize(
    "secret_environment", (
        pmap({'SECRET1': {
            'secret': 'taskprocns-secret-1', 'key': 'secret-1', 'namespace': 'otherns'
        }}),
        pmap({'SECRET1': {'secret': 'taskprocns-secret-2'}})
    )
)
def test_secret_env_rejects_invalid_specification(secret_environment):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake_task_name",
            uuid="fake_id",
            image="fake_docker_image",
            command="fake_command",
            secret_environment=secret_environment
        )
