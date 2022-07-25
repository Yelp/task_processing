import pytest
from pyrsistent import InvariantException
from pyrsistent import pmap

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


def test_kubernetes_task_config_set_pod_name():
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command"
    )
    result = task_config.set_pod_name(pod_name="mock_pod.mock_uuid")

    assert result.pod_name == "mock--pod.mock--uuid"


def test_kubernetes_task_config_set_pod_name_truncates_long_name():
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command"
    )

    task_config = task_config.set(name="a" * 1000)
    assert task_config.name == "a" * 1000
    assert task_config.pod_name != "a" * 1000


def test_kubernetes_task_config_enforces_command_requirmenets():
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
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
            name="fake--task--name",
            uuid="fake--id",
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
        name="fake--task--name",
        uuid="fake--id",
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
            name="fake--task--name",
            uuid="fake--id",
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
        name="fake--task--name",
        uuid="fake--id",
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
            name="fake--task--name",
            uuid="fake--id",
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
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        volumes=volumes
    )

    assert tuple(task_config.volumes) == volumes


@pytest.mark.parametrize(
    "empty_volumes", (
        ({"medium": None},),
        ({"medium": "Memory"},),
        ({"size": None},),
        ({"size": "1500m"},),
        ({"size": None, "medium": None},),
        ({"size": "1500m", "medium": None},),
        ({"size": None, "medium": "Memory"},),
        ({"size": "1500m", "medium": "Memory"},),
        ({"random_garbage": "aaaaa"},),
    )
)
def test_empty_volume_rejects_invalid_specification(empty_volumes):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake--task--name",
            uuid="fake--id",
            image="fake_docker_image",
            command="fake_command",
            empty_volumes=empty_volumes
        )


@pytest.mark.parametrize(
    "empty_volumes", (
        ({"container_path": "/a", "size": None, "medium": None},),
        ({"container_path": "/a", "size": "1500m", "medium": None},),
        ({"container_path": "/a", "size": None, "medium": "Memory"},),
        ({"container_path": "/a", "size": "1500m", "medium": "Memory"},),
    )
)
def test_empty_volume_valid_specification(empty_volumes):
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        empty_volumes=empty_volumes
    )

    assert tuple(task_config.empty_volumes) == empty_volumes


@pytest.mark.parametrize(
    "secret_environment", (
        pmap({'SECRET1': {'secret_name': 'taskprocns-secret-secret1', 'key': 'secret_1'}}),
        pmap({
            'SECRET_A': {'secret_name': 'taskprocns-secret-secret--a', 'key': 'secreta'},
            'SECRET_B': {'secret_name': 'taskprocns-secret-secret--b', 'key': 'secretb'},
        }),
    )
)
def test_secret_env_valid_specification(secret_environment):
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        secret_environment=secret_environment
    )

    assert task_config.secret_environment == secret_environment


@pytest.mark.parametrize(
    "secret_environment", (
        pmap({'SECRET1': {
            'secret_name': 'taskprocns-secret-1', 'key': 'secret-1', 'namespace': 'otherns'
        }}),
        pmap({'SECRET1': {'secret_name': 'taskprocns-secret-2'}})
    )
)
def test_secret_env_rejects_invalid_specification(secret_environment):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake--task--name",
            uuid="fake--id",
            image="fake_docker_image",
            command="fake_command",
            secret_environment=secret_environment
        )


@pytest.mark.parametrize(
    "field_selector_environment", (
        pmap({'PAASTA_POD_IP': {"field_path": "status.podIP"}}),
        pmap({
            'PAASTA_POD_IP': {"field_path": "status.podIP"},
            'PAASTA_HOST': {"field_path": "spec.nodeName"},
        }),
    )
)
def test_field_selector_env_valid_specification(field_selector_environment):
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        field_selector_environment=field_selector_environment
    )

    assert task_config.field_selector_environment == field_selector_environment


@pytest.mark.parametrize(
    "field_selector_environment", (
        pmap({'PAASTA_POD_IP': {}}),
        pmap({
            'PAASTA_POD_IP': {"path": "status.podIP"},
            'PAASTA_HOST': {"field_path": "spec.nodeName"},
        }),
    )
)
def test_field_selector_env_rejects_invalid_specification(field_selector_environment):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake--task--name",
            uuid="fake--id",
            image="fake_docker_image",
            command="fake_command",
            field_selector_environment=field_selector_environment
        )


@pytest.mark.parametrize(
    "node_affinity,exc_msg", [
        ({}, "missing keys"),  # missing required keys
        ({"key": "a_label"}, "missing keys"),  # missing operator
        ({"key": "a_label", "operator": "Exists"}, "missing keys"),  # missing value
        (  # invalid operator
            {"key": "a_label", "operator": "BAD", "value": None},
            "got 'BAD', but expected one of",
        ),
        (  # non-list value
            {"key": "a_label", "operator": "In", "value": None},
            "got non-list value 'None' for affinity operator 'In'",
        ),
        (  # non-list value
            {"key": "a_label", "operator": "NotIn", "value": None},
            "got non-list value 'None' for affinity operator 'NotIn'",
        ),
        (  # non-int value
            {"key": "a_label", "operator": "Gt", "value": None},
            "got non-int value 'None' for affinity operator 'Gt'",
        ),
        (  # non-int value
            {"key": "a_label", "operator": "Lt", "value": None},
            "got non-int value 'None' for affinity operator 'Lt'",
        ),
    ],
)
def test_valid_node_affinities_invalid_affinity(node_affinity, exc_msg):
    with pytest.raises(InvariantException) as exc:
        KubernetesTaskConfig(
            image="fake_docker_image",
            command="fake_command",
            node_affinities=[node_affinity]
        )
    assert exc_msg in exc.value.invariant_errors[0]


@pytest.mark.parametrize(
    "service_account_name", (
        "",
        "F" * 300,
        "bad_name",
    )
)
def test_service_account_name_invariant(service_account_name):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            name="fake--task--name",
            uuid="fake--id",
            image="fake_docker_image",
            command="fake_command",
            service_account_name=service_account_name,
        )


@pytest.mark.parametrize(
    "service_account_name", (
        None,
        "yay",
    )
)
def test_service_account_name_invariant_success(service_account_name):
    KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        service_account_name=service_account_name,
    )


@pytest.mark.parametrize(
    "ports", (
        [0],
        [99999],
        [65536],
        [1, 2, 3, 4, 0],
    ),
)
def test_valid_ports_invariant_failure(ports):
    with pytest.raises(InvariantException):
        KubernetesTaskConfig(
            image="fake_docker_image",
            command="fake_command",
            ports=ports
        )


@pytest.mark.parametrize(
    "ports", (
        [1],
        [65535],
        [1, 2, 3, 4, 5],
    ),
)
def test_valid_ports_invariant(ports):
    task_config = KubernetesTaskConfig(
        name="fake--task--name",
        uuid="fake--id",
        image="fake_docker_image",
        command="fake_command",
        ports=ports,
    )

    assert task_config.ports == ports
