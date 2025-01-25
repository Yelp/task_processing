import pytest
from kubernetes.client import V1Capabilities
from kubernetes.client import V1EmptyDirVolumeSource
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1KeyToPath
from kubernetes.client import V1NodeAffinity
from kubernetes.client import V1NodeSelector
from kubernetes.client import V1NodeSelectorRequirement
from kubernetes.client import V1NodeSelectorTerm
from kubernetes.client import V1ObjectFieldSelector
from kubernetes.client import V1ProjectedVolumeSource
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecretVolumeSource
from kubernetes.client import V1ServiceAccountTokenProjection
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from kubernetes.client import V1VolumeProjection
from pyrsistent import pmap
from pyrsistent import pvector
from pyrsistent import v

from task_processing.plugins.kubernetes.types import NodeAffinity
from task_processing.plugins.kubernetes.utils import (
    get_capabilities_for_capability_changes,
)
from task_processing.plugins.kubernetes.utils import get_kubernetes_empty_volume_mounts
from task_processing.plugins.kubernetes.utils import get_kubernetes_env_vars
from task_processing.plugins.kubernetes.utils import get_kubernetes_secret_volume_mounts
from task_processing.plugins.kubernetes.utils import (
    get_kubernetes_service_account_token_volume_mounts,
)
from task_processing.plugins.kubernetes.utils import get_kubernetes_volume_mounts
from task_processing.plugins.kubernetes.utils import get_node_affinity
from task_processing.plugins.kubernetes.utils import get_pod_empty_volumes
from task_processing.plugins.kubernetes.utils import get_pod_secret_volumes
from task_processing.plugins.kubernetes.utils import (
    get_pod_service_account_token_volumes,
)
from task_processing.plugins.kubernetes.utils import get_pod_volumes
from task_processing.plugins.kubernetes.utils import get_sanitised_kubernetes_name
from task_processing.plugins.kubernetes.utils import get_sanitised_volume_name


@pytest.mark.parametrize(
    "cap_add,cap_drop,expected",
    (
        (v(), v(), None),
        (v("AUDIT_READ"), v(), V1Capabilities(add=["AUDIT_READ"])),
        (v("AUDIT_READ"), v("AUDIT_READ"), V1Capabilities(add=["AUDIT_READ"])),
        (v(), v("AUDIT_READ"), V1Capabilities(drop=["AUDIT_READ"])),
        (
            v("AUDIT_WRITE"),
            v("AUDIT_READ"),
            V1Capabilities(add=["AUDIT_WRITE"], drop=["AUDIT_READ"]),
        ),
    ),
)
def test_get_capabilities_for_capability_changes(cap_add, cap_drop, expected):
    assert get_capabilities_for_capability_changes(cap_add, cap_drop) == expected


@pytest.mark.parametrize(
    "name,expected_name,length_limit",
    (
        (
            "hello_world",
            "hello--world",
            0,
        ),
        (
            "hello_world",
            "hello--world",
            0,
        ),
        (
            "--hello_world",
            "underscore-hello--world",
            0,
        ),
        (
            "TeSt",
            "test",
            0,
        ),
        (
            "hello--world.123.run",
            "hello--world--cf28",
            19,
        ),
    ),
)
def test_get_sanitised_kubernetes_name(name, expected_name, length_limit):
    assert (
        get_sanitised_kubernetes_name(name, length_limit=length_limit) == expected_name
    )


@pytest.mark.parametrize(
    "name,length_limit,expected",
    (
        ("host--hello--world", 63, "host--hello--world"),
        ("host--hello--world/", 63, "host--hello--world"),
        ("host--hello_world", 63, "host--hello--world"),
        ("host--hello/world", 63, "host--helloslash-world"),
        ("host--hello/.world", 63, "host--helloslash-dot-world"),
        ("host--hello_world", 0, "host--hello--world"),
        (
            "host--hello_world" + ("a" * 60),
            63,
            "host--hello--worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa--1090",
        ),
        (
            "a/b/c/d/e/f/g/h/i/j/k/l",
            63,
            "aslash-bslash-cslash-dslash-eslash-fslash-gslash-hslash-i--646b",
        ),
    ),
)
def test_get_sanitised_volume_name(name, length_limit, expected):
    assert get_sanitised_volume_name(name, length_limit) == expected


@pytest.mark.parametrize(
    "volumes,expected",
    (
        (
            v({"container_path": "/a", "host_path": "/b", "mode": "RO"}),
            [V1VolumeMount(mount_path="/a", name="host--slash-b", read_only=True)],
        ),
        (
            v(
                {"container_path": "/a", "host_path": "/b", "mode": "RO"},
                {
                    "container_path": "/b",
                    "host_path": "/a/b/cd/e/f/g/h/u/j/k/l",
                    "mode": "RW",
                },
            ),
            [
                V1VolumeMount(mount_path="/a", name="host--slash-b", read_only=True),
                V1VolumeMount(
                    mount_path="/b",
                    name="host--slash-aslash-bslash-cdslash-eslash-fslash-gslash-hs--f2c8",
                    read_only=False,
                ),
            ],
        ),
    ),
)
def test_get_kubernetes_volume_mounts(volumes, expected):
    assert get_kubernetes_volume_mounts(volumes) == expected


@pytest.mark.parametrize(
    "volumes,expected",
    (
        (
            v({"container_path": "/a", "host_path": "/b", "mode": "RO"}),
            [V1Volume(name="host--slash-b", host_path=V1HostPathVolumeSource("/b"))],
        ),
        (
            v(
                {"container_path": "/a", "host_path": "/b", "mode": "RO"},
                {
                    "container_path": "/b",
                    "host_path": "/a/b/cd/e/f/g/h/u/j/k/l",
                    "mode": "RW",
                },
            ),
            [
                V1Volume(name="host--slash-b", host_path=V1HostPathVolumeSource("/b")),
                V1Volume(
                    name="host--slash-aslash-bslash-cdslash-eslash-fslash-gslash-hs--f2c8",
                    host_path=V1HostPathVolumeSource("/a/b/cd/e/f/g/h/u/j/k/l"),
                ),
            ],
        ),
    ),
)
def test_get_pod_volumes(volumes, expected):
    assert get_pod_volumes(volumes) == expected


@pytest.mark.parametrize(
    "volumes,expected",
    (
        (
            v(
                {
                    "container_path": "/b",
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "default_mode": "755",
                    "items": None,
                }
            ),
            [V1VolumeMount(mount_path="/b", name="secret--secret", read_only=True)],
        ),
        (
            v(
                {
                    "container_path": "/b",
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "default_mode": "755",
                    "items": [
                        {
                            "key": "key",
                            "path": "path",
                            "mode": "755",
                        }
                    ],
                }
            ),
            [V1VolumeMount(mount_path="/b", name="secret--secret", read_only=True)],
        ),
    ),
)
def test_get_kubernetes_secret_volume_mounts(volumes, expected):
    assert get_kubernetes_secret_volume_mounts(volumes) == expected


@pytest.mark.parametrize(
    "volumes,expected",
    (
        (
            v(
                {
                    "container_path": "/b",
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "default_mode": "755",
                    "items": None,
                }
            ),
            [
                V1Volume(
                    name="secret--secret",
                    secret=V1SecretVolumeSource(
                        secret_name="secretvolumename",
                        default_mode=493,
                        items=None,
                    ),
                )
            ],
        ),
        (
            v(
                {
                    "container_path": "/b",
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "default_mode": "755",
                    "items": [
                        {
                            "key": "key",
                            "path": "path",
                            "mode": "755",
                        }
                    ],
                }
            ),
            [
                V1Volume(
                    name="secret--secret",
                    secret=V1SecretVolumeSource(
                        secret_name="secretvolumename",
                        default_mode=493,
                        items=[
                            V1KeyToPath(
                                key="key",
                                mode=493,
                                path="path",
                            )
                        ],
                    ),
                )
            ],
        ),
        (
            v(
                {
                    "container_path": "/b",
                    "secret_volume_name": "secretvolumename",
                    "secret_name": "secret",
                    "default_mode": "755",
                    "items": None,
                },
                {
                    "container_path": "/c",
                    "secret_volume_name": "secretvolumename2",
                    "secret_name": "secret2",
                    "default_mode": "755",
                    "items": [
                        {
                            "key": "key",
                            "path": "path",
                            "mode": "755",
                        }
                    ],
                },
            ),
            [
                V1Volume(
                    name="secret--secret",
                    secret=V1SecretVolumeSource(
                        secret_name="secretvolumename",
                        default_mode=493,
                        items=None,
                    ),
                ),
                V1Volume(
                    name="secret--secret2",
                    secret=V1SecretVolumeSource(
                        secret_name="secretvolumename2",
                        default_mode=493,
                        items=[
                            V1KeyToPath(
                                key="key",
                                mode=493,
                                path="path",
                            )
                        ],
                    ),
                ),
            ],
        ),
    ),
)
def test_get_pod_secret_volumes(volumes, expected):
    assert get_pod_secret_volumes(volumes) == expected


@pytest.mark.parametrize(
    "empty_volumes,expected",
    (
        (
            v({"container_path": "/a", "medium": None, "size": None}),
            [V1VolumeMount(mount_path="/a", name="empty--slash-a")],
        ),
        (
            v({"container_path": "/a", "medium": "Memory", "size": "1500m"}),
            [V1VolumeMount(mount_path="/a", name="empty--slash-a")],
        ),
    ),
)
def test_get_kubernetes_empty_volume_mounts(empty_volumes, expected):
    assert get_kubernetes_empty_volume_mounts(empty_volumes) == expected


@pytest.mark.parametrize(
    "empty_volumes,expected",
    (
        (
            v({"container_path": "/a", "medium": None, "size": None}),
            [
                V1Volume(
                    name="empty--slash-a",
                    empty_dir=V1EmptyDirVolumeSource(medium=None, size_limit=None),
                )
            ],
        ),
        (
            v({"container_path": "/a", "medium": "Memory", "size": "1500m"}),
            [
                V1Volume(
                    name="empty--slash-a",
                    empty_dir=V1EmptyDirVolumeSource(
                        medium="Memory", size_limit="1500m"
                    ),
                )
            ],
        ),
    ),
)
def test_get_pod_empty_volumes(empty_volumes, expected):
    assert get_pod_empty_volumes(empty_volumes) == expected


def test_get_kubernetes_env_vars():
    test_env_vars = pmap(
        {
            "FAKE_PLAIN_VAR": "not_secret_data",
        }
    )
    test_secret_env_vars = pmap(
        {
            "FAKE_SECRET": {
                "secret_name": "taskns-secret-taskname-some--secret--name",
                "key": "some_secret_name",
            },
            "FAKE_SHARED_SECRET": {
                "secret_name": "taskns-secret-underscore-shared-shared--secret-name",
                "key": "shared_secret-name",
            },
        }
    )
    test_field_selector_env_vars = pmap(
        {"PAASTA_POD_IP": {"field_path": "status.podIP"}}
    )

    expected_env_vars = [
        V1EnvVar(name="FAKE_PLAIN_VAR", value="not_secret_data"),
        V1EnvVar(
            name="FAKE_SECRET",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="taskns-secret-taskname-some--secret--name",
                    key="some_secret_name",
                    optional=False,
                ),
            ),
        ),
        V1EnvVar(
            name="FAKE_SHARED_SECRET",
            value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="taskns-secret-underscore-shared-shared--secret-name",
                    key="shared_secret-name",
                    optional=False,
                ),
            ),
        ),
        V1EnvVar(
            name="PAASTA_POD_IP",
            value_from=V1EnvVarSource(
                field_ref=V1ObjectFieldSelector(
                    field_path="status.podIP",
                )
            ),
        ),
    ]
    env_vars = get_kubernetes_env_vars(
        environment=test_env_vars,
        secret_environment=test_secret_env_vars,
        field_selector_environment=test_field_selector_env_vars,
    )

    assert sorted(expected_env_vars, key=lambda x: x.name) == sorted(
        env_vars, key=lambda x: x.name
    )

    test_dupe_env_vars = test_env_vars.set("FAKE_SECRET", "SECRET(not_secret_data)")

    env_vars = get_kubernetes_env_vars(
        environment=test_dupe_env_vars,
        secret_environment=test_secret_env_vars,
        field_selector_environment=test_field_selector_env_vars,
    )

    assert sorted(expected_env_vars, key=lambda x: x.name) == sorted(
        env_vars, key=lambda x: x.name
    )


def test_get_node_affinity_ok():
    affinities = pvector(
        [
            NodeAffinity(key="label0", operator="In", value=[1, 2, 3]),
            NodeAffinity(key="label1", operator="NotIn", value=[3, 2, 1]),
            NodeAffinity(key="label2", operator="Gt", value=1),
            NodeAffinity(key="label3", operator="Lt", value=2),
            NodeAffinity(key="label4", operator="Exists", value="hi"),
            NodeAffinity(key="label5", operator="DoesNotExist", value="bye"),
        ]
    )

    assert get_node_affinity(affinities) == V1NodeAffinity(
        required_during_scheduling_ignored_during_execution=V1NodeSelector(
            node_selector_terms=[
                V1NodeSelectorTerm(
                    match_expressions=[
                        V1NodeSelectorRequirement(
                            key="label0",
                            operator="In",
                            values=["1", "2", "3"],
                        ),
                        V1NodeSelectorRequirement(
                            key="label1",
                            operator="NotIn",
                            values=["3", "2", "1"],
                        ),
                        V1NodeSelectorRequirement(
                            key="label2",
                            operator="Gt",
                            values=["1"],
                        ),
                        V1NodeSelectorRequirement(
                            key="label3",
                            operator="Lt",
                            values=["2"],
                        ),
                        V1NodeSelectorRequirement(
                            key="label4",
                            operator="Exists",
                            values=[],
                        ),
                        V1NodeSelectorRequirement(
                            key="label5",
                            operator="DoesNotExist",
                            values=[],
                        ),
                    ]
                )
            ]
        )
    )


def test_get_node_affinity_empty():
    assert get_node_affinity([]) is None


def test_get_pod_service_account_token_volume():
    assert get_pod_service_account_token_volumes(
        [{"audience": "foo.bar.com", "expiration_seconds": 1234}]
    ) == [
        V1Volume(
            name="projected-sa--foodot-bardot-com",
            projected=V1ProjectedVolumeSource(
                sources=[
                    V1VolumeProjection(
                        service_account_token=V1ServiceAccountTokenProjection(
                            audience="foo.bar.com",
                            expiration_seconds=1234,
                            path="token",
                        ),
                    ),
                ],
            ),
        )
    ]


def test_get_kubernetes_service_account_token_volume_mount():
    assert get_kubernetes_service_account_token_volume_mounts(
        [
            {
                "audience": "foo.bar.com",
                "expiration_seconds": 1234,
                "container_path": "/var/run/secrets/foo.bar.com/serviceaccount",
            }
        ],
    ) == [
        V1VolumeMount(
            mount_path="/var/run/secrets/foo.bar.com/serviceaccount",
            name="projected-sa--foodot-bardot-com",
            read_only=True,
        )
    ]
