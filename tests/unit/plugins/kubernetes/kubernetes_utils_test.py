import pytest
from kubernetes.client import V1Capabilities
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1HostPathVolumeSource
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1SecurityContext
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount
from pyrsistent import pmap
from pyrsistent import v

from task_processing.plugins.kubernetes.utils import get_kubernetes_env_vars
from task_processing.plugins.kubernetes.utils import get_kubernetes_volume_mounts
from task_processing.plugins.kubernetes.utils import get_pod_volumes
from task_processing.plugins.kubernetes.utils import get_sanitised_kubernetes_name
from task_processing.plugins.kubernetes.utils import get_sanitised_volume_name
from task_processing.plugins.kubernetes.utils import get_secret_kubernetes_env_var
from task_processing.plugins.kubernetes.utils import get_secret_name_from_ref
from task_processing.plugins.kubernetes.utils import get_security_context_for_capabilities
from task_processing.plugins.kubernetes.utils import is_secret_env_var
from task_processing.plugins.kubernetes.utils import is_shared_secret


@pytest.mark.parametrize(
    "cap_add,cap_drop,expected", (
        (v(), v(), None),
        (v("AUDIT_READ"), v(), V1SecurityContext(capabilities=V1Capabilities(add=["AUDIT_READ"]))),
        (v(), v("AUDIT_READ"), V1SecurityContext(capabilities=V1Capabilities(drop=["AUDIT_READ"]))),
        (v("AUDIT_WRITE"), v("AUDIT_READ"), V1SecurityContext(
            capabilities=V1Capabilities(add=["AUDIT_WRITE"], drop=["AUDIT_READ"]))),
    )
)
def test_get_security_context_for_capabilities(cap_add, cap_drop, expected):
    assert get_security_context_for_capabilities(cap_add, cap_drop) == expected


@pytest.mark.parametrize(
    "value,expected", (
        ("lol", False),
        ("CAPS", False),
        ("SECRET", False,),
        ("SHARED_SECRET", False),
        ("SECRET(secret)", True),
        ("SHARED_SECRET(secret)", True),
        ("SECRET(WOW1)", True),
        ("SHARED_SECRET(WOAH_2)", True),
        ("SECRET(test-2)", True),
        ("SHARED_SECRET(WOAH_2", False),
        ("SECRET(test-2", False),
    )
)
def test_is_secret_env_var(value, expected):
    assert is_secret_env_var(value) is expected


@pytest.mark.parametrize(
    "value,expected", (
        ("SECRET(secret)", "secret"),
        ("SHARED_SECRET(secret2)", "secret2"),
    )
)
def test_get_secret_name_from_ref(value, expected):
    assert get_secret_name_from_ref(value) == expected


@pytest.mark.parametrize(
    "value,expected", (
        ("not_a_secret", False),
        ("SECRET(secret)", False),
        ("SHARED_SECRET(secret2)", True),
    )
)
def test_is_shared_secret(value, expected):
    assert is_shared_secret(value) is expected


@pytest.mark.parametrize(
    "name,expected_name", (
        ("hello_world", "hello--world",),
        ("hello_world", "hello--world",),
        ("--hello_world", "underscore-hello--world",),
        ("TeSt", "test",),
    )
)
def test_get_sanitised_kubernetes_name(name, expected_name):
    assert get_sanitised_kubernetes_name(name) == expected_name


@pytest.mark.parametrize(
    "name,length_limit,expected", (
        ("host--hello--world", 63, "host--hello--world"),
        ("host--hello--world/", 63, "host--hello--world"),
        ("host--hello_world", 63, "host--hello--world"),
        ("host--hello/world", 63, "host--helloslash-world"),
        ("host--hello/.world", 63, "host--helloslash-dot-world"),
        ("host--hello_world", 0, "host--hello--world"),
        (
            "host--hello_world" + ("a" * 60),
            63,
            "host--hello--worldaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa--1090"
        ),
        (
            "a/b/c/d/e/f/g/h/i/j/k/l",
            63,
            "aslash-bslash-cslash-dslash-eslash-fslash-gslash-hslash-i--646b"
        ),
    )
)
def test_get_sanitised_volume_name(name, length_limit, expected):
    assert get_sanitised_volume_name(name, length_limit) == expected


@pytest.mark.parametrize(
    "volumes,expected", (
        (
            v({"container_path": "/a", "host_path": "/b", "mode": "RO"}),
            [V1VolumeMount(mount_path="/a", name="host--slash-b", read_only=True)]
        ),
        (
            v(
                {"container_path": "/a", "host_path": "/b", "mode": "RO"},
                {"container_path": "/b", "host_path": "/a/b/cd/e/f/g/h/u/j/k/l", "mode": "RW"},
            ),
            [
                V1VolumeMount(mount_path="/a", name="host--slash-b", read_only=True),
                V1VolumeMount(
                    mount_path="/b",
                    name="host--slash-aslash-bslash-cdslash-eslash-fslash-gslash-hs--f2c8",
                    read_only=False
                ),
            ]
        ),
    )
)
def test_get_kubernetes_volume_mounts(volumes, expected):
    assert get_kubernetes_volume_mounts(volumes) == expected


@pytest.mark.parametrize(
    "volumes,expected", (
        (
            v({"container_path": "/a", "host_path": "/b", "mode": "RO"}),
            [V1Volume(name="host--slash-b", host_path=V1HostPathVolumeSource("/b"))]
        ),
        (
            v(
                {"container_path": "/a", "host_path": "/b", "mode": "RO"},
                {"container_path": "/b", "host_path": "/a/b/cd/e/f/g/h/u/j/k/l", "mode": "RW"},
            ),
            [
                V1Volume(name="host--slash-b", host_path=V1HostPathVolumeSource("/b")),
                V1Volume(
                    name="host--slash-aslash-bslash-cdslash-eslash-fslash-gslash-hs--f2c8",
                    host_path=V1HostPathVolumeSource("/a/b/cd/e/f/g/h/u/j/k/l"),
                ),
            ]
        ),
    )
)
def test_get_pod_volumes(volumes, expected):
    assert get_pod_volumes(volumes) == expected


@pytest.mark.parametrize(
    "key, value, task_name, namespace, expected", (
        ("FAKE_LOCAL_SECRET", "SECRET(plain_secret)", "taskprefix.subtask.etc", "taskns",
            V1EnvVar(name="FAKE_LOCAL_SECRET", value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="taskns-secret-taskprefix-plain--secret",
                    key="plain_secret",
                    optional=False
                )
            )
            )
         ),
        ("FAKE_LOCAL_SECRET2", "SECRET(plain_secret)", "taskname", "taskns",
            V1EnvVar(name="FAKE_LOCAL_SECRET2", value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="taskns-secret-taskname-plain--secret",
                    key="plain_secret",
                    optional=False
                )
            )
            )
         ),
        ("FAKE_SHARED_SECRET", "SHARED_SECRET(plain_secret)", "taskprefix", "taskns",
            V1EnvVar(name="FAKE_SHARED_SECRET", value_from=V1EnvVarSource(
                secret_key_ref=V1SecretKeySelector(
                    name="taskns-secret-underscore-shared-plain--secret",
                    key="plain_secret",
                    optional=False
                )
            )
            )
         ),
    )
)
def test_get_secret_kubernetes_env_var(key, value, task_name, namespace, expected):
    assert get_secret_kubernetes_env_var(key, value, task_name, namespace) == expected


def test_get_kubernetes_env_vars():
    test_env_vars = pmap(
        {
            "FAKE_PLAIN_VAR": "not_secret_data",
            "FAKE_SECRET": "SECRET(some_secret_name)",
            "FAKE_SHARED_SECRET": "SHARED_SECRET(shared_secret-name)",
        }
    )

    expected_env_vars = [
        V1EnvVar(name="FAKE_PLAIN_VAR", value="not_secret_data"),
        V1EnvVar(name="FAKE_SECRET", value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(
                name="taskns-secret-taskname-some--secret--name",
                key="some_secret_name",
                optional=False,
            ),
        ),
        ),
        V1EnvVar(name="FAKE_SHARED_SECRET", value_from=V1EnvVarSource(
            secret_key_ref=V1SecretKeySelector(
                name="taskns-secret-underscore-shared-shared--secret-name",
                key="shared_secret-name",
                optional=False,
            ),
        ),
        ),
    ]
    env_vars = get_kubernetes_env_vars(environment=test_env_vars,
                                       task_name="taskname.subtask",
                                       namespace="taskns",
                                       )

    assert sorted(expected_env_vars, key=lambda x: x.name) == sorted(env_vars, key=lambda x: x.name)
