import os
from unittest import mock

import pytest
from kubernetes.client.exceptions import ApiException

from task_processing.plugins.kubernetes.kube_client import ExceededMaxAttempts
from task_processing.plugins.kubernetes.kube_client import KubeClient


def test_KubeClient_no_kubeconfig():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ), pytest.raises(
        ValueError
    ):
        KubeClient()


def test_KubeClient_kubeconfig_init():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client:
        client = KubeClient(kubeconfig_path="/some/kube/config.conf")

        assert client.core == mock_kube_client.CoreV1Api()


def test_KubeClient_kubeconfig_env_var():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        client = KubeClient()

        assert client.core == mock_kube_client.CoreV1Api()


def test_KubeClient_kubeconfig_init_overrides_env_var():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ) as mock_load_config, mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        mock_config_path = "/OVERRIDE.conf"

        client = KubeClient(kubeconfig_path=mock_config_path)

        assert client.core == mock_kube_client.CoreV1Api()
        mock_load_config.assert_called_once_with(
            config_file=mock_config_path, context=None
        )


def test_KubeClient_get_pod_too_many_failures():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ), pytest.raises(
        ExceededMaxAttempts
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_kube_client.CoreV1Api().read_namespaced_pod.side_effect = [
            ApiException,
            ApiException,
        ]
        client = KubeClient(kubeconfig_path=mock_config_path)
        client.get_pod(namespace="ns", pod_name="pod-name", attempts=2)
    assert mock_kube_client.CoreV1Api().read_namespaced_pod.call_count == 2


def test_KubeClient_get_pod_unknown_exception():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ), pytest.raises(
        Exception
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_kube_client.CoreV1Api().read_namespaced_pod.side_effect = [Exception]
        client = KubeClient(kubeconfig_path=mock_config_path)
        client.get_pod(namespace="ns", pod_name="pod-name", attempts=2)


def test_KubeClient_get_pod():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_kube_client.CoreV1Api().read_namespaced_pod.return_value = mock.Mock()
        client = KubeClient(kubeconfig_path=mock_config_path)
        client.get_pod(namespace="ns", pod_name="pod-name", attempts=1)
    mock_kube_client.CoreV1Api().read_namespaced_pod.assert_called_once_with(
        namespace="ns", name="pod-name"
    )


def test_KubeClient_get_pods():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_pods = [mock.Mock(), mock.Mock()]
        mock_kube_client.CoreV1Api().list_namespaced_pod.return_value.items = mock_pods
        client = KubeClient(kubeconfig_path=mock_config_path)
        result = client.get_pods(namespace="ns", attempts=1)

        assert result == mock_pods
        mock_kube_client.CoreV1Api().list_namespaced_pod.assert_called_once_with(
            namespace="ns"
        )


def test_KubeClient_get_pods_404():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        mock_config_path = "/OVERRIDE.conf"
        api_exception = ApiException(status=404)
        mock_kube_client.CoreV1Api().list_namespaced_pod.side_effect = api_exception
        client = KubeClient(kubeconfig_path=mock_config_path)
        result = client.get_pods(namespace="ns", attempts=1)

        assert result == []
        mock_kube_client.CoreV1Api().list_namespaced_pod.assert_called_once_with(
            namespace="ns"
        )


def test_KubeClient_get_pods_unauthorized():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ):
        mock_config_path = "/OVERRIDE.conf"
        api_exception = ApiException(status=401)
        mock_kube_client.CoreV1Api().list_namespaced_pod.side_effect = [
            api_exception,
            mock.Mock(items=[mock.Mock()]),
        ]
        client = KubeClient(kubeconfig_path=mock_config_path)

        # Mock the reload method to verify it's called
        with mock.patch.object(
            client, "reload_kubeconfig", autospec=True
        ) as mock_reload:
            result = client.get_pods(namespace="ns", attempts=2)

            assert len(result) == 1
            assert mock_reload.call_count == 1
            assert mock_kube_client.CoreV1Api().list_namespaced_pod.call_count == 2


def test_KubeClient_get_pods_too_many_failures():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ), pytest.raises(
        ExceededMaxAttempts
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_kube_client.CoreV1Api().list_namespaced_pod.side_effect = [
            ApiException(status=500),
            ApiException(status=500),
        ]
        client = KubeClient(kubeconfig_path=mock_config_path)
        client.get_pods(namespace="ns", attempts=2)

    assert mock_kube_client.CoreV1Api().list_namespaced_pod.call_count == 2


def test_KubeClient_get_pods_unknown_exception():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True,
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client", autospec=True
    ) as mock_kube_client, mock.patch.dict(
        os.environ, {"KUBECONFIG": "/another/kube/config.conf"}
    ), pytest.raises(
        Exception
    ):
        mock_config_path = "/OVERRIDE.conf"
        mock_kube_client.CoreV1Api().list_namespaced_pod.side_effect = Exception(
            "Unknown error"
        )
        client = KubeClient(kubeconfig_path=mock_config_path)
        client.get_pods(namespace="ns", attempts=2)
