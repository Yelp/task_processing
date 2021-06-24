import os
from unittest import mock

import pytest

from task_processing.plugins.kubernetes.kube_client import KubeClient


def test_KubeClient_no_kubeconfig():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ), pytest.raises(ValueError):
        KubeClient()


def test_KubeClient_kubeconfig_init():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ) as mock_kube_client:
        client = KubeClient(kube_config_path="/some/kube/config.conf")

        assert client.core == mock_kube_client.CoreV1Api()


def test_KubeClient_kubeconfig_env_var():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ), mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ) as mock_kube_client, mock.patch.dict(os.environ, {"KUBECONFIG": "/another/kube/config.conf"}):
        client = KubeClient()

        assert client.core == mock_kube_client.CoreV1Api()


def test_KubeClient_kubeconfig_init_overrides_env_var():
    with mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_config.load_kube_config",
        autospec=True
    ) as mock_load_config, mock.patch(
        "task_processing.plugins.kubernetes.kube_client.kube_client",
        autospec=True
    ) as mock_kube_client, mock.patch.dict(os.environ, {"KUBECONFIG": "/another/kube/config.conf"}):
        mock_config_path = "/OVERRIDE.conf"

        client = KubeClient(kube_config_path=mock_config_path)

        assert client.core == mock_kube_client.CoreV1Api()
        mock_load_config.assert_called_once_with(config_file=mock_config_path, context=None)
