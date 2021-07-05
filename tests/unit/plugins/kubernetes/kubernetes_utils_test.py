from unittest import mock

from task_processing.plugins.kubernetes.utils import ensure_namespace


def test_ensure_namespace():
    mock_metadata = mock.Mock()
    type(mock_metadata).name = "tron"
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="tron")
    assert not mock_client.core.create_namespace.called

    mock_metadata = mock.Mock()
    type(mock_metadata).name = "kube-system"
    mock_namespaces = mock.Mock(items=[mock.Mock(metadata=mock_metadata)])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="tron")
    assert mock_client.core.create_namespace.called

    mock_client.core.create_namespace.reset_mock()
    mock_namespaces = mock.Mock(items=[])
    mock_client = mock.Mock(
        core=mock.Mock(list_namespace=mock.Mock(return_value=mock_namespaces))
    )
    ensure_namespace(mock_client, namespace="tron")
    assert mock_client.core.create_namespace.called
