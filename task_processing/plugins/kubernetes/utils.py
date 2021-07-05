import logging

from kubernetes.client import V1Namespace
from kubernetes.client import V1ObjectMeta

from task_processing.plugins.kubernetes.kube_client import KubeClient

logger = logging.getLogger(__name__)


def ensure_namespace(kube_client: KubeClient, namespace: str) -> None:
    tron_namespace = V1Namespace(
        metadata=V1ObjectMeta(name=namespace, labels={"name": namespace})
    )
    namespaces = kube_client.core.list_namespace()
    namespace_names = [item.metadata.name for item in namespaces.items]
    if namespace not in namespace_names:
        logger.debug(f"No namespace {namespace} found. Creating namespace: {namespace} ")
        kube_client.core.create_namespace(body=tron_namespace)
    else:
        logger.debug(f"Namespace {namespace} already exists.")
