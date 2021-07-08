import os
from typing import Optional

from kubernetes import client as kube_client
from kubernetes import config as kube_config


class KubeClient:
    def __init__(self, kubeconfig_path: Optional[str] = None) -> None:
        kubeconfig_path = kubeconfig_path or os.environ.get("KUBECONFIG")
        if kubeconfig_path is None:
            raise ValueError(
                "No kubeconfig specified: set a KUBECONFIG environment variable "
                "or pass a value for `kubeconfig_path`!"
            )

        kube_config.load_kube_config(
            config_file=kubeconfig_path,
            context=os.environ.get("KUBECONTEXT")
        )

        # any Kubernetes APIs that we use should be added as members here (much like as we
        # do in the KubeClient class in PaaSTA) to ensure that they're only used after we've
        # loaded the relevant kubeconfig
        self.core = kube_client.CoreV1Api()
