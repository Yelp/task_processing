import os
from typing import Optional

from kubernetes import client as kube_client
from kubernetes import config as kube_config


class KubeClient:
    def __init__(self, kube_config_path: Optional[str] = None) -> None:
        kube_config_path = kube_config_path or os.environ.get("KUBECONFIG")
        if kube_config_path is None:
            raise ValueError(
                "No kubeconfig specified: set a KUBECONFIG environment variable "
                "or pass a value for `kube_config_path`!"
            )

        self.kube_config_path = kube_config_path
        self.kubecontext = os.environ.get("KUBECONTEXT")

        kube_config.load_kube_config(
            config_file=self.kube_config_path,
            context=self.kubecontext
        )

        # any Kubernetes APIs that we use should be added as members here (much like as we
        # do in the KubeClient class in PaaSTA) to ensure that they're only used after we've
        # loaded the relevant kubeconfig. Additionally, they should also be recreated in
        # reload_kubeconfig() so that changes in certs are picked up.
        self.core = kube_client.CoreV1Api()

    def reload_kubeconfig(self) -> None:
        """
        Helper meant to be called after a 401 from apiserver.

        Since we rotate certs regularly internally and the kubernetes clientlib doesn't
        automatically watch for changes to the certs specified by the kubeconfig file, we
        need to manually reload said file as well as recreate any API classes (as otherwise
        they don't pick up the new configuration)
        """
        kube_config.load_kube_config(
            config_file=self.kube_config_path,
            context=self.kubecontext
        )
        self.core = kube_client.CoreV1Api()
