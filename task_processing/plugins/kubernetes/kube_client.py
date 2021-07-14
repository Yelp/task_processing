import logging
import os
from http import HTTPStatus
from typing import Optional

from kubernetes import client as kube_client
from kubernetes import config as kube_config
from kubernetes.client.exceptions import ApiException
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes.client.models.v1_status import V1Status

logger = logging.getLogger(__name__)

DEFAULT_ATTEMPTS = 2


class KubeClient:
    def __init__(self, kubeconfig_path: Optional[str] = None) -> None:
        kubeconfig_path = kubeconfig_path or os.environ.get("KUBECONFIG")
        if kubeconfig_path is None:
            raise ValueError(
                "No kubeconfig specified: set a KUBECONFIG environment variable "
                "or pass a value for `kubeconfig_path`!"
            )

        self.kubeconfig_path = kubeconfig_path
        self.kubecontext = os.environ.get("KUBECONTEXT")

        kube_config.load_kube_config(
            config_file=self.kubeconfig_path,
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
            config_file=self.kubeconfig_path,
            context=self.kubecontext
        )
        self.core = kube_client.CoreV1Api()

    def maybe_reload_on_exception(self, exception: Exception) -> bool:
        """
        Small wrapper around KubeClient::reload_kubeconfig() to centralize when to reload kubeconfig
        on exceptions.

        Currently, the only exception that will cause a reload is an ApiException with an
        HTTP 401 (unauthorized) status.
        """
        if isinstance(exception, ApiException):
            if exception.status == HTTPStatus.UNAUTHORIZED.value:
                logger.info(
                    "Recieved UNAUTHORIZED response from apiserver - assuming certs have "
                    "expired and reloading."
                )
                self.reload_kubeconfig()
                return True
            logger.info(f"Recieved HTTP {exception.status} from apiserver, not reloading certs.")
        return False

    def terminate_pod(
        self,
        namespace: str,
        pod_name: str,
        attempts: int = DEFAULT_ATTEMPTS,
    ) -> bool:
        """
        Wrapper around delete_namespaced_pod() in the kubernetes clientlib that adds in
        retrying on ApiExceptions.

        Returns True on success, False otherwise.
        """
        while attempts:
            try:
                logger.info(f"Attempting to terminate {pod_name}")
                status: V1Status = self.core.delete_namespaced_pod(
                    name=pod_name,
                    namespace=namespace,
                    # attempt to delete immediately - Pods launched by task_processing
                    # shouldn't need time to clean-up/drain
                    grace_period_seconds=0,
                    # this is the default, but explcitly request background deletion of releated
                    # objects. see:
                    # https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
                    propagation_policy="Background"
                )
                # this is not ideal, but the k8s clientlib returns the status of the request as a
                # string that is either "Success" or "Failure" - we could potentially use `code`
                # instead but it's not exactly documented what HTTP return codes will be used
                return status.status == "Success"
            except ApiException as e:
                if not self.maybe_reload_on_exception(exception=e) and attempts:
                    logger.exception(
                        f"Failed to request termination for {pod_name} due to unhandled API "
                        "exception, retrying."
                    )
                attempts -= 1
            except Exception:
                logger.exception(
                    f"Failed to request termination for {pod_name} due to unhandled exception."
                )
                return False

        logger.info(f"Ran out of retries attempting to request termination of {pod_name}.")
        return False

    def create_pod(
        self,
        namespace: str,
        pod: V1Pod,
        attempts: int = DEFAULT_ATTEMPTS,
    ) -> bool:
        """
        Wrapper around create_namespaced_pod() in the kubernetes clientlib that adds in
        retrying on ApiExceptions.

        Returns True on success, False otherwise.
        """
        while attempts:
            try:
                self.core.create_namespaced_pod(
                    namespace=namespace,
                    body=pod,
                )
                logger.info(f"Successfully created pod {pod.metadata.name}")
                return True
            except ApiException as e:
                if not self.maybe_reload_on_exception(exception=e) and attempts:
                    logger.exception(
                        f"Failed to create {pod.metadata.name} due to unhandled API exception, "
                        "retrying."
                    )
                attempts -= 1
            except Exception:
                logger.exception(
                    f"Failed to create {pod.metadata.name} due to unhandled "
                    "exception."
                )
                return False

        logger.info(f"Ran out of retries attempting to create {pod.metadata.name}.")
        return False
