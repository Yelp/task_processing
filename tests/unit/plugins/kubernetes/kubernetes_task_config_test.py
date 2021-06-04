from pyrsistent import InvariantException

from task_processing.plugins.kubernetes.task_config import KubernetesTaskConfig


def test_kubernetes_task_config_set_pod_name():
    k1 = KubernetesTaskConfig(name="fake_pod_name")
    k2 = KubernetesTaskConfig(name="fake_pod_name2")

    try:
        k1 = k1.set(name='a'*254)
        assert False, 'Pod name must have up to 253 characters.'
    except InvariantException as e:
        print(e)
        assert True

    try:
        k2 = k2.set(name="INVALID"+k2.name)
        assert False, 'Must comply with Kubernetes pod naming standards.'
    except InvariantException as e:
        print(e)
        assert True
