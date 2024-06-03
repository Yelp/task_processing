import pytest
from pyrsistent import m
from pyrsistent import v

from task_processing.plugins.mesos.resource_helpers import allocate_task_resources
from task_processing.plugins.mesos.resource_helpers import get_offer_resources
from task_processing.plugins.mesos.resource_helpers import ResourceSet
from task_processing.plugins.mesos.resource_helpers import task_fits


@pytest.fixture
def offer_resources():
    return ResourceSet(
        cpus=10,
        mem=1024,
        disk=1000,
        gpus=1,
    )


@pytest.mark.parametrize("role", ["fake_role", "none"])
def test_get_offer_resources(fake_offer, role):
    assert get_offer_resources(fake_offer, role) == ResourceSet(
        cpus=10 if role != "none" else 0,
        mem=1024 if role != "none" else 0,
        disk=1000 if role != "none" else 0,
        gpus=1 if role != "none" else 0,
        ports=v(m(begin=31200, end=31500)) if role != "none" else v(),
    )


@pytest.mark.parametrize(
    "available_ports",
    [
        v(m(begin=5, end=10)),
        v(m(begin=3, end=3), m(begin=6, end=10)),
    ],
)
def test_allocate_task_resources(fake_task, offer_resources, available_ports):
    offer_resources = offer_resources.set("ports", available_ports)
    expected_port = available_ports[0].begin
    consumed, remaining = allocate_task_resources(fake_task, offer_resources)
    assert consumed == fake_task.set(ports=v(m(begin=expected_port, end=expected_port)))
    assert remaining == {
        "cpus": 0,
        "mem": 0,
        "disk": 0,
        "gpus": 0,
        "ports": v(m(begin=6, end=10)),
    }


@pytest.mark.parametrize(
    "cpus,available_ports",
    [
        (5, v([m(begin=5, end=10)])),
        (10, v()),
        (10, v([m(begin=5, end=10)])),
    ],
)
def test_task_fits(fake_task, offer_resources, cpus, available_ports):
    offer_resources = offer_resources.set("cpus", cpus)
    offer_resources = offer_resources.set("ports", available_ports)
    assert task_fits(fake_task, offer_resources) == (
        cpus == 10 and len(available_ports) > 0
    )
