import pytest

from task_processing.plugins.mesos.execution_framework import (
    ExecutionFramework
)


@pytest.fixture
def ef():
    return ExecutionFramework("name", "role")


def test_ef_kills_stuck_tasks():
    pass


def test_ef_blacklists_stuck_slaves():
    pass


def test_enqueue_task():
    pass


def test_get_available_ports():
    pass


def test_get_tasks_to_launch():
    pass


def test_create_new_docker_task():
    pass


def test_stop():
    pass


def test_slave_lost():
    pass


def test_reregistered():
    pass


def test_resource_offers():
    pass


def test_status_update():
    pass
