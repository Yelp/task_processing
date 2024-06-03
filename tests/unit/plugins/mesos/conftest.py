import addict
import mock
import pytest
from pyrsistent import m
from pyrsistent import v

from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def fake_task():
    return MesosTaskConfig(
        name="fake_name",
        cpus=10.0,
        mem=1024.0,
        disk=1000.0,
        gpus=1,
        ports=v(m(begin=31200, end=31200)),
        image="fake_image",
        cmd='echo "fake"',
    )


@pytest.fixture
def fake_offer():
    return addict.Dict(
        id=addict.Dict(value="fake_offer_id"),
        agent_id=addict.Dict(value="fake_agent_id"),
        hostname="fake_hostname",
        resources=[
            addict.Dict(
                role="fake_role",
                name="cpus",
                scalar=addict.Dict(value=10),
                type="SCALAR",
            ),
            addict.Dict(
                role="other_fake_role",
                name="cpus",
                scalar=addict.Dict(value=20),
                type="SCALAR",
            ),
            addict.Dict(
                role="fake_role",
                name="mem",
                scalar=addict.Dict(value=1024),
                type="SCALAR",
            ),
            addict.Dict(
                role="fake_role",
                name="disk",
                scalar=addict.Dict(value=1000),
                type="SCALAR",
            ),
            addict.Dict(
                role="fake_role",
                name="gpus",
                scalar=addict.Dict(value=1),
                type="SCALAR",
            ),
            addict.Dict(
                role="fake_role",
                name="ports",
                ranges=addict.Dict(range=[addict.Dict(begin=31200, end=31500)]),
                type="RANGES",
            ),
        ],
        attributes=[
            addict.Dict(name="pool", text=addict.Dict(value="fake_pool_text")),
            addict.Dict(
                name="region",
                text=addict.Dict(value="fake_region_text"),
            ),
        ],
    )


@pytest.fixture
def mock_fw_and_driver():
    with mock.patch(
        "task_processing.plugins.mesos.mesos_executor.ExecutionFramework"
    ) as mock_execution_framework, mock.patch(
        "task_processing.plugins.mesos.mesos_executor.MesosSchedulerDriver"
    ) as mock_scheduler_driver:
        mock_execution_framework.return_value.framework_info = mock.Mock()
        yield mock_execution_framework, mock_scheduler_driver
