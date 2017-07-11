import mock
import pytest
import yaml

import task_processing.plugins.smartstack as smartstack


@pytest.fixture(params=[
    {},
    {'proxy_port': 'fake_port'}
])
def mock_yamlload(request):
    fake_conf = {'mesos_leader_paasta_fake_cluster_fake_region': request.param}

    with mock.patch.object(yaml, 'load', return_value=fake_conf) as\
            mock_yamlload:
        yield mock_yamlload


@pytest.mark.parametrize(
    'mock_yamlload',
    [{'proxy_port': 'fake_port'}],
    indirect=True
)
def test_get_mesos_master(mock_yamlload):
    smartstack.TASKPROC_SERVICE_FILE = '/dev/null'

    master_ip = smartstack.get_mesos_master('fake_cluster', 'fake_region')

    assert master_ip == '169.254.255.254:fake_port'


@pytest.mark.parametrize(
    'mock_yamlload',
    [{'proxy_port': 'fake_port'}],
    indirect=True
)
def test_get_mesos_master_no_leader(mock_yamlload):
    smartstack.TASKPROC_SERVICE_FILE = '/dev/null'

    with pytest.raises(smartstack.MesosLeaderNotFound) as exc_info:
        smartstack.get_mesos_master('wrong_cluster', 'fake_region')

    assert isinstance(exc_info.value, smartstack.MesosLeaderNotFound)


@pytest.mark.parametrize('mock_yamlload', [{}], indirect=True)
def test_get_mesos_master_no_port(mock_yamlload):
    smartstack.TASKPROC_SERVICE_FILE = '/dev/null'

    with pytest.raises(smartstack.ProxyPortNotFound) as exc_info:
        smartstack.get_mesos_master('fake_cluster', 'fake_region')

    assert isinstance(exc_info.value, smartstack.ProxyPortNotFound)
