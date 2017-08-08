import mock
import pytest
import yaml

from task_processing import utils


@pytest.fixture
def mock_yamlload():
    services = {
        'task_processing.fake_prefix_fake_cluster': {
            'host': 'fake_host',
            'port': 'fake_port',
        },
    }

    with mock.patch.object(
        yaml,
        'load',
        return_value=services,
    ) as mock_yamlload:
        yield mock_yamlload


def test_get_cluster_master_by_proxy(mock_yamlload):
    proxy_addr = utils.get_cluster_master_by_proxy(
        'fake_prefix',
        'fake_cluster',
        '/dev/null',
    )

    assert proxy_addr == 'fake_host:fake_port'


def test_get_cluster_master_by_proxy_no_proxy(mock_yamlload):
    with pytest.raises(KeyError) as exc_info:
        utils.get_cluster_master_by_proxy(
            'fake_prefix',
            'wrong_cluster',
            '/dev/null',
        )

    assert isinstance(exc_info.value, KeyError)
