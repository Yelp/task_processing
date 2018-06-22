import mock
import pytest

from task_processing.plugins.mesos.mesos_pod_executor import get_pods_for_offer


@pytest.fixture
def resource_patches():
    with mock.patch(
        'task_processing.plugins.mesos.mesos_pod_executor.task_fits',
    ) as mock_fits, mock.patch(
        'task_processing.plugins.mesos.mesos_pod_executor.attributes_match_constraints',
    ) as mock_constraints, mock.patch(
        'task_processing.plugins.mesos.mesos_pod_executor.allocate_task_resources',
    ) as mock_allocate:
        yield mock_fits, mock_constraints, mock_allocate


@pytest.mark.parametrize('fits,constraints', [(False, True), (True, False)])
def test_get_pods_for_offer_doesnt_fit(resource_patches, fits, constraints):
    mock_fits, mock_constraints, mock_allocate = resource_patches
    mock_fits.side_effect = [True, True, fits]
    mock_constraints.side_effect = [True, True, constraints]
    mock_allocate.return_value = mock.Mock(), []
    pods_to_launch, pods_to_defer = get_pods_for_offer(
        [mock.Mock(tasks=[mock.Mock(), mock.Mock(), mock.Mock()])],
        mock.Mock(),
        mock.Mock(),
        'role',
    )

    assert mock_allocate.call_count == 2
    assert len(pods_to_launch) == 0
    assert len(pods_to_defer) == 1


def test_get_tasks_for_offer(resource_patches):
    _, _, mock_allocate = resource_patches
    mock_allocate.return_value = mock.Mock(), []
    tasks_to_launch, tasks_to_defer = get_pods_for_offer(
        [mock.Mock(tasks=[mock.Mock(), mock.Mock(), mock.Mock()])],
        mock.Mock(),
        mock.Mock(),
        'role',
    )

    assert mock_allocate.call_count == 3
    assert len(tasks_to_launch) == 1
    assert len(tasks_to_defer) == 0
