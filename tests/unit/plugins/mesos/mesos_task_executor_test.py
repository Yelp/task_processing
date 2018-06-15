import mock
import pytest

from task_processing.plugins.mesos.mesos_task_executor import MesosTaskExecutor


@pytest.fixture
def mesos_task_executor(request, mock_Thread, mock_fw_and_driver):
    executor = MesosTaskExecutor('role')

    def mesos_executor_teardown():
        executor.stop()
    request.addfinalizer(mesos_executor_teardown)

    return executor


@pytest.fixture
def resource_patches():
    with mock.patch(
        'task_processing.plugins.mesos.mesos_task_executor.get_offer_resources',
    ), mock.patch(
        'task_processing.plugins.mesos.mesos_task_executor.task_fits',
    ) as mock_fits, mock.patch(
        'task_processing.plugins.mesos.mesos_task_executor.offer_matches_task_constraints',
    ) as mock_constraints, mock.patch(
        'task_processing.plugins.mesos.mesos_task_executor.allocate_task_resources',
    ) as mock_allocate, mock.patch(
        'task_processing.plugins.mesos.mesos_task_executor.make_mesos_task_info',
    ) as mock_make_task:
        yield mock_fits, mock_constraints, mock_allocate, mock_make_task


@pytest.mark.parametrize('fits,constraints', [(False, True), (True, False)])
def test_handle_offer_doesnt_fit(mesos_task_executor, resource_patches, fits, constraints):
    mock_fits, mock_constraints, mock_allocate, mock_make_task = resource_patches
    mock_fits.return_value = fits
    mock_constraints.return_value = constraints
    tasks_to_launch, tasks_to_defer = mesos_task_executor.handle_offer([mock.Mock()], mock.Mock())

    assert mock_allocate.call_count == 0
    assert mock_make_task.call_count == 0
    assert len(tasks_to_launch) == 0
    assert len(tasks_to_defer) == 1


def test_handle_offer(mesos_task_executor, resource_patches):
    _, _, mock_allocate, mock_make_task = resource_patches
    fake_mesos_task_info = mock.Mock()
    mock_allocate.return_value = mock.Mock(), []
    mock_make_task.return_value = fake_mesos_task_info
    tasks_to_launch, tasks_to_defer = mesos_task_executor.handle_offer([mock.Mock()], mock.Mock())

    assert mock_allocate.call_count == 1
    assert mock_make_task.call_count == 1
    assert tasks_to_launch == [fake_mesos_task_info]
    assert len(tasks_to_defer) == 0
