import socket
import threading
import time
from queue import Queue

import mock
import pytest
from addict import Dict
from pyrsistent import m

from task_processing.plugins.mesos import metrics
from task_processing.plugins.mesos.constraints import attributes_match_constraints
from task_processing.plugins.mesos.execution_framework import ExecutionFramework
from task_processing.plugins.mesos.execution_framework import TaskMetadata
from task_processing.plugins.mesos.mesos_executor import MesosExecutorCallbacks
from task_processing.plugins.mesos.task_config import MesosTaskConfig


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def ef(mock_Thread):
    return ExecutionFramework("fake_name", "fake_role", mock.Mock(), 240)


@pytest.fixture
def mock_driver():
    with mock.patch('pymesos.MesosSchedulerDriver', autospec=True) as m:
        m.id = 'mock_driver'
        yield m


@pytest.fixture
def mock_get_metric():
    with mock.patch(
        'task_processing.plugins.mesos.execution_framework.get_metric',
    ) as mock_get_metric:
        yield mock_get_metric


@pytest.fixture
def mock_time():
    with mock.patch.object(time, 'time') as mock_time:
        yield mock_time


@pytest.fixture
def mock_sleep(ef):
    def stop_killing(task_id):
        ef.stopping = True

    with mock.patch.object(time, 'sleep', side_effect=stop_killing) as mock_sleep:
        yield mock_sleep


def test_ef_kills_stuck_tasks(
    ef,
    fake_task,
    mock_sleep,
    mock_get_metric
):
    task_id = fake_task.task_id
    task_metadata = TaskMetadata(
        agent_id='fake_agent_id',
        task_config=fake_task,
        task_state='TASK_STAGING',
        task_state_history=m(TASK_STAGING=0.0),
    )
    ef.task_staging_timeout_s = 0
    ef.kill_task = mock.Mock()
    ef.blacklist_slave = mock.Mock()
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.callbacks = MesosExecutorCallbacks(mock.Mock(), mock.Mock(), mock.Mock())

    ef._background_check()

    assert ef.kill_task.call_count == 1
    assert ef.kill_task.call_args == mock.call(task_id)
    assert ef.blacklist_slave.call_count == 1
    assert ef.blacklist_slave.call_args == mock.call(
        agent_id='fake_agent_id',
        timeout=900
    )
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(metrics.TASK_STUCK_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_reenqueue_tasks_stuck_in_unknown_state(
    ef,
    fake_task,
    mock_sleep,
    mock_get_metric
):
    task_id = fake_task.task_id
    task_metadata = TaskMetadata(
        agent_id='fake_agent_id',
        task_config=fake_task,
        task_state='UNKNOWN',
        task_state_history=m(UNKNOWN=0.0),
    )
    ef.task_staging_timeout_s = 0
    ef.kill_task = mock.Mock()
    ef.blacklist_slave = mock.Mock()
    ef.enqueue_task = mock.Mock()
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)

    ef._background_check()

    assert ef.enqueue_task.call_count == 1
    assert ef.enqueue_task.call_args == mock.call(
        ef.task_metadata[task_id].task_config
    )
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(metrics.TASK_FAILED_TO_LAUNCH_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_offer_matches_pool_no_pool(ef, fake_offer):
    match, _ = ef.offer_matches_pool(fake_offer)
    assert match


def test_offer_matches_pool_match(ef, fake_offer):
    ef.pool = 'fake_pool_text'
    match, _ = ef.offer_matches_pool(fake_offer)

    assert match


def test_offer_matches_pool_no_match(ef, fake_offer):
    ef.pool = 'fake_other_pool_text'
    match, _ = ef.offer_matches_pool(fake_offer)

    assert not match


def test_offer_matches_constraints_no_constraints(ef, fake_task, fake_offer):
    attributes = {attribute.name: attribute.value for attribute in fake_offer.attributes}
    match = attributes_match_constraints(attributes, fake_task.constraints)
    assert match


def test_offer_matches_constraints_match(ef, fake_offer):
    attributes = {attribute.name: attribute.text.value for attribute in fake_offer.attributes}
    fake_task = MesosTaskConfig(
        image='fake_image',
        cmd='echo "fake"',
        constraints=[
            ['region', '==', 'fake_region_text'],
        ],
    )
    match = attributes_match_constraints(attributes, fake_task.constraints)
    assert match


def test_offer_matches_constraints_no_match(ef, fake_offer):
    attributes = {attribute.name: attribute.text.value for attribute in fake_offer.attributes}
    fake_task = MesosTaskConfig(
        image='fake_image',
        cmd='echo "fake"',
        constraints=[
            ['region', '==', 'another_fake_region_text'],
        ],
    )
    match = attributes_match_constraints(attributes, fake_task.constraints)
    assert not match


def test_kill_task(ef, mock_driver):
    ef.driver = mock_driver

    ef.kill_task('fake_task_id')

    assert mock_driver.killTask.call_count == 1
    assert mock_driver.killTask.call_args == mock.call(
        Dict(value='fake_task_id')
    )


def test_kill_task_from_task_queue(ef, mock_driver):
    ef.driver = mock_driver
    ef.task_queue = Queue()
    ef.task_queue.put(mock.Mock(task_id='fake_task_id'))
    ef.task_queue.put(mock.Mock(task_id='fake_task_id1'))

    ef.kill_task('fake_task_id')

    assert mock_driver.killTask.call_count == 0
    assert ef.task_queue.qsize() == 1


def test_blacklist_slave(
    ef,
    mock_get_metric,
    mock_time
):
    agent_id = 'fake_agent_id'
    mock_time.return_value = 2.0

    ef.blacklisted_slaves = ef.blacklisted_slaves.append(agent_id)
    ef.blacklist_slave(agent_id, timeout=2.0)

    assert agent_id in ef.blacklisted_slaves
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(metrics.BLACKLISTED_AGENTS_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_unblacklist_slave(
    ef,
    mock_time,
    mock_sleep
):
    agent_id = 'fake_agent_id'

    ef.blacklisted_slaves = ef.blacklisted_slaves.append(agent_id)
    ef.unblacklist_slave(agent_id, timeout=0.0)

    assert agent_id not in ef.blacklisted_slaves


def test_enqueue_task(
    ef,
    fake_task,
    mock_driver,
    mock_get_metric
):
    ef.are_offers_suppressed = True
    ef.driver = mock_driver

    ef.enqueue_task(fake_task)

    assert ef.task_metadata[fake_task.task_id].task_state == 'TASK_INITED'
    assert not ef.task_queue.empty()
    assert ef.driver.reviveOffers.call_count == 1
    assert not ef.are_offers_suppressed
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(metrics.TASK_ENQUEUED_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_stop(ef):
    ef.stop()

    assert ef.stopping


def test_initialize_metrics(ef):
    default_dimensions = {
        'framework_name': 'fake_name',
        'framework_role': 'fake_role'
    }
    with mock.patch(
        'task_processing.plugins.mesos.execution_framework.create_counter',
    ) as mock_create_counter, mock.patch(
        'task_processing.plugins.mesos.execution_framework.create_timer',
    ) as mock_create_timer:
        ef._initialize_metrics()

        counters = [
            metrics.TASK_LAUNCHED_COUNT,
            metrics.TASK_FINISHED_COUNT,
            metrics.TASK_FAILED_COUNT,
            metrics.TASK_LAUNCH_FAILED_COUNT,
            metrics.TASK_FAILED_TO_LAUNCH_COUNT,
            metrics.TASK_KILLED_COUNT,
            metrics.TASK_LOST_COUNT,
            metrics.TASK_LOST_DUE_TO_INVALID_OFFER_COUNT,
            metrics.TASK_ERROR_COUNT,
            metrics.TASK_ENQUEUED_COUNT,
            metrics.TASK_INSUFFICIENT_OFFER_COUNT,
            metrics.TASK_STUCK_COUNT,
            metrics.BLACKLISTED_AGENTS_COUNT,
            metrics.TASK_OFFER_TIMEOUT,
        ]
        assert mock_create_counter.call_count == len(counters)
        for cnt in counters:
            mock_create_counter.assert_any_call(cnt, default_dimensions)

        timers = [
            metrics.TASK_QUEUED_TIME_TIMER,
            metrics.OFFER_DELAY_TIMER
        ]
        assert mock_create_timer.call_count == len(timers)
        for tmr in timers:
            mock_create_timer.assert_any_call(tmr, default_dimensions)


def test_slave_lost(ef, mock_driver):
    ef.slaveLost(mock_driver, 'fake_slave_id')


def test_registered(ef, mock_driver):
    ef.registered(
        mock_driver,
        Dict(value='fake_framework_id'),
        'fake_master_info'
    )

    assert ef.driver == mock_driver


def test_reregistered(ef, mock_driver):
    ef.reregistered(
        mock_driver,
        'fake_master_info'
    )


def test_resource_offers_launch(
    ef,
    fake_task,
    fake_offer,
    mock_driver,
    mock_get_metric,
    mock_time
):
    task_id = fake_task.task_id
    ef.driver = mock_driver
    ef._last_offer_time = 1.0
    mock_time.return_value = 2.0
    ef.suppress_after = 0.0
    ef.offer_matches_pool = mock.Mock(return_value=(True, None))
    task_metadata = TaskMetadata(
        task_config=fake_task,
        task_state='fake_state',
        task_state_history=m(fake_state=time.time(), TASK_INITED=time.time())
    )
    fake_task_2 = mock.Mock()
    ef.callbacks.get_tasks_for_offer = mock.Mock(return_value=([fake_task], [fake_task_2]))

    ef.task_queue.put(fake_task)
    ef.task_queue.put(fake_task_2)
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.resourceOffers(ef.driver, [fake_offer])

    assert ef.task_metadata[task_id].agent_id == 'fake_agent_id'
    assert mock_driver.suppressOffers.call_count == 0
    assert not ef.are_offers_suppressed
    assert mock_driver.declineOffer.call_count == 0
    assert mock_driver.launchTasks.call_count == 1
    assert mock_get_metric.call_count == 4
    mock_get_metric.assert_any_call(metrics.OFFER_DELAY_TIMER)
    mock_get_metric.assert_any_call(metrics.TASK_LAUNCHED_COUNT)
    mock_get_metric.assert_any_call(metrics.TASK_QUEUED_TIME_TIMER)
    mock_get_metric.assert_any_call(metrics.TASK_INSUFFICIENT_OFFER_COUNT)
    assert mock_get_metric.return_value.record.call_count == 2
    assert mock_get_metric.return_value.count.call_count == 2


def test_resource_offers_launch_tasks_failed(
    ef,
    fake_task,
    fake_offer,
    mock_driver,
    mock_get_metric,
    mock_time
):
    task_id = fake_task.task_id
    ef.driver = mock_driver
    ef.driver.launchTasks = mock.Mock(side_effect=socket.timeout)
    ef._last_offer_time = None
    mock_time.return_value = 2.0
    ef.suppress_after = 0.0
    ef.offer_matches_pool = mock.Mock(return_value=(True, None))
    task_metadata = TaskMetadata(
        task_config=fake_task,
        task_state='fake_state',
        task_state_history=m(fake_state=time.time(), TASK_INITED=time.time())
    )
    ef.callbacks.get_tasks_for_offer = mock.Mock(return_value=([fake_task], []))
    ef.task_queue.put(fake_task)
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.resourceOffers(ef.driver, [fake_offer])

    assert mock_driver.suppressOffers.call_count == 0
    assert not ef.are_offers_suppressed
    assert mock_driver.declineOffer.call_count == 0
    assert mock_driver.launchTasks.call_count == 1
    assert mock_get_metric.call_count == 2
    assert ef.task_metadata[task_id].task_state == 'UNKNOWN'


def test_resource_offers_no_tasks_to_launch(
    ef,
    fake_offer,
    mock_driver,
    mock_get_metric
):
    ef.suppress_after = 0.0

    ef.resourceOffers(mock_driver, [fake_offer])

    assert mock_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert mock_driver.suppressOffers.call_count == 1
    assert ef.are_offers_suppressed
    assert mock_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def test_resource_offers_blacklisted_offer(
    ef,
    fake_task,
    fake_offer,
    mock_driver,
    mock_get_metric
):
    ef.blacklisted_slaves = ef.blacklisted_slaves.append(
        fake_offer.agent_id.value,
    )
    ef.task_queue.put(fake_task)
    ef.resourceOffers(mock_driver, [fake_offer])

    assert mock_driver.declineOffer.call_count == 1
    assert mock_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert mock_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def test_resource_offers_not_for_pool(
    ef,
    fake_task,
    fake_offer,
    mock_driver,
    mock_get_metric
):
    ef.offer_matches_pool = mock.Mock(return_value=(False, None))

    ef.task_queue.put(fake_task)
    ef.resourceOffers(mock_driver, [fake_offer])

    assert ef.offer_matches_pool.call_count == 1
    assert ef.offer_matches_pool.call_args == mock.call(fake_offer)
    assert mock_driver.declineOffer.call_count == 1
    assert mock_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert mock_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def test_resource_offers_unmet_reqs(
    ef,
    fake_task,
    fake_offer,
    mock_driver,
    mock_get_metric
):
    ef.callbacks.get_tasks_for_offer = mock.Mock(return_value=([], [fake_task]))

    ef.task_queue.put(fake_task)
    ef.resourceOffers(mock_driver, [fake_offer])

    assert mock_driver.declineOffer.call_count == 1
    assert mock_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert mock_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 1
    mock_get_metric.assert_any_call(metrics.TASK_INSUFFICIENT_OFFER_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1


def status_update_test_prep(state, reason=''):
    task = MesosTaskConfig(
        cmd='/bin/true', name='fake_name', image='fake_image')
    task_id = task.task_id
    update = Dict(
        task_id=Dict(value=task_id),
        state=state,
        reason=reason
    )
    task_metadata = TaskMetadata(
        task_config=task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=time.time()),
    )

    return update, task_id, task_metadata


def test_status_update_record_only(
    ef,
    mock_driver
):
    update, task_id, task_metadata = status_update_test_prep('fake_state1')
    ef.translator = mock.Mock()

    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.statusUpdate(mock_driver, update)

    assert ef.task_metadata[task_id].task_state == 'fake_state1'
    assert len(ef.task_metadata[task_id].task_state_history) == 2
    assert mock_driver.acknowledgeStatusUpdate.call_count == 1
    assert mock_driver.acknowledgeStatusUpdate.call_args == mock.call(update)


def test_status_update_finished(
    ef,
    mock_driver,
    mock_get_metric
):
    # finished task does same thing as other states
    update, task_id, task_metadata = status_update_test_prep('TASK_FINISHED')
    ef.translator = mock.Mock()

    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.statusUpdate(mock_driver, update)

    assert task_id not in ef.task_metadata
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(metrics.TASK_FINISHED_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)
    assert mock_driver.acknowledgeStatusUpdate.call_count == 1
    assert mock_driver.acknowledgeStatusUpdate.call_args == mock.call(update)


def test_ignore_status_update(
    ef,
    mock_driver,
    mock_get_metric
):
    update, task_id, task_metadata = status_update_test_prep('TASK_FINISHED')
    ef.translator = mock.Mock()

    ef.statusUpdate(mock_driver, update)

    assert task_id not in ef.task_metadata
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0
    assert mock_driver.acknowledgeStatusUpdate.call_count == 1


def test_task_lost_due_to_invalid_offers(
    ef,
    mock_driver,
    mock_get_metric
):
    update, task_id, task_metadata = status_update_test_prep(
        state='TASK_LOST',
        reason='REASON_INVALID_OFFERS'
    )
    ef.task_metadata = ef.task_metadata.set(
        task_id,
        task_metadata
    )

    ef.statusUpdate(mock_driver, update)

    assert task_id in ef.task_metadata
    assert mock_get_metric.call_count == 2
    assert ef.event_queue.qsize() == 0
    assert ef.task_queue.qsize() == 1
    assert mock_driver.acknowledgeStatusUpdate.call_count == 1


def test_background_thread_removes_offer_timeout(
    ef,
    mock_driver,
    fake_task,
    mock_time,
    mock_sleep,
):
    mock_time.return_value = 2.0
    task_id = fake_task.task_id
    fake_task = fake_task.set(
        offer_timeout=1
    )
    task_metadata = TaskMetadata(
        agent_id='fake_agent_id',
        task_config=fake_task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=0.0),
    )
    ef.driver = mock_driver
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef._background_check()
    assert ef.task_queue.empty()
    assert task_id not in ef.task_metadata.keys()
    assert not ef.event_queue.empty()
    event = ef.event_queue.get(block=False)
    assert event.terminal is True
    assert event.success is False
    assert event.task_id == task_id
