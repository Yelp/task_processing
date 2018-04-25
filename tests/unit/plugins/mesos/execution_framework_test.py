import socket
import threading
import time

import mock
import pytest
from addict import Dict
from pyrsistent import m
from pyrsistent import v
from six.moves.queue import Queue

from task_processing.plugins.mesos import execution_framework as ef_mdl
from task_processing.plugins.mesos import mesos_executor as me_mdl
from task_processing.plugins.mesos.constraints import \
    offer_matches_task_constraints


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, 'Thread') as mock_Thread:
        yield mock_Thread


@pytest.fixture
def ef(mock_Thread):
    return ef_mdl.ExecutionFramework("fake_name", "fake_role", 240)


@pytest.fixture(
    params=[None, 'fake_pool_text'],
    ids=['without_pool', 'with_default_pool'],
)
def fake_task(request):
    return me_mdl.MesosTaskConfig(
        name='fake_name',
        cpus=10.0,
        mem=1024.0,
        disk=1000.0,
        gpus=1,
        image='fake_image',
        cmd='echo "fake"',
        pool=request.param,
    )


@pytest.fixture
def fake_offer():
    return Dict(
        id=Dict(value='fake_offer_id'),
        agent_id=Dict(value='fake_agent_id'),
        hostname='fake_hostname',
        resources=[
            Dict(
                role='fake_role',
                name='cpus',
                scalar=Dict(value=10),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='mem',
                scalar=Dict(value=1024),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='disk',
                scalar=Dict(value=1000),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='gpus',
                scalar=Dict(value=1),
                type='SCALAR',
            ),
            Dict(
                role='fake_role',
                name='ports',
                ranges=Dict(range=[Dict(begin=31200, end=31500)]),
                type='RANGES',
            ),
        ],
        attributes=[
            Dict(
                name='pool',
                text=Dict(value='fake_pool_text')
            ),
            Dict(
                name='region',
                text=Dict(value='fake_region_text'),
            ),
        ]
    )


@pytest.fixture
def fake_driver():
    fake_driver = mock.Mock(spec=[
        'id',
        'declineOffer',
        'suppressOffers',
        'reviveOffers',
        'launchTasks',
        'killTask',
        'acknowledgeStatusUpdate'
    ])
    fake_driver.id = 'fake_driver'

    return fake_driver


@pytest.fixture
def mock_get_metric():
    with mock.patch.object(ef_mdl, 'get_metric') as mock_get_metric:
        yield mock_get_metric


@pytest.fixture
def mock_time():
    with mock.patch.object(time, 'time') as mock_time:
        yield mock_time


@pytest.fixture
def mock_sleep(ef):
    def stop_killing(task_id):
        ef.stopping = True

    with mock.patch.object(time, 'sleep', side_effect=stop_killing) as\
            mock_sleep:
        yield mock_sleep


def test_ef_kills_stuck_tasks(
    ef,
    fake_task,
    mock_sleep,
    mock_get_metric
):
    task_id = fake_task.task_id
    task_metadata = ef_mdl.TaskMetadata(
        agent_id='fake_agent_id',
        task_config=fake_task,
        task_state='TASK_STAGING',
        task_state_history=m(TASK_STAGING=0.0),
    )
    ef.task_staging_timeout_s = 0
    ef.kill_task = mock.Mock()
    ef.blacklist_slave = mock.Mock()
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)

    ef._background_check()

    assert ef.kill_task.call_count == 1
    assert ef.kill_task.call_args == mock.call(task_id)
    assert ef.blacklist_slave.call_count == 1
    assert ef.blacklist_slave.call_args == mock.call(
        agent_id='fake_agent_id',
        timeout=900
    )
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(ef_mdl.TASK_STUCK_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_reenqueue_tasks_stuck_in_unknown_state(
    ef,
    fake_task,
    mock_sleep,
    mock_get_metric
):
    task_id = fake_task.task_id
    task_metadata = ef_mdl.TaskMetadata(
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
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.TASK_FAILED_TO_LAUNCH_COUNT
    )
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
    match = offer_matches_task_constraints(fake_offer, fake_task)
    assert match


def test_offer_matches_constraints_match(ef, fake_offer):
    fake_task = me_mdl.MesosTaskConfig(
        image='fake_image',
        cmd='echo "fake"',
        constraints=[
            ['region', '==', 'fake_region_text'],
        ],
    )
    match = offer_matches_task_constraints(fake_offer, fake_task)
    assert match


def test_offer_matches_constraints_no_match(ef, fake_offer):
    fake_task = me_mdl.MesosTaskConfig(
        image='fake_image',
        cmd='echo "fake"',
        constraints=[
            ['region', '==', 'another_fake_region_text'],
        ],
    )
    match = offer_matches_task_constraints(fake_offer, fake_task)
    assert not match


def test_kill_task(ef, fake_driver):
    ef.driver = fake_driver

    ef.kill_task('fake_task_id')

    assert fake_driver.killTask.call_count == 1
    assert fake_driver.killTask.call_args == mock.call(
        Dict(value='fake_task_id')
    )


def test_kill_task_from_task_queue(ef, fake_driver):
    ef.driver = fake_driver
    ef.task_queue = Queue()
    ef.task_queue.put(mock.Mock(task_id='fake_task_id'))
    ef.task_queue.put(mock.Mock(task_id='fake_task_id1'))

    ef.kill_task('fake_task_id')

    assert fake_driver.killTask.call_count == 0
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
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.BLACKLISTED_AGENTS_COUNT
    )
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)

    for i in range(0, 2):
        ef.blacklisted_slaves = ef.blacklisted_slaves.remove(agent_id)


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
    fake_driver,
    mock_get_metric
):
    ef.are_offers_suppressed = True
    ef.driver = fake_driver

    ef.enqueue_task(fake_task)

    assert ef.task_metadata[fake_task.task_id].task_state == 'TASK_INITED'
    assert not ef.task_queue.empty()
    assert ef.driver.reviveOffers.call_count == 1
    assert not ef.are_offers_suppressed
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(ef_mdl.TASK_ENQUEUED_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_get_available_ports(ef, fake_offer):
    ports_resource = [r for r in fake_offer.resources if r.name is 'ports'][0]

    ports = ef.get_available_ports(ports_resource)

    for p in range(31200, 31500):
        assert p in ports


def test_get_tasks_to_launch_sufficient_offer(
    ef,
    fake_task,
    fake_offer,
    mock_get_metric,
    mock_time
):
    task_metadata = ef_mdl.TaskMetadata(
        task_config=fake_task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=1.0)
    )
    ef.create_new_docker_task = mock.Mock()
    mock_time.return_value = 2.0

    ef.task_queue.put(fake_task)
    ef.task_metadata = ef.task_metadata.set(fake_task.task_id, task_metadata)
    tasks_to_launch, _ = ef.get_tasks_to_launch(fake_offer)

    assert ef.create_new_docker_task.return_value in tasks_to_launch
    assert ef.task_queue.qsize() == 0
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.TASK_QUEUED_TIME_TIMER
    )
    assert mock_get_metric.return_value.record.call_count == 1
    assert mock_get_metric.return_value.record.call_args == mock.call(1.0)


@pytest.mark.parametrize(
    "task_cpus,task_mem,task_disk,task_gpus",
    [(20.0, 1024.0, 1000.0, 1),
     (10.0, 2048.0, 1000.0, 1),
     (10.0, 1024.0, 2000.0, 1),
     (10.0, 1024.0, 1000.0, 2)]
)
def test_get_tasks_to_launch_insufficient_offer(
    ef,
    fake_offer,
    mock_get_metric,
    task_cpus,
    task_mem,
    task_disk,
    task_gpus,
):
    ef.create_new_docker_task = mock.Mock()
    task = me_mdl.MesosTaskConfig(
        cmd='/bin/true',
        name='fake_name',
        image='fake_image',
        cpus=task_cpus,
        mem=task_mem,
        disk=task_disk,
        gpus=task_gpus,
    )

    ef.task_queue.put(task)
    tasks_to_launch, _ = ef.get_tasks_to_launch(fake_offer)

    assert len(tasks_to_launch) == 0
    assert ef.task_queue.qsize() == 1
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.TASK_INSUFFICIENT_OFFER_COUNT
    )
    assert mock_get_metric.call_args != mock.call(
        ef_mdl.TASK_QUEUED_TIME_TIMER
    )
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


@pytest.mark.parametrize('gpus_count,containerizer,container', [
    (1, 'MESOS', Dict(
        type='MESOS',
        volumes=[Dict(
            container_path='fake_container_path',
            host_path='fake_host_path',
            mode='RO'
        )],
        mesos=Dict(
            image=Dict(
                type='DOCKER',
                docker=Dict(name='fake_image'),
            ),
        ),
        network_infos=Dict(
            port_mappings=[Dict(host_port=31200,
                                container_port=8888)],
        ),
    )),
    (0, 'DOCKER', Dict(
        type='DOCKER',
        volumes=[Dict(
            container_path='fake_container_path',
            host_path='fake_host_path',
            mode='RO'
        )],
        docker=Dict(
            image='fake_image',
            network='BRIDGE',
            force_pull_image=True,
            port_mappings=[Dict(host_port=31200,
                                container_port=8888)],
            parameters=[],
        ),
    )),
])
def test_create_new_docker_task(
    ef,
    fake_offer,
    fake_task,
    gpus_count,
    containerizer,
    container,
):
    available_ports = [31200]
    task_id = fake_task.task_id
    task_metadata = ef_mdl.TaskMetadata(
        task_config=fake_task,
        task_state='fake_state',
        task_state_history=m(fake_state=time.time())
    )
    fake_task = fake_task.set(
        volumes=v(
            Dict(
                mode='RO',
                container_path='fake_container_path',
                host_path='fake_host_path'
            )
        ),
        gpus=gpus_count,
        containerizer=containerizer,
    )

    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    docker_task = ef.create_new_docker_task(
        fake_offer,
        fake_task,
        available_ports
    )

    new_docker_task = Dict(
        task_id=Dict(value=task_id),
        agent_id=Dict(value='fake_agent_id'),
        name='executor-{id}'.format(id=task_id),
        resources=[
            Dict(name='cpus',
                 type='SCALAR',
                 role='fake_role',
                 scalar=Dict(value=10.0)),
            Dict(name='mem',
                 type='SCALAR',
                 role='fake_role',
                 scalar=Dict(value=1024.0)),
            Dict(name='disk',
                 type='SCALAR',
                 role='fake_role',
                 scalar=Dict(value=1000.0)),
            Dict(name='gpus',
                 type='SCALAR',
                 role='fake_role',
                 scalar=Dict(value=gpus_count)),
            Dict(name='ports',
                 type='RANGES',
                 role='fake_role',
                 ranges=Dict(
                     range=[Dict(begin=31200, end=31200)]))
        ],
        command=Dict(
            value='echo "fake"',
            uris=[],
            environment=Dict(variables=[])
        ),
        container=container,
    )
    assert ef.task_metadata[task_id].agent_id == 'fake_agent_id'
    assert docker_task == new_docker_task


def test_stop(ef):
    ef.stop()

    assert ef.stopping


def test_initialize_metrics(ef):
    default_dimensions = {
        'framework_name': 'fake_name',
        'framework_role': 'fake_role'
    }
    ef_mdl.create_counter = mock.Mock()
    ef_mdl.create_timer = mock.Mock()

    ef._initialize_metrics()

    assert ef_mdl.create_counter.call_count == 14
    ef_mdl_counters = [
        ef_mdl.TASK_LAUNCHED_COUNT,
        ef_mdl.TASK_FINISHED_COUNT,
        ef_mdl.TASK_FAILED_COUNT,
        ef_mdl.TASK_LAUNCH_FAILED_COUNT,
        ef_mdl.TASK_FAILED_TO_LAUNCH_COUNT,
        ef_mdl.TASK_KILLED_COUNT,
        ef_mdl.TASK_LOST_COUNT,
        ef_mdl.TASK_LOST_DUE_TO_INVALID_OFFER_COUNT,
        ef_mdl.TASK_ERROR_COUNT,
        ef_mdl.TASK_ENQUEUED_COUNT,
        ef_mdl.TASK_INSUFFICIENT_OFFER_COUNT,
        ef_mdl.TASK_STUCK_COUNT,
        ef_mdl.BLACKLISTED_AGENTS_COUNT,
        ef_mdl.TASK_OFFER_TIMEOUT,
    ]
    for cnt in ef_mdl_counters:
        ef_mdl.create_counter.assert_any_call(cnt, default_dimensions)
    assert ef_mdl.create_timer.call_count == 2
    ef_mdl_timers = [
        ef_mdl.TASK_QUEUED_TIME_TIMER,
        ef_mdl.OFFER_DELAY_TIMER
    ]
    for tmr in ef_mdl_timers:
        ef_mdl.create_timer.assert_any_call(tmr, default_dimensions)


def test_slave_lost(ef, fake_driver):
    ef.slaveLost(fake_driver, 'fake_slave_id')


def test_registered(ef, fake_driver):
    ef.registered(
        fake_driver,
        Dict(value='fake_framework_id'),
        'fake_master_info'
    )

    assert ef.driver == fake_driver


def test_reregistered(ef, fake_driver):
    ef.reregistered(
        fake_driver,
        'fake_master_info'
    )


def test_resource_offers_launch(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric,
    mock_time
):
    ef.driver = fake_driver
    ef._last_offer_time = 1.0
    mock_time.return_value = 2.0
    ef.suppress_after = 0.0
    ef.offer_matches_pool = mock.Mock(return_value=(True, None))
    ef_mdl.offer_matches_task_constraints = mock.Mock(return_value=True)
    task_id = fake_task.task_id
    docker_task = Dict(task_id=Dict(value=task_id))
    task_metadata = ef_mdl.TaskMetadata(
        task_config=fake_task,
        task_state='fake_state',
        task_state_history=m(fake_state=time.time())
    )
    ef.get_tasks_to_launch = mock.Mock(return_value=([docker_task], True))

    ef.task_queue.put(fake_task)
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.resourceOffers(ef.driver, [fake_offer])

    assert fake_driver.suppressOffers.call_count == 0
    assert not ef.are_offers_suppressed
    assert fake_driver.declineOffer.call_count == 0
    assert fake_driver.launchTasks.call_count == 1
    assert mock_get_metric.call_count == 2
    mock_get_metric.assert_any_call(ef_mdl.OFFER_DELAY_TIMER)
    mock_get_metric.assert_any_call(ef_mdl.TASK_LAUNCHED_COUNT)
    assert mock_get_metric.return_value.record.call_count == 1
    assert mock_get_metric.return_value.record.call_args == mock.call(1.0)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_resource_offers_launch_tasks_failed(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric,
    mock_time
):
    ef.driver = fake_driver
    ef.driver.launchTasks = mock.Mock(side_effect=socket.timeout)
    ef._last_offer_time = None
    mock_time.return_value = 2.0
    ef.suppress_after = 0.0
    ef.offer_matches_pool = mock.Mock(return_value=(True, None))
    ef_mdl.offer_matches_task_constraints = mock.Mock(return_value=True)
    task_id = fake_task.task_id
    docker_task = Dict(task_id=Dict(value=task_id))
    task_metadata = ef_mdl.TaskMetadata(
        task_config=fake_task,
        task_state='fake_state',
        task_state_history=m(fake_state=time.time())
    )
    ef.get_tasks_to_launch = mock.Mock(return_value=([docker_task], True))
    ef.task_queue.put(fake_task)
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.resourceOffers(ef.driver, [fake_offer])

    assert fake_driver.suppressOffers.call_count == 0
    assert not ef.are_offers_suppressed
    assert fake_driver.declineOffer.call_count == 0
    assert fake_driver.launchTasks.call_count == 1
    assert mock_get_metric.call_count == 1
    assert ef.task_metadata[task_id].task_state == 'UNKNOWN'


def test_get_tasks_to_launch_no_ports(
    ef,
    fake_offer,
    fake_task,
    fake_driver,
    mock_get_metric
):
    ef.create_new_docker_task = mock.Mock()
    ef.get_available_ports = mock.Mock(return_value=[])
    ef.task_queue.put(fake_task)

    tasks, _ = ef.get_tasks_to_launch(fake_offer)

    assert len(tasks) == 0
    assert ef.task_queue.qsize() == 1
    assert ef.create_new_docker_task.call_count == 0


def test_get_tasks_to_launch_ports_available(
    ef,
    fake_offer,
    fake_task,
    fake_driver,
    mock_get_metric
):
    ef.create_new_docker_task = mock.Mock()
    ef.get_available_ports = mock.Mock(return_value=[30000])
    ef.task_queue.put(fake_task)
    task_metadata = ef_mdl.TaskMetadata(
        task_config=fake_task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=time.time())
    )
    ef.task_metadata = ef.task_metadata.set(
        fake_task.task_id,
        task_metadata
    )

    tasks, _ = ef.get_tasks_to_launch(fake_offer)

    assert len(tasks) == 1
    assert ef.task_queue.qsize() == 0
    assert ef.create_new_docker_task.call_count == 1


def test_resource_offers_no_tasks_to_launch(
    ef,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    ef.suppress_after = 0.0

    ef.resourceOffers(fake_driver, [fake_offer])

    assert fake_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert fake_driver.suppressOffers.call_count == 1
    assert ef.are_offers_suppressed
    assert fake_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def test_resource_offers_blacklisted_offer(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    ef.blacklisted_slaves = ef.blacklisted_slaves.append(
        fake_offer.agent_id.value,
    )
    ef.task_queue.put(fake_task)
    ef.resourceOffers(fake_driver, [fake_offer])

    assert fake_driver.declineOffer.call_count == 1
    assert fake_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert fake_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def offers_not_for_pool(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    ef.task_queue.put(fake_task)
    ef.resourceOffers(fake_driver, [fake_offer])

    assert ef.offer_matches_pool.call_count == 1
    assert ef.offer_matches_pool.call_args == mock.call(fake_offer)
    assert fake_driver.declineOffer.call_count == 1
    assert fake_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert fake_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.TASK_INSUFFICIENT_OFFER_COUNT,
    )
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_resource_offers_not_for_pool(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    ef.offer_matches_pool = mock.Mock(return_value=(False, None))

    offers_not_for_pool(
        ef,
        fake_task,
        fake_offer,
        fake_driver,
        mock_get_metric,
    )


def test_resource_offers_not_for_task_pool(
    ef,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    task = me_mdl.MesosTaskConfig(
        name='fake_name',
        cpus=10.0,
        mem=1024.0,
        disk=1000.0,
        gpus=1,
        image='fake_image',
        cmd='echo "fake"',
        pool='fake_other_pool_text',
    )
    task_metadata = ef_mdl.TaskMetadata(
        task_config=task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=time.time()),
    )
    ef.task_metadata = ef.task_metadata.set(task.task_id, task_metadata)

    ef.offer_matches_pool = mock.Mock(return_value=(True, 'fake_pool_text'))

    offers_not_for_pool(
        ef,
        task,
        fake_offer,
        fake_driver,
        mock_get_metric,
    )


def test_resource_offers_not_for_constraints(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric,
):
    ef_mdl.offer_matches_task_constraints = mock.Mock(return_value=False)

    ef.task_queue.put(fake_task)
    ef.resourceOffers(fake_driver, [fake_offer])

    assert ef_mdl.offer_matches_task_constraints.call_count == 1
    assert ef_mdl.offer_matches_task_constraints.call_args == mock.call(
        fake_offer,
        fake_task,
    )
    assert fake_driver.declineOffer.call_count == 1
    assert fake_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter,
    )
    assert fake_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(
        ef_mdl.TASK_INSUFFICIENT_OFFER_COUNT,
    )
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)


def test_resource_offers_unmet_reqs(
    ef,
    fake_task,
    fake_offer,
    fake_driver,
    mock_get_metric
):
    ef.get_tasks_to_launch = mock.Mock(return_value=([], True))

    ef.task_queue.put(fake_task)
    ef.resourceOffers(fake_driver, [fake_offer])

    assert fake_driver.declineOffer.call_count == 1
    assert fake_driver.declineOffer.call_args == mock.call(
        [fake_offer.id],
        ef.offer_decline_filter
    )
    assert fake_driver.launchTasks.call_count == 0
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0


def status_update_test_prep(state, reason=''):
    task = me_mdl.MesosTaskConfig(
        cmd='/bin/true', name='fake_name', image='fake_image')
    task_id = task.task_id
    update = Dict(
        task_id=Dict(value=task_id),
        state=state,
        reason=reason
    )
    task_metadata = ef_mdl.TaskMetadata(
        task_config=task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=time.time()),
    )

    return update, task_id, task_metadata


def test_status_update_record_only(
    ef,
    fake_driver
):
    update, task_id, task_metadata = status_update_test_prep('fake_state1')
    ef.translator = mock.Mock()

    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.statusUpdate(fake_driver, update)

    assert ef.task_metadata[task_id].task_state == 'fake_state1'
    assert len(ef.task_metadata[task_id].task_state_history) == 2
    assert fake_driver.acknowledgeStatusUpdate.call_count == 1
    assert fake_driver.acknowledgeStatusUpdate.call_args == mock.call(update)


def test_status_update_finished(
    ef,
    fake_driver,
    mock_get_metric
):
    # finished task does same thing as other states
    update, task_id, task_metadata = status_update_test_prep('TASK_FINISHED')
    ef.translator = mock.Mock()

    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef.statusUpdate(fake_driver, update)

    assert task_id not in ef.task_metadata
    assert mock_get_metric.call_count == 1
    assert mock_get_metric.call_args == mock.call(ef_mdl.TASK_FINISHED_COUNT)
    assert mock_get_metric.return_value.count.call_count == 1
    assert mock_get_metric.return_value.count.call_args == mock.call(1)
    assert fake_driver.acknowledgeStatusUpdate.call_count == 1
    assert fake_driver.acknowledgeStatusUpdate.call_args == mock.call(update)


def test_ignore_status_update(
    ef,
    fake_driver,
    mock_get_metric
):
    update, task_id, task_metadata = status_update_test_prep('TASK_FINISHED')
    ef.translator = mock.Mock()

    ef.statusUpdate(fake_driver, update)

    assert task_id not in ef.task_metadata
    assert mock_get_metric.call_count == 0
    assert mock_get_metric.return_value.count.call_count == 0
    assert fake_driver.acknowledgeStatusUpdate.call_count == 1


def test_task_lost_due_to_invalid_offers(
    ef,
    fake_driver,
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

    ef.statusUpdate(fake_driver, update)

    assert task_id in ef.task_metadata
    assert mock_get_metric.call_count == 2
    assert ef.event_queue.qsize() == 0
    assert ef.task_queue.qsize() == 1
    assert fake_driver.acknowledgeStatusUpdate.call_count == 1


def test_background_thread_removes_offer_timeout(
    ef,
    fake_driver,
    fake_task,
    mock_time,
    mock_sleep,
):
    mock_time.return_value = 2.0
    task_id = fake_task.task_id
    fake_task = fake_task.set(
        offer_timeout=1
    )
    task_metadata = ef_mdl.TaskMetadata(
        agent_id='fake_agent_id',
        task_config=fake_task,
        task_state='TASK_INITED',
        task_state_history=m(TASK_INITED=0.0),
    )
    ef.driver = fake_driver
    ef.task_metadata = ef.task_metadata.set(task_id, task_metadata)
    ef._background_check()
    assert ef.task_queue.empty()
    assert task_id not in ef.task_metadata.keys()
    assert not ef.event_queue.empty()
    event = ef.event_queue.get(block=False)
    assert event.terminal is True
    assert event.success is False
    assert event.task_id == task_id
