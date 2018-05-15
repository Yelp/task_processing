import logging
import threading
import queue

from pymesos import MesosSchedulerDriver

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.execution_framework import (
    ExecutionFramework
)
from task_processing.plugins.mesos.task_config import MesosTaskConfig
from task_processing.plugins.mesos.translator import mesos_status_to_event

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
logging.basicConfig(format=FORMAT)


class MesosExecutor(TaskExecutor):
    TASK_CONFIG_INTERFACE = MesosTaskConfig

    def __init__(
        self,
        role,
        pool=None,
        principal='taskproc',
        secret=None,
        mesos_address='127.0.0.1:5050',
        initial_decline_delay=1.0,
        framework_translator=mesos_status_to_event,
        framework_name='taskproc-default',
        framework_staging_timeout=240,
    ):
        """
        Constructs the instance of a task execution, encapsulating all state
        required to run, monitor and stop the job.

        :param dict credentials: Mesos principal and secret.
        """

        self.logger = logging.getLogger(__name__)

        self.execution_framework = ExecutionFramework(
            role=role,
            pool=pool,
            name=framework_name,
            translator=framework_translator,
            task_staging_timeout_s=framework_staging_timeout,
            initial_decline_delay=initial_decline_delay,
            offer_queue=queue.Queue()
        )

        # TODO: Get mesos master ips from smartstack
        self.driver = MesosSchedulerDriver(
            sched=self.execution_framework,
            framework=self.execution_framework.framework_info,
            use_addict=True,
            master_uri=mesos_address,
            implicit_acknowledgements=False,
            principal=principal,
            secret=secret,
        )

        self.offerQueue = queue.Queue()

        # start driver thread immediately
        self.driver_thread = threading.Thread(
            target=self.driver.run, args=())
        self.driver_thread.daemon = True
        self.driver_thread.start()

    def run(self, task_config):
        self.task_queue
        self.execution_framework.enqueue_task(task_config)

    def kill(self, task_id):
        self.execution_framework.kill_task(task_id)

    def stop(self):
        self.execution_framework.stop()
        self.driver.stop()
        self.driver.join()

    def get_event_queue(self):
        return self.execution_framework.event_queue

    def processOffer(self, allocation_strategy, offers, tasks):
        """
        """
        tasks_to_launch, tasks_to_rehome = allocation_strategy(offers, tasks)
        launches = [
            self.execution_framework.launch_tasks(
                mesos_task = task_to_mesos_task(task_allocation.task, task_allocation.offer.agent_id),
                offer=task_allocation.offer
            ) for task_allocation in tasks_to_launch
        ]

        used_offers = [allocation.offer for allocation in tasks_to_launch]
        rejections = [
            self.execution_framework.reject_offer(offer)
            for offer in set(offers) - set(used_offers)
        ]

        self.tasks_waiting = tasks_to_rehome()

def task_to_mesos_task(task, agent_id):
    # Handle the case of multiple port allocations
    port_to_use = available_ports[0]
    available_ports[:] = available_ports[1:]

    if task_config.containerizer == 'DOCKER':
        container = Dict(
            type='DOCKER',
            volumes=thaw(task_config.volumes),
            docker=Dict(
                image=task_config.image,
                network='BRIDGE',
                port_mappings=[Dict(host_port=port_to_use,
                                    container_port=8888)],
                parameters=thaw(task_config.docker_parameters),
                force_pull_image=True,
            ),
        )
    elif task_config.containerizer == 'MESOS':
        container = Dict(
            type='MESOS',
            # for docker, volumes should include parameters
            volumes=thaw(task_config.volumes),
            network_infos=Dict(
                port_mappings=[Dict(host_port=port_to_use,
                                    container_port=8888)],
            ),
        )
        # For this to work, image_providers needs to be set to 'docker'
        # on mesos agents
        if 'image' in task_config:
            container.mesos = Dict(
                image=Dict(
                    type='DOCKER',
                    docker=Dict(name=task_config.image),
                )
            )

    return Dict(
        task_id=Dict(value=task_config.task_id),
        agent_id=Dict(value=agent_id.value),
        name='executor-{id}'.format(id=task_config.task_id),
        resources=[
            Dict(name='cpus',
                 type='SCALAR',
                 role=self.role,
                 scalar=Dict(value=task_config.cpus)),
            Dict(name='mem',
                 type='SCALAR',
                 role=self.role,
                 scalar=Dict(value=task_config.mem)),
            Dict(name='disk',
                 type='SCALAR',
                 role=self.role,
                 scalar=Dict(value=task_config.disk)),
            Dict(name='gpus',
                 type='SCALAR',
                 role=self.role,
                 scalar=Dict(value=task_config.gpus)),
            Dict(name='ports',
                 type='RANGES',
                 role=self.role,
                 ranges=Dict(
                     range=[Dict(begin=port_to_use, end=port_to_use)]))
        ],
        command=Dict(
            value=task_config.cmd,
            uris=[
                Dict(value=uri, extract=False)
                for uri in task_config.uris
            ],
            environment=Dict(variables=[
                Dict(name=k, value=v) for k, v in
                task_config.environment.items()
            ])
        ),
        container=container
    )


