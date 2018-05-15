from collections import namedtuple

TaskAllocation = namedtuple('TaskAllocation', [offer, task])


def greedy_allocation(offers, tasks):
    """Implements a greedy algorithm for packing tasks into offers.
    Given a list of tasks, the resources in the offer will be used on a first-come-first-serve
    basis by those tasks in the list
    """
    tasks_to_launch = {}
    tasks_without_home = []

    for offer in offers:
        tasks_to_launch, tasks_to_put_back_in_queue = _greedy_fit_tasks_to_offer(
            offer, tasks)
        tasks_without_home = tasks_to_put_back_in_queue
        tasks_to_launch + [TaskAllocation(offer, task)
                           for task in tasks_to_launch]

    return tasks_to_launch, task_without_home


def _greedy_fit_tasks_to_offer(role, offer, tasks):
    tasks_to_launch = []
    tasks_to_put_back_in_queue = []

    remaining_cpus = 0
    remaining_mem = 0
    remaining_disk = 0
    remaining_gpus = 0
    available_ports = []
    for resource in offer.resources:
        if resource.name == "cpus" and resource.role == role:
            remaining_cpus += resource.scalar.value
        elif resource.name == "mem" and resource.role == role:
            remaining_mem += resource.scalar.value
        elif resource.name == "disk" and resource.role == role:
            remaining_disk += resource.scalar.value
        elif resource.name == "gpus" and resource.role == role:
            remaining_gpus += resource.scalar.value
        elif resource.name == "ports" and resource.role == role:
            # TODO: Validate if the ports available > ports required
            available_ports = self.get_available_ports(resource)

    log.info(
        "Received offer {id} with cpus: {cpu}, mem: {mem}, "
        "disk: {disk} gpus: {gpu} role: {role}".format(
            id=offer.id.value,
            cpu=remaining_cpus,
            mem=remaining_mem,
            disk=remaining_disk,
            gpu=remaining_gpus,
            role=self.role
        )
    )

    tasks_to_put_back_in_queue = []

    # Get all the tasks of the queue
    while not task_queue.empty():
        task = self.task_queue.get()

        if ((remaining_cpus >= task.cpus and
             remaining_mem >= task.mem and
             remaining_disk >= task.disk and
             remaining_gpus >= task.gpus and
             len(available_ports) > 0 and
             offer_matches_task_constraints(offer, task))):
            # This offer is sufficient for us to launch task
            tasks_to_launch.append(
                self.create_new_docker_task(
                    offer,
                    task,
                    available_ports
                )
            )

            # Deduct the resources taken by this task from the total
            # available resources.
            remaining_cpus -= task.cpus
            remaining_mem -= task.mem
            remaining_disk -= task.disk
            remaining_gpus -= task.gpus

            md = self.task_metadata[task.task_id]
            get_metric(TASK_QUEUED_TIME_TIMER).record(
                time.time() - md.task_state_history['TASK_INITED']
            )
        else:
            # This offer is insufficient for this task. We need to put
            # it back in the queue
            tasks_to_put_back_in_queue.append(task)
    return tasks_to_launch
