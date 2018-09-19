import collections
import logging
import time
from queue import Queue
from threading import Lock
from threading import Thread

from task_processing.interfaces.task_executor import TaskExecutor

log = logging.getLogger(__name__)

TaskEntry = collections.namedtuple('TaskEntry', ['task_id', 'deadline'])


class TimeoutExecutor(TaskExecutor):
    def __init__(self, downstream_executor):
        self.downstream_executor = downstream_executor

        self.tasks_lock = Lock()
        # Tasks that are pending termination
        self.killed_tasks = []
        # Tasks that are currently running
        self.running_tasks = []

        self.src_queue = downstream_executor.get_event_queue()
        self.dest_queue = Queue()
        self.stopping = False

        self.timeout_thread = Thread(target=self.timeout_loop)
        self.timeout_thread.daemon = True
        self.timeout_thread.start()

    def timeout_loop(self):
        while True:
            # process downstream events
            while not self.src_queue.empty():
                e = self.src_queue.get()
                self.dest_queue.put(e)

                if not e.kind == 'task':
                    continue
                elif not e.terminal:
                    with self.tasks_lock:
                        if e.task_id not in [entry.task_id for entry in self.running_tasks]:
                            # No record of e's task_id in self.running_tasks,
                            # so we need to add it back in. We lack access to
                            # the original time the task was started, so to set
                            # a deadline, we use e's timestamp as a baseline.
                            new_entry = TaskEntry(
                                task_id=e.task_id,
                                deadline=e.task_config.timeout + e.timestamp,
                            )
                            self._insert_new_running_task_entry(new_entry)
                else:
                    # Update running and killed tasks
                    with self.tasks_lock:
                        for idx, entry in enumerate(self.running_tasks):
                            if e.task_id == entry.task_id:
                                self.running_tasks.pop(idx)
                                break
                        if e.task_id in self.killed_tasks:
                            self.killed_tasks.remove(e.task_id)

            # Check timeouts
            current_time = time.time()
            with self.tasks_lock:
                delete_idx = None
                for idx, entry in enumerate(self.running_tasks):
                    if entry.deadline < current_time:
                        log.info(
                            'Killing task {}: timed out'.format(entry.task_id))
                        self.downstream_executor.kill(entry.task_id)
                        self.killed_tasks.append(entry.task_id)
                        delete_idx = idx
                    # Skip the rest of tasks in the list because they are
                    # appended to the list later.
                    else:
                        break
                if delete_idx is not None:
                    self.running_tasks = self.running_tasks[delete_idx + 1:]

            if self.stopping:
                return

            # Since src_queue has to be polled continuously, sleep(1) is used.
            # Otherwise, a notify() from run() plus wait(delta between now and
            # the earliest deadline) is more efficient.
            time.sleep(1)

    def run(self, task_config):
        # Tasks are dynamically added and removed from running_tasks and
        # and killed_tasks. It's preferable for the client or execution
        # framework to check for duplicated tasks. The duplicate task check does
        # NOT happen here.
        new_entry = TaskEntry(
            task_id=task_config.task_id,
            deadline=task_config.timeout + time.time()
        )
        with self.tasks_lock:
            self._insert_new_running_task_entry(new_entry)

        self.downstream_executor.run(task_config)

    def reconcile(self, task_config):
        self.downstream_executor.reconcile(task_config)

    def kill(self, task_id):
        with self.tasks_lock:
            for idx, entry in enumerate(self.running_tasks):
                if task_id == entry.task_id:
                    log.info('Killing task {}: requested'.format(task_id))
                    result = self.downstream_executor.kill(task_id)
                    if result is not False:
                        self.running_tasks.pop(idx)
                        self.killed_tasks.append(task_id)
                    return result

    def stop(self):
        self.downstream_executor.stop()
        self.stopping = True
        self.timeout_thread.join()

    def get_event_queue(self):
        return self.dest_queue

    def _insert_new_running_task_entry(self, new_entry):
        # Insertion sort for task entries in self.running_tasks
        for idx, entry in enumerate(self.running_tasks):
            if new_entry.deadline <= entry.deadline:
                self.running_tasks.insert(idx, new_entry)
                return
        self.running_tasks.append(new_entry)
