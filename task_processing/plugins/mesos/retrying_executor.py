import time
from threading import Thread

from pyrsistent import m
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor


class RetryingExecutor(TaskExecutor):
    def __init__(self,
                 executor,
                 retry_pred=lambda e: not e.success,
                 retries=3):
        self.executor = executor
        self.retries = retries
        self.retry_pred = retry_pred
        self.task_retries = m()
        self.src_queue = executor.get_event_queue()
        self.dest_queue = Queue()
        self.retry_thread = Thread(target=self.retry_loop)
        self.retry_thread.start()
        self.stopping = False
        self.TASK_CONFIG_INTERFACE = executor.TASK_CONFIG_INTERFACE

    def retry_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()
                if e.terminal and self.retry_pred(e):
                    if self.task_retries[e.task_id] > 0:
                        self.run(e.task_config)
                        self.task_retries = self.task_retries.set(
                            e.task_id, self.task_retries[e.task_id] - 1)
                        continue
                et = e.transform(
                    ('extensions', 'retrying'),
                    self.retries - self.task_retries[e.task_id])
                self.dest_queue.put(et)

            if self.stopping:
                return

            time.sleep(1)

    def run(self, task_config):
        self.tasks_retries[task_config.task_id] = self.retries
        self.executor.run(task_config)

    def kill(self, task_id):
        # retries = -1 so that manually killed tasks can be distinguished
        self.tasks_retries = self.task_retries.set(task_id, -1)
        self.executor.kill(task_id)

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.retry_thread.join()

    def get_event_queue(self):
        return self.dest_queue
