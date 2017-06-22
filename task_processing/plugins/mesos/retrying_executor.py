import logging
import time
from operator import sub
from threading import Lock
from threading import Thread

from pyrsistent import m
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s'
LEVEL = logging.DEBUG
logging.basicConfig(format=FORMAT, level=LEVEL)
log = logging.getLogger(__name__)


class RetryingExecutor(TaskExecutor):
    def __init__(self,
                 executor,
                 retry_pred=lambda e: not e.success,
                 retries=3):
        self.executor = executor
        self.retries = retries
        self.retry_pred = retry_pred

        self.task_retries = m()
        self.task_retries_lock = Lock()

        self.src_queue = executor.get_event_queue()
        self.dest_queue = Queue()
        self.stopping = False
        self.TASK_CONFIG_INTERFACE = executor.TASK_CONFIG_INTERFACE

        self.retry_thread = Thread(target=self.retry_loop)
        self.retry_thread.start()

    def event_with_retries(self, event):
        return event.transform(
            ('extensions', 'RetryingExecutor/tries'),
            "{}/{}".format(
                self.retries - self.task_retries.get(event.task_id, -1),
                self.retries
            )
        )

    def retry(self, event):
        current_retries = self.task_retries.get(event.task_id, -1)
        if current_retries <= 0:
            return False

        log.info(
            'Retrying task {}, {} of {}, fail event: {}'.format(
                event.task_config.name, 1 + self.retries - current_retries,
                self.retries, event.raw
            )
        )

        self.run(event.task_config)
        with self.task_retries_lock:
            self.task_retries = self.task_retries.update_with(
                sub, {event.task_id: 1}
            )
        return True

    def retry_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.event_with_retries(self.src_queue.get())

                if e.terminal:
                    if self.retry_pred(e):
                        if self.retry(e):
                            continue
                    else:
                        with self.task_retries_lock:
                            self.task_retries = \
                                self.task_retries.remove(e.task_id)

                self.dest_queue.put(e)

            if self.stopping:
                return

            time.sleep(1)

    def run(self, task_config):
        if task_config.task_id not in self.task_retries:
            with self.task_retries_lock:
                self.task_retries = self.task_retries.set(
                    task_config.task_id, self.retries)
        self.executor.run(task_config)

    def kill(self, task_id):
        # retries = -1 so that manually killed tasks can be distinguished
        with self.task_retries_lock:
            self.tasks_retries = self.task_retries.update_with(
                sub, {task_id: 1})
        self.executor.kill(task_id)

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.retry_thread.join()

    def get_event_queue(self):
        return self.dest_queue
