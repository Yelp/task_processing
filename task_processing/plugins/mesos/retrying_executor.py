import logging
from threading import Lock
from threading import Thread

from pyrsistent import m
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor

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

        self.retry_thread = Thread(target=self.retry_loop)
        self.retry_thread.daemon = True
        self.retry_thread.start()

    def event_with_retries(self, event):
        return event.transform(
            ('extensions', 'RetryingExecutor/try'),
            self.task_retries.get(event.task_id, 0)
        )

    def retry(self, event):
        task_id = event.task_id
        current_retry = self.task_retries.get(task_id, 0) + 1
        if current_retry > self.retries:
            log.info('failing {}, maximum retries {} of {} reached'.format(
                task_id, current_retry, self.retries
            ))
            return False

        log.info('retrying {}, {} of {}, fail event: {}'.format(
            task_id, current_retry, self.retries, event.raw
        ))

        self.run(event.task_config)

        with self.task_retries_lock:
            self.task_retries = self.task_retries.set(task_id, current_retry)

        return True

    def retry_loop(self):
        while True:
            e = self.src_queue.get()

            if e.kind != 'task':
                self.dest_queue.put(e)
                continue

            e = self.event_with_retries(e)

            if e.terminal:
                if self.retry_pred(e) and self.retry(e):
                    continue

                with self.task_retries_lock:
                    self.task_retries = self.task_retries.remove(e.task_id)

            self.dest_queue.put(e)

    def run(self, task_config):
        with self.task_retries_lock:
            if task_config.task_id not in self.task_retries:
                self.task_retries = self.task_retries.set(
                    task_config.task_id, 0)

        return self.executor.run(task_config)

    def kill(self, task_id):
        # retries = max+1 so that manually killed tasks can be distinguished
        with self.task_retries_lock:
            self.tasks_retries = self.task_retries.set(
                task_id, self.retries + 1)

        return self.executor.kill(task_id)

    def stop(self):
        return self.executor.stop()

    def get_event_queue(self):
        return self.dest_queue
