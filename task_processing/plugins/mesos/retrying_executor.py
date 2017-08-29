import logging
import time
from operator import sub
from threading import RLock
from threading import Thread

from pyrsistent import m
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor

log = logging.getLogger(__name__)

TASK_ID_SEPARATOR = ':'


class RetryingExecutor(TaskExecutor):
    def __init__(self,
                 executor,
                 retry_pred=lambda e: not e.success,
                 retries=3):
        self.executor = executor
        self.retries = retries
        self.retry_pred = retry_pred

        self.task_retries = m()
        self.task_retries_lock = RLock()

        self.src_queue = executor.get_event_queue()
        self.dest_queue = Queue()
        self.stopping = False

        self.retry_thread = Thread(target=self.retry_loop)
        self.retry_thread.daemon = True
        self.retry_thread.start()

    def event_with_retries(self, event):
        return event.transform(
            ('extensions', 'RetryingExecutor/tries'),
            "{}/{}".format(
                1 + self.retries - self.task_retries.get(event.task_id, -1),
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

        with self.task_retries_lock:
            self.task_retries = self.task_retries.update_with(
                sub, {event.task_id: 1}
            )
            self.run(event.task_config, event.task_id)

        return True

    def retry_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()

                if e.kind != 'task':
                    self.dest_queue.put(e)
                    continue

                # shrink the id so layers up the stack can recognize it
                shrink_id, attempt = self.task_id_shrink(e.task_id)
                current_attempt = self.retries - self.task_retries[shrink_id]

                if attempt != current_attempt:
                    log.info('Event from previous attempt, ignoring')
                    continue

                e = self.event_with_retries(e.set(task_id=shrink_id))

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

    def run(self, task_config, task_id):
        if task_id not in self.task_retries:
            with self.task_retries_lock:
                self.task_retries = self.task_retries.set(
                    task_id, self.retries)

        self.executor.run(task_config, self.task_id_grow(task_id))

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

    def task_id_split(self, task_id):
        return task_id.split(TASK_ID_SEPARATOR)

    def task_id_join(self, task_id):
        return TASK_ID_SEPARATOR.join(task_id)

    def task_id_grow(self, task_id):
        components = self.task_id_split(task_id)
        components.append(str(self.retries - self.task_retries[task_id]))
        return self.task_id_join(components)

    def task_id_shrink(self, task_id):
        components = self.task_id_split(task_id)
        return self.task_id_join(components[0:-1]), int(components[-1])
