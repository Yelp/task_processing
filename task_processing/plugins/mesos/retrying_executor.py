import logging
import time
from operator import sub
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
        self.stopping = False

        self.retry_thread = Thread(target=self.retry_loop)
        self.retry_thread.daemon = True
        self.retry_thread.start()

    def event_with_retries(self, event):
        return event.transform(
            ('extensions', 'RetryingExecutor/tries'),
            "{}/{}".format(
                self.task_retries[event.task_id],
                self.retries
            )
        )

    def retry(self, event):
        current_retry_attempt = self.task_retries[event.task_id]

        if current_retry_attempt == self.retries:
            return False

        log.info(
            'Retrying task {}, {} of {}, fail event: {}'.format(
                event.task_config.name, current_retry_attempt,
                self.retries, event.raw
            )
        )

        with self.task_retries_lock:
            self.task_retries = self.task_retries.set(
                event.task_id,
                current_retry_attempt + 1
            )
        self.run(event.task_config)

        return True

    def retry_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()
                # This is to remove trailing '-retry*'
                original_task_id = '-'.join([item for item in
                                             e.task_id.split('-')[:-1]])

                # Check if the update is for current attempt. Discard if
                # it is not.
                if not self._is_current_attempt(e, original_task_id):
                    continue

                # Set the task id back to original task_id
                e = self._restore_task_id(e, original_task_id)

                if e.kind != 'task':
                    self.dest_queue.put(e)
                    continue

                e = self.event_with_retries(e)

                if e.terminal:
                    if self.retry_pred(e):
                        if self.retry(e):
                            continue

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
                    task_config.task_id, 1)
        self.executor.run(self._task_config_with_retry(task_config))

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

    def _task_config_with_retry(self, task_config):
        return task_config.set(uuid='{id}-retry{attempt}'.format(
            id=task_config.uuid,
            attempt=self.task_retries[task_config.task_id]
        ))

    def _restore_task_id(self, e, original_task_id):
        # Fix task_id references
        mesos_status = e.raw
        mesos_status.task_id.value = original_task_id
        mesos_status.executor_id.value = original_task_id

        task_config = e.task_config.set(uuid='-'.join(
            [item for item in e.task_config.uuid.split('-')[:-1]]
        ))

        # Set the task id back to original task_id
        return e.set(
            task_id=original_task_id,
            task_config=task_config,
            raw=mesos_status
        )

    def _is_current_attempt(self, e, original_task_id):
        retry_suffix = '-'.join([item for item in
                                 e.task_id.split('-')[-1:]])

        # This is to extract retry attempt from retry_suffix
        # eg: if retry_suffix= 'retry2', then attempt==2
        attempt = int(retry_suffix[5:])
        if attempt == self.task_retries[original_task_id]:
            return True
        return False
