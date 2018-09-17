import logging
import time
from queue import Queue
from threading import Lock
from threading import Thread

from pyrsistent import m

from task_processing.interfaces.task_executor import TaskExecutor

log = logging.getLogger(__name__)


class RetryingExecutor(TaskExecutor):
    def __init__(self,
                 downstream_executor,
                 retry_pred=lambda e: not e.success,
                 retries=3):
        self.executor = downstream_executor
        self.retries = retries
        self.retry_pred = retry_pred

        self.task_retries = m()
        self.task_retries_lock = Lock()

        self.src_queue = downstream_executor.get_event_queue()
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
        retries_remaining = self.task_retries[event.task_id]
        if retries_remaining <= 0:
            return False

        total_retries = self._task_or_executor_retries(event.task_config)
        log.info(
            'Retrying task {}, {} of {}, fail event: {}'.format(
                event.task_config.name, total_retries - retries_remaining + 1,
                total_retries, event.raw
            )
        )

        with self.task_retries_lock:
            self.task_retries = self.task_retries.set(
                event.task_id,
                retries_remaining - 1
            )
        self.run(event.task_config)

        return True

    def retry_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()

                if e.kind != 'task':
                    self.dest_queue.put(e)
                    continue

                # This is to remove trailing '-retry*'
                original_task_id = '-'.join([item for item in
                                             e.task_id.split('-')[:-1]])

                # Check if the update is for current attempt. Discard if
                # it is not.
                if not self._is_current_attempt(e, original_task_id):
                    continue

                # Set the task id back to original task_id
                e = self._restore_task_id(e, original_task_id)

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
                    task_config.task_id,
                    self._task_or_executor_retries(task_config)
                )
        self.executor.run(self._task_config_with_retry(task_config))

    def reconcile(self, task_config):
        self.executor.reconcile(task_config)

    def kill(self, task_id):
        # retries = -1 so that manually killed tasks can be distinguished
        with self.task_retries_lock:
            self.task_retries = self.task_retries.set(
                task_id,
                -1
            )
        return self.executor.kill(task_id)

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
        task_config = e.task_config.set(uuid='-'.join(
            [item for item in str(e.task_config.uuid).split('-')[:-1]]
        ))

        # Set the task id back to original task_id
        return e.set(
            task_id=original_task_id,
            task_config=task_config,
        )

    def _is_current_attempt(self, e, original_task_id):
        retry_suffix = '-'.join([item for item in
                                 e.task_id.split('-')[-1:]])

        # This is to extract retry attempt from retry_suffix
        # eg: if retry_suffix= 'retry2', then attempt==2
        attempt = int(retry_suffix[5:])

        # This is to reregister a task with the retry executor in the event
        # of reconciliation and attempts were lost
        with self.task_retries_lock:
            if original_task_id not in self.task_retries:
                self.task_retries = self.task_retries.set(
                    original_task_id,
                    attempt,
                )
                return True

        if attempt == self.task_retries[original_task_id]:
            return True

        return False

    def _task_or_executor_retries(self, task_config):
        return task_config.retries \
            if 'retries' in task_config else self.retries
