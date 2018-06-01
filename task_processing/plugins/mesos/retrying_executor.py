import logging
import time
from threading import Lock
from threading import Thread

from pyrsistent import m
from pyrsistent import v
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.plugins.mesos.utils import is_pod

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
        log.warning(
            'Retrying task {}, {} of {} '.format(
                event.task_config.name, total_retries - retries_remaining + 1,
                total_retries
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

                original_task_id = self._remove_retry_suffix(e.task_id)

                # Check if the update is for current attempt. Discard if
                # it is not.
                if not self._is_current_attempt(e, original_task_id):
                    continue

                # Set the task id back to original task_id
                e = self._restore_task_id(e, original_task_id)

                if e.kind not in ('task', 'pod'):
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
                    task_config.task_id,
                    self._task_or_executor_retries(task_config)
                )

        if is_pod(task_config):
            self.executor.run(self._task_config_with_retry(task_config))
        else:
            self.executor.run(self._task_config_with_retry(task_config))

    def kill(self, task_id):
        # retries = -1 so that manually killed tasks can be distinguished
        with self.task_retries_lock:
            self.tasks_retries = self.task_retries.set(
                task_id,
                -1
            )
        self.executor.kill(task_id)

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.retry_thread.join()

    def get_event_queue(self):
        return self.dest_queue

    def _task_config_with_retry(self, task_config):
        if not is_pod(task_config):
            return task_config.set(uuid='{id}-retry{attempt}'.format(
                id=task_config.uuid,
                attempt=self.task_retries[task_config.task_id]
            ))
        else:
            attempt = self.task_retries[task_config.task_id]
            tmp_list = v()
            for task in task_config.tasks:
                tmp_list = tmp_list.append(
                    task.set(uuid='{id}-retry{attempt}'.format(
                        id=task_config.uuid,
                        attempt=attempt
                    ))
                )
            task_config = task_config.set(
                uuid='{id}-retry{attempt}'.format(
                    id=task_config.uuid,
                    attempt=attempt
                ),
                tasks=tmp_list
            )
            return task_config

    def _restore_task_id(self, e, original_task_id):
        if e.kind is 'pod':
            # Fix the uuid of the nested containers
            tmp_task_config_map = m()
            tmp_mesos_status_map = m()
            for task_id in e.task_config_per_task_id:
                task_config = e.task_config_per_task_id[task_id]
                task_config = task_config.set(
                    uuid='-'.join(
                        [item for item in
                         str(task_config.uuid).split('-')[:-1]]
                    )
                )
                tmp_task_config_map = tmp_task_config_map.set(
                    task_config.task_id,
                    task_config
                )

                status_update = e.mesos_status_per_task_id[task_id]
                status_update['task_id'] = task_config.task_id
                tmp_mesos_status_map = tmp_mesos_status_map.set(
                    status_update.task_id,
                    status_update
                )

            task_config = e.task_config
            task_config = task_config.set(
                uuid='-'.join(
                    [item for item in
                     str(task_config.uuid).split('-')[:-1]]
                )
            )

            e = e.set(
                task_id=original_task_id,
                task_config_per_task_id=tmp_task_config_map,
                mesos_status_per_task_id=tmp_mesos_status_map,
                task_config=task_config
            )

            return e

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
        if attempt == self.task_retries[original_task_id]:
            return True
        return False

    def _task_or_executor_retries(self, task_config):
        return task_config.retries \
            if 'retries' in task_config else self.retries

    def _remove_retry_suffix(self, task_id):
        return '-'.join([item for item in
                         task_id.split('-')[:-1]])
