import logging
import threading
import traceback
from queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor

log = logging.getLogger(__name__)


class StatefulTaskExecutor(TaskExecutor):
    """
    """

    def __init__(self, downstream_executor, persister):
        self.downstream_executor = downstream_executor
        self.writer_queue = Queue()
        self.queue_for_processed_events = Queue()
        self.persister = persister
        worker_thread = threading.Thread(
            target=self.subscribe_to_updates_for_task
        )
        worker_thread.daemon = True
        worker_thread.start()

    def run(self, task_config):
        self.downstream_executor.run(task_config)

    def reconcile(self, task_config):
        self.downstream_executor.reconcile(task_config)

    def kill(self, task_id):
        return self.downstream_executor.kill(task_id)

    def status(self, task_id):
        return sorted(
            self.persister.read(task_id),
            key=lambda x: x['timestamp']
        )

    def stop(self):
        return self.downstream_executor.stop()

    def get_event_queue(self):
        return self.queue_for_processed_events

    def subscribe_to_updates_for_task(self):
        while True:
            result = self.downstream_executor.get_event_queue().get()
            try:
                self.persister.write(event=result)
            except Exception:
                log.error(traceback.format_exc())
            self.queue_for_processed_events.put(result)
            self.downstream_executor.get_event_queue().task_done()
