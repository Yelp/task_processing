import threading

from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor


class StatefulTaskExecutor(TaskExecutor):
    """
    """

    def __init__(self, downstream_executor, persister):
        self.downstream_executor = downstream_executor
        self.writer_queue = Queue()
        self.queue_for_processed_events = Queue()
        self.persister = persister
        worker = worker_for_event_queue(
            event_queue=self.downstream_executor.get_event_queue(),
            persister=persister,
            on_process=lambda event:
                self.queue_for_processed_events.put(event)
        )
        worker_thread = threading.Thread(target=worker)
        worker_thread.daemon = True
        worker_thread.start()

    def run(self, task_config):
        self.downstream_executor.run(task_config)

    def kill(self, task_id):
        return self.downstream_executor.kill(task_id)

    def status(self, task_id):
        return sorted(self.persister.read(task_id), key=lambda x: x.timestamp)

    def stop(self):
        return self.downstream_executor.stop()

    def get_event_queue(self):
        return self.queue_for_processed_events


def worker_for_event_queue(event_queue, persister, on_process):
    def subscribe_to_updates_for_task():
        while True:
            result = event_queue.get()
            persister.write(event=result)
            on_process(result)
            event_queue.task_done()

    return subscribe_to_updates_for_task
