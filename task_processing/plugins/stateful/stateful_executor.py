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
        worker = worker_for_event_queue(
            event_queue=self.downstream_executor.get_event_queue(),
            persister=persister,
            on_process=lambda event:
                put_to_outbound_queue(event, self.queue_for_processed_events)
        )
        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()

    def run(self, task_config):
        self.downstream_executor.run(task_config)

    def kill(self, task_id):
        return self.downstream_executor.kill(task_id)

    def status(self, task_id):
        pass

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


def put_to_outbound_queue(event, processed_queue):
    print("processed event {}".format(event))
    processed_queue.put(event)
