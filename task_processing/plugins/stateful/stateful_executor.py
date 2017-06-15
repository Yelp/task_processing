from task_processing.interfaces.task_executor import TaskExecutor
from six.moves.queue import Queue
import threading


class StatefulTaskExecutor(TaskExecutor):
    """
    """

    def __init__(self, downstream_executor, persister):
        self.downstream_executor = downstream_executor
        self.writer_queue = Queue()
        worker = worker_for_event_queue(
            event_queue=self.event_queue(),
            persister=persister
        )
        worker_thread = threading.Thread(target=worker)
        worker_thread.start()

    def run(self, task_config):
        task_id = task_config.task_id
        self.downstream_executor.run(task_config)

    def kill(self, task_id):
        return self.downstream_executor.kill(task_id)

    def status(self, task_id):
        pass

    def event_queue(self):
        return self.downstream_executor.event_queue()



def worker_for_event_queue(event_queue, persister):
    def subscribe_to_updates_for_task():
        while True:
            result = event_queue.get()
            persister.write(event=result)
            event_queue.task_done()

    return subscribe_to_updates_for_task
