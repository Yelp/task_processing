from queue import Empty
from queue import Full
from threading import Thread

from task_processing.interfaces.runner import Runner


class Subscription(Runner):
    def __init__(self, executor, queue):
        self.executor = executor
        self.TASK_CONFIG_INTERFACE = executor.TASK_CONFIG_INTERFACE
        self.event_queue = queue
        self.stopping = False
        self.producer_t = Thread(target=self.event_producer)
        self.producer_t.daemon = True
        self.producer_t.start()

    def event_producer(self):
        executor_queue = self.executor.get_event_queue()
        while True:
            if self.stopping:
                return
            try:
                event = executor_queue.get(block=True, timeout=1)
                self.event_queue.put(event, False)
            except Empty:
                pass
            except Full:
                pass

    def run(self, task_config):
        return self.executor.run(task_config)

    def kill(self, task_id):
        return self.executor.kill(task_id)

    def reconcile(self, task_config):
        self.executor.reconcile(task_config)

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.producer_t.join()
