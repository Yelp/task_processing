from collections import namedtuple
from threading import Thread

from six.moves.queue import Empty

from task_processing.interfaces.runner import Runner

EventHandler = namedtuple('EventHandler', ['predicate', 'cb'])


class AsyncError(Exception):
    pass


class Async(Runner):
    def __init__(self, executor, callbacks=None):
        if not callbacks:
            raise AsyncError("must provide at least one callback")

        self.callbacks = callbacks
        self.executor = executor
        self.TASK_CONFIG_INTERFACE = executor.TASK_CONFIG_INTERFACE
        self.stopping = False

        self.callback_t = Thread(target=self.callback_loop)
        self.callback_t.daemon = True
        self.callback_t.start()

    def run(self, task_config):
        return self.executor.run(task_config)

    def kill(self, task_config):
        pass

    def callback_loop(self):
        event_queue = self.executor.get_event_queue()

        while True:
            if self.stopping:
                return

            try:
                event = event_queue.get(True, 10)

                for cb in self.callbacks:
                    if cb.predicate(event):
                        cb.cb(event)
            except Empty:
                pass

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.callback_t.join()
