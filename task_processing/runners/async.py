import logging
import os
import traceback
from collections import namedtuple
from queue import Empty
from threading import Thread

from task_processing.interfaces.runner import Runner

EventHandler = namedtuple('EventHandler', ['predicate', 'cb'])

log = logging.getLogger(__name__)


class AsyncError(Exception):
    pass


class Async(Runner):
    # TODO: "callbacks" is inconsistent with the EventHandler terminology
    # above. This should either be event_handlers, or
    # EventHandler should be Callback
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

    def kill(self, task_id):
        return self.executor.kill(task_id)

    def reconcile(self, task_config):
        self.executor.reconcile(task_config)

    def callback_loop(self):
        event_queue = self.executor.get_event_queue()

        while True:
            if self.stopping:
                return

            try:
                event = event_queue.get(True, 10)

                # TODO: have a default callback? raise exception when this
                # event is ignored?
                if event.kind == 'control' and \
                   event.message == 'stop':
                    self.stopping = True
                    continue

                for cb in self.callbacks:
                    if cb.predicate(event):
                        try:
                            cb.cb(event)
                        except Exception:
                            log.error(traceback.format_exc())
                            os._exit(1)
            except Empty:
                pass

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.callback_t.join()
