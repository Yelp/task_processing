import logging
import time
from threading import Event
from threading import Lock
from threading import Thread

from pyrsistent import m
from six.moves.queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor
from task_processing.vendor.mesos_cli.task_executor import MesosMaster
from task_processing.vendor.mesos_cli.task_executor import NewConfig

log = logging.getLogger(__name__)


class LogFetcherExecutor(TaskExecutor):
    def __init__(self, executor):
        self.executor = executor
        self._log_fetcher_queue = Queue()
        self._task_id_thread_mapping = {}
        # This lock is to prevent multiple threads from reading logs
        # simultaneously.
        self._lock = threading.RLock()
        self.src_queue = executor.get_event_queue()
        self.dest_queue = Queue()
        self.stopping = False

    def _fetch_logs(self, task_id, event):
        # Wait for the main thread to signal us to start
        event.wait()

        # Clear the event
        event.clear()

        c = NewConfig(address=os.environ.get('MESOS_MASTER_ADDRESS'))
        master = MesosMaster(c)
        task = master.task(task_id)
        stderr = task.file(os.path.join(task.executor['directory'], 'stderr'))
        stdout = task.file(os.path.join(task.executor['directory'], 'stdout'))

        # reset the file offsets to 0
        stderr.seek(0)
        stdout.seek(0)

        # Start reading the files and dump the output to stdout and stderr
        while True:
            # Wait for the framework to signal us for the task completion
            if event.is_set is True:
                event.clear()
                return

            for f in (stderr, stdout):
                for line in f.readlines(500):
                    print(line, file=sys.stderr if f is stderr else sys.stdout)

            time.sleep(1)

    def retry_loop(self):
        event = Event()
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()
                # Start the log reader thread only if we receive TASK_RUNNING
                if e.event.platform_type == 'starting':
                    Thread(target=self._fetch_logs,
                           args=(e.task_id, event)).start()

                self.dest_queue.put(e)

            if self.stopping:
                return

            time.sleep(1)

    def run(self, task_config):
        self.executor.run(task_config)

    def kill(self, task_id):
        self.executor.kill(task_id)

    def stop(self):
        self.executor.stop()
        self.stopping = True
        self.retry_thread.join()

    def get_event_queue(self):
        return self.dest_queue
