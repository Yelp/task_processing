import logging
import sys
import time
from queue import Queue
from threading import Lock
from threading import Thread
from urllib.parse import urlparse

import requests
from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord
from pyrsistent import v

from task_processing.interfaces.task_executor import TaskExecutor


log = logging.getLogger(__name__)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Read task log in 4K chunks
TASK_LOG_CHUNK_LEN = 4096
DEFAULT_FORMAT = '{task_id}[{container_id}@{agent}]: {line}'


class LogMetadata(PRecord):
    log_url = field(type=str, initial='')
    log_path = field(type=str, initial='')
    log_offsets = field(type=PMap,
                        factory=pmap,
                        initial=pmap({'stdout': 0, 'stderr': 0}))
    container_id = field(type=str, initial='')
    executor_id = field(type=str, initial='')


def standard_handler(task_id, message, stream):
    print(message, file=sys.stderr if stream is 'stderr' else sys.stdout)


class MesosLoggingExecutor(TaskExecutor):
    def __init__(
        self,
        downstream_executor,
        handler=standard_handler,
        format_string=DEFAULT_FORMAT,
    ):
        self.downstream_executor = downstream_executor
        self.TASK_CONFIG_INTERFACE = downstream_executor.TASK_CONFIG_INTERFACE
        self.handler = handler
        self.format_string = format_string

        self.src_queue = downstream_executor.get_event_queue()
        self.dest_queue = Queue()
        self.stopping = False

        self.staging_tasks = m()
        self.running_tasks = m()
        self.done_tasks = v()

        # A lock is needed to synchronize logging and event processing
        self.task_lock = Lock()

        self.event_thread = Thread(target=self.event_loop)
        self.event_thread.daemon = True
        self.event_thread.start()

        self.logging_thread = Thread(target=self.logging_loop)
        self.logging_thread.daemon = True
        self.logging_thread.start()

    def log_line(self, stream, line, task_id, container_id, agent):
        formatted_line = self.format_string.format(
            task_id=task_id,
            container_id=container_id,
            agent=agent,
            line=line,
        )
        self.handler(task_id, formatted_line, stream)

    def set_task_log_path(self, task_id):
        log_md = self.running_tasks[task_id]
        try:
            response = requests.get(log_md.log_url + '/files/debug').json()
        except Exception as e:
            log.error("Failed to fetch files {error}".format(error=e))
            return

        for key in response.keys():
            if log_md.executor_id in key and log_md.container_id in key:
                with self.task_lock:
                    self.running_tasks = self.running_tasks.set(
                        task_id,
                        log_md.set(log_path=key),
                    )
                break

    def stream_task_log(self, task_id):
        if self.running_tasks[task_id].log_path == "":
            self.set_task_log_path(task_id)

        # Abort in case the log path discovery was not successful
        log_md = self.running_tasks[task_id]
        if log_md.log_path == "":
            return

        offsets = {
            'stdout': log_md.log_offsets['stdout'],
            'stderr': log_md.log_offsets['stderr']
        }
        agent = urlparse(log_md.log_url).hostname

        for f in ['stdout', 'stderr']:
            offset = offsets[f]
            log_path = log_md.log_path + "/" + f
            while True:
                payload = {
                    'path': log_path,
                    'length': str(TASK_LOG_CHUNK_LEN),
                    'offset': str(offset)
                }

                try:
                    response = requests.get(
                        log_md.log_url + '/files/read',
                        params=payload).json()

                    log_length = len(response['data'])
                    for line in response['data'].splitlines():
                        self.log_line(
                            stream=f,
                            line=line,
                            task_id=task_id,
                            container_id=log_md.container_id,
                            agent=agent,
                        )
                except Exception as e:
                    log.error("Failed to get {path}@{agent} {error}".format(
                        path=log_path, agent=agent, error=e
                    ))
                    break

                offset = offset + log_length
                # Stop if there is no more data
                if log_length < TASK_LOG_CHUNK_LEN:
                    break
            # Update offset of this stream
            offsets[f] = offset

        # Update both offsets for the task
        with self.task_lock:
            self.running_tasks = self.running_tasks.set(
                task_id,
                log_md.set(log_offsets=pmap(offsets)),
            )

    # process downstream events
    def event_loop(self):
        while True:
            while not self.src_queue.empty():
                e = self.src_queue.get()
                self.dest_queue.put(e)
                self.src_queue.task_done()

                # Record the base log url
                if e.kind == 'task' and e.platform_type == 'staging':
                    url = e.raw.offer.url.scheme + '://' + \
                        e.raw.offer.url.address.ip + ':' + \
                        str(e.raw.offer.url.address.port)
                    self.staging_tasks = self.staging_tasks.set(e.task_id, url)

                if e.kind == 'task' and e.platform_type == 'running':
                    if e.task_id not in self.staging_tasks:
                        log.info(
                            f"Task {e.task_id} already running, not fetching logs")
                        continue

                    url = self.staging_tasks[e.task_id]
                    self.staging_tasks = self.staging_tasks.discard(e.task_id)

                    # Simply pass the needed fields and let the logging thread
                    # to take care of the slow path discovery.
                    container_id = e.raw.container_status.container_id.value
                    executor_id = e.raw.executor_id.value
                    with self.task_lock:
                        self.running_tasks = self.running_tasks.set(
                            e.task_id,
                            LogMetadata(
                                log_url=url,
                                container_id=container_id,
                                executor_id=executor_id
                            )
                        )

                # Fetch the last log and remove the entry if the task is active
                if e.kind == 'task' and e.terminal:
                    with self.task_lock:
                        if e.task_id in self.running_tasks:
                            self.done_tasks = self.done_tasks.append(e.task_id)

            if self.stopping:
                return

            time.sleep(1)

    def logging_loop(self):
        while True:
            # grab logs
            for task_id in self.running_tasks.keys():
                self.stream_task_log(task_id)

            while len(self.done_tasks):
                task_id = self.done_tasks[0]
                self.stream_task_log(task_id)
                with self.task_lock:
                    self.done_tasks = self.done_tasks.remove(task_id)
                    self.running_tasks = self.running_tasks.discard(task_id)

            if self.stopping:
                return

            time.sleep(10)

    def run(self, task_config):
        self.downstream_executor.run(task_config)

    def stop(self):
        self.downstream_executor.stop()
        self.stopping = True
        self.event_thread.join()
        self.logging_thread.join()

    def get_event_queue(self):
        return self.dest_queue

    def reconcile(self, task_config):
        self.downstream_executor.reconcile(task_config)

    def kill(self, task_id):
        return self.downstream_executor.kill(task_id)
