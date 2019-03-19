from queue import Queue

from task_processing.interfaces.task_executor import TaskExecutor


def test_task_executor():
    class TaskExecutorImpl(TaskExecutor):
        def run(self, task_config):
            return task_config

        def kill(self, task_id):
            return True

        def get_event_queue(self):
            return Queue()

        def reconcile(self, task_config):
            pass

        def stop(self):
            pass

    t = TaskExecutorImpl()
    assert t.run('foo') == 'foo'
    assert t.kill('mock')
