import abc

import six
from pyrsistent import PRecord


class DefaultTaskConfigInterface(PRecord):
    pass


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    """The core interface for Task Processing
    This is the class you want to implement to add a new TaskExecutor
    """

    TASK_CONFIG_INTERFACE = DefaultTaskConfigInterface
    """
    The interface, specified as a PRecord of
    objects that you will be passing as task_configs to run
    """

    @abc.abstractmethod
    def run(self, task_config, task_id):
        """Run the supplied task with task_id.

        :param task_config: An object satistfying the TASK_CONFIG_INTERFACE
        The executor should start running the provided task and return the
        task id.

        :returns pvector task_id: Callers get the id of the task that was run
        to check status or kill it later
        """
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        """Kill the specified task

        :param str task_id: The task that you want to kill
        """
        pass

    @abc.abstractmethod
    def stop(self):
        """Stop the executor stack
        """
        pass

    @abc.abstractmethod
    def get_event_queue(self):
        """Get queue of events

        :returns: Object with .pull and .push methods defined on it.
        """
        pass
