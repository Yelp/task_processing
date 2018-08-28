import abc
import uuid

from pyrsistent import field
from pyrsistent import PRecord


class DefaultTaskConfigInterface(PRecord):
    task_id = field(type=uuid.UUID, initial=uuid.uuid4)
    name = field(type=str, initial='default')


class TaskExecutor(metaclass=abc.ABCMeta):
    """The core interface for Task Processing
    This is the class you want to implement to add a new TaskExecutor
    """

    TASK_CONFIG_INTERFACE = DefaultTaskConfigInterface
    """
    The interface, specified as a PRecord of
    objects that you will be passing as task_configs to run
    """

    @abc.abstractmethod
    def run(self, task_config):
        """Run the supplied task

        :param task_config: An object satistfying the TASK_CONFIG_INTERFACE
        The executor should start running the provided task and return the
        task id.

        :returns str task_id: Callers get the id of the task that was run
        to check status or kill it later
        """
        pass

    @abc.abstractmethod
    def reconcile(self, task_config):
        """Request a status event for a task

        :param task_config: The task that you want to check
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

        :returns: TBD
        """
        pass
