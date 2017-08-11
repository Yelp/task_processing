import abc
import uuid

import six
from pyrsistent import pvec

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface


@six.add_metaclass(abc.ABCMeta)
class Runner(object):
    TASK_CONFIG_INTERFACE = DefaultTaskConfigInterface
    """
    The interface, specified as a PRecord of
    objects that you will be passing as task_configs to run
    """

    @abc.abstractmethod
    def run(self, task_config, task_id=None):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass

    def new_task_id():
        return pvec(uuid.uuid4())
