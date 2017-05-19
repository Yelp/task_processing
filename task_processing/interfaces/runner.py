import abc

import six

from task_processing.interfaces.task_executor import DefaultTaskConfigInterface


@six.add_metaclass(abc.ABCMeta)
class Runner(object):
    TASK_CONFIG_INTERFACE = DefaultTaskConfigInterface
    """
    The interface, specified as a PRecord of
    objects that you will be passing as task_configs to run
    """

    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
