import abc

import six


@six.add_metaclass(abc.ABCMeta)
class Runner(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
