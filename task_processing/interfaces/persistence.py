import abc

import six


@six.add_metaclass(abc.ABCMeta)
class Persister():

    @abc.abstractmethod
    def read(self, task_id):
        pass

    @abc.abstractmethod
    def write(self, event):
        pass
