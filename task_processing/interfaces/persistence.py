import abc


class Persister(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def read(self, task_id):
        pass

    @abc.abstractmethod
    def write(self, event):
        pass
