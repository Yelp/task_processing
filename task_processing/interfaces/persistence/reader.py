import abc

import six


@six.add_metaclass(abc.ABCMeta)
class Reader():

    @abc.abstractmethod
    def read(event):
        pass
