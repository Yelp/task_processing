import abc

import six


@six.add_metaclass(abc.ABCMeta)
class Writer():

    @abc.abstractmethod
    def write():
        pass
