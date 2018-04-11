import abc

from pyrsistent import field
from pyrsistent import PRecord


class Constriant(PRecord):
    attribute = field(type=str)
    comparator = field(type=str, invariant=lambda x: x in ("!=", "="))
    value = field(type=str)
