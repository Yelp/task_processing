from pyrsistent import field
from pyrsistent import PRecord


class ConstraintMatcher(object):
    @abc.abstractmethod
    def match(constraint):
        raise NotImplementedError(
            'subclass must define match to use this baseclass')


class EqualsConstraint(ConstraintMatcher):
    def match(attribute, value):
        return attribute == value


class NotEqualsConstraint(ConstraintMatcher):
    def match(attribute, value):
        return attribute != value


def matcher_for_op(op):
    x = {'=': EqualsConstraint, '!=': NotEqualsConstraint}.get(op)


results = []
for constraint in task_config.constraints():
    # constraint = ["region", "!=", "uswest-2"]
    c = Constraint(constraint[0], constraint[1], constraint[2])
    matcher = matcher_for_op(c.comparator)
    matches = matcher(c.attribute, c.value)
    results.append(matches)

constraints_satisfied = all(results)
