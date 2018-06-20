import re

from pyrsistent import field
from pyrsistent import PRecord


def equals_op(expected_value, actual_value):
    return expected_value == actual_value


def notequals_op(expected_value, actual_value):
    return expected_value != actual_value


def like_op(re_pattern, actual_value):
    return re.match(re_pattern, actual_value)


def unlike_op(re_pattern, actual_value):
    return not like_op(re_pattern, actual_value)


OPERATORS = {
    'EQUALS': equals_op,
    '==': equals_op,
    'NOTEQUALS': notequals_op,
    '!=': notequals_op,
    'LIKE': like_op,
    'UNLIKE': unlike_op,
}


def _attributes_match_constraint(attributes, constraint):
    actual_value = attributes.get(constraint.attribute)
    # If the dictionary doesn't contain an attribute from the constraint then
    # the constraint is satisfied.
    if actual_value is None:
        return True

    # The operator names have already been validated by the validator in
    # `MesosTaskConfig`, so it's guaranteed that it's in `OPERATORS`.
    return OPERATORS[constraint.operator](constraint.value, actual_value)


def attributes_match_constraints(attributes, constraints):
    # If constraints aren't specified then they are satisfied.
    if constraints is None:
        return True

    return all(_attributes_match_constraint(attributes, c)
               for c in constraints)


def valid_constraint_operator_name(name):
    operators_names = OPERATORS.keys()
    return (name in operators_names,
            '{operator} is not a valid operator, valid operators are '
            '{operators}.'.format(operator=name,
                                  operators=operators_names))


class Constraint(PRecord):
    attribute = field(type=str)
    operator = field(type=str, invariant=valid_constraint_operator_name)
    value = field(type=str)
