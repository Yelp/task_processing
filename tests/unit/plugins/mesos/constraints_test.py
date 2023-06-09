import pytest
from pyrsistent import m

from task_processing.plugins.mesos.constraints import attributes_match_constraints
from task_processing.plugins.mesos.constraints import Constraint


@pytest.fixture
def fake_dict():
    return m(
        pool="fake_pool_text",
        region="fake_region_text",
    )


def test_constraints_eq_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="==",
                value="fake_region_text",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="==",
                value="random_text",
            ),
        ],
    )


def test_constraints_eq_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="==",
                value="another_fake_region_text",
            ),
        ],
    )


def test_constraints_EQUALS_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="EQUALS",
                value="fake_region_text",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="EQUALS",
                value="random_text",
            ),
        ],
    )


def test_constraints_EQUALS_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="EQUALS",
                value="another_fake_region_text",
            ),
        ],
    )


def test_constraints_ne_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="!=",
                value="another_fake_region_text",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="!=",
                value="random_text",
            ),
        ],
    )


def test_constraints_ne_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="!=",
                value="fake_region_text",
            ),
        ],
    )


def test_constraints_NOTEQUALS_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="NOTEQUALS",
                value="another_fake_region_text",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="NOTEQUALS",
                value="random_text",
            ),
        ],
    )


def test_constraints_NOTEQUALS_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="NOTEQUALS",
                value="fake_region_text",
            ),
        ],
    )


def test_constraints_LIKE_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="LIKE",
                value="fak.*t..t",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="LIKE",
                value="random_text",
            ),
        ],
    )


def test_constraints_LIKE_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="LIKE",
                value="another_fak.*t..t",
            ),
        ],
    )
    assert not attributes_match_constraints(
        fake_dict,
        [Constraint(attribute="region", operator="LIKE", value="fake_region")],
    )


def test_constraints_UNLIKE_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="UNLIKE",
                value="another_fak.*t..t",
            ),
        ],
    )
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="fake_attribute_name",
                operator="UNLIKE",
                value="random_text",
            ),
        ],
    )


def test_constraints_UNLIKE_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="UNLIKE",
                value="fak.*t..t",
            ),
        ],
    )


def test_constraints_all_match(fake_dict):
    assert attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="==",
                value="fake_region_text",
            ),
            Constraint(
                attribute="pool",
                operator="==",
                value="fake_pool_text",
            ),
        ],
    )


def test_constraints_all_not_match(fake_dict):
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="==",
                value="another_fake_region_text",
            ),
            Constraint(
                attribute="pool",
                operator="==",
                value="fake_pool_text",
            ),
        ],
    )
    assert not attributes_match_constraints(
        fake_dict,
        [
            Constraint(
                attribute="region",
                operator="==",
                value="fake_region_text",
            ),
            Constraint(
                attribute="pool",
                operator="==",
                value="another_fake_pool_text",
            ),
        ],
    )
