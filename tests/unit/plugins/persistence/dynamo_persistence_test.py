import pytest
from hypothesis import given
from hypothesis import strategies as st

from task_processing.interfaces.event import Event
from task_processing.plugins.persistence.dynamodb_persistence \
    import DynamoDBPersister


@pytest.fixture
def persister(mocker):
    mock_session = mocker.Mock()
    mock_session.client.return_value = []

    mock_resource = mocker.Mock()
    mock_resource.Table.return_value = mocker.Mock()
    mock_session.resource.return_value = mock_resource
    persister = DynamoDBPersister(
        table_name='foo',
        session=mock_session
    )
    return persister


@given(x=st.dictionaries(
    keys=st.text(),
    values=st.decimals(allow_nan=False, allow_infinity=False)
))
def test_replaces_decimals_dict(x, persister):
    for k, v in persister._replace_decimals(x).items():
        assert type(v) == float


@given(x=st.decimals(allow_nan=False, allow_infinity=False))
def test_replaces_decimals_decimal(x, persister):
    assert type(persister._replace_decimals(x)) is float


@given(x=st.lists(st.decimals(allow_nan=False, allow_infinity=False)))
def test_replaces_decimals_list(x, persister):
    assert all([type(v) == float for v in persister._replace_decimals(x)])


@given(x=st.one_of(
    st.text(),
    st.booleans(),
))
def test_replaces_decimals_unaffected(x, persister):
    assert persister._replace_decimals(x) == x


@given(x=st.builds(Event,
                   kind=st.sampled_from(['task', 'control']),
                   timestamp=st.floats(
                       min_value=0, allow_nan=False, allow_infinity=False),
                   terminal=st.booleans(),
                   success=st.booleans(),
                   task_config=st.dictionaries(
                       keys=st.text(), values=st.text()),
                   ))
def test_event_to_item_timestamp(x, persister):
    res = persister._event_to_item(x)['M']
    print(res)
    assert 'N' in res['timestamp'].keys()
    assert 'BOOL' in res['success'].keys()
    assert 'BOOL' in res['terminal'].keys()
    assert 'M' in res['task_config'].keys()


@given(x=st.builds(
    Event,
    kind=st.sampled_from(['task', 'control']),
    timestamp=st.floats(min_value=0, allow_nan=False, allow_infinity=False),
    terminal=st.booleans(),
    success=st.booleans(),
    task_config=st.dictionaries(
       keys=st.text(),
       values=st.lists(
           st.one_of(
               st.text(),
               st.dictionaries(
                   keys=st.text(),
                   values=st.text(),
               )
           )
       )
       ),
))
def test_event_to_item_list(x, persister):
    res = persister._event_to_item(x)['M']
    for k, v in x.task_config.items():
        if len(v) > 0:
            assert k in res['task_config']['M']
        else:
            assert k not in res['task_config']['M']
            assert all([{'S': val} in ['task_config']['M'][k]['L']
                        for val in v])
