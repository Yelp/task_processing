import pytest
from pyrsistent import InvariantException
from pyrsistent import PRecord
from pyrsistent import PTypeError

from task_processing.interfaces.event import Event


@pytest.fixture
def event():
    return Event(kind='task')


def test_event_creation():
    x = object()
    e = Event(kind='task', raw=x, terminal=True, platform_type='killed')
    assert e.raw == x
    assert e.terminal
    assert e.platform_type == 'killed'


def test_event_is_immutable(event):
    assert isinstance(event, PRecord)


def test_event_has_task_id(event):
    assert event.set(task_id='foo').task_id == 'foo'


def test_event_type_checks(event):
    with pytest.raises(InvariantException) as e:
        Event()

    assert 'missing_fields' in str(e.value)

    with pytest.raises(PTypeError) as e:
        event.set(terminal="hello")

    with pytest.raises(PTypeError) as e:
        event.set(platform_type=123)

    with pytest.raises(PTypeError) as e:
        event.set(task_id=123)
