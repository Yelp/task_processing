from task_processing.interfaces.event import Event


def test_event_creation():
    x = object()
    e = Event(raw=x, terminal=True, platform_type='killed')
    assert e.raw == x
    assert e.terminal
    assert e.platform_type == 'killed'
