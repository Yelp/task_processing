from unittest.mock import MagicMock

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.translator import MESOS_STATUS_MAP
from task_processing.plugins.mesos.translator import mesos_status_to_event


def test_translator_maps_status_to_event():
    for k in MESOS_STATUS_MAP:
        mesos_status = MagicMock()
        mesos_status.state = k
        assert isinstance(mesos_status_to_event(mesos_status, ['123']), Event)
