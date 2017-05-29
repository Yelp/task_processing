from unittest.mock import MagicMock

from task_processing.interfaces.event import Event
from task_processing.plugins.mesos.translator import mesos_status_to_event
from task_processing.plugins.mesos.translator import MESOS_TASK_STATUS_TO_EVENT


def test_translator_maps_status_to_event():
    for k in MESOS_TASK_STATUS_TO_EVENT:
        mesos_status = MagicMock()
        mesos_status.state = k
        assert isinstance(mesos_status_to_event(mesos_status, 123), Event)
