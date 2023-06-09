import threading

import mock
import pytest


@pytest.fixture(autouse=True)
def mock_sleep():
    with mock.patch("time.sleep"):
        yield


@pytest.fixture
def mock_Thread():
    with mock.patch.object(threading, "Thread") as mock_Thread:
        yield mock_Thread
