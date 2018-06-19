from pyrsistent import InvariantException

from task_processing.plugins.mesos.mesos_executor import MesosTaskConfig


def test_mesos_task_config_factories():
    m = MesosTaskConfig(
        cmd='/bin/true', cpus=1, mem=64, disk=15, gpus=6.0, image='fake_image')

    assert type(m.cpus) is float
    assert m.cpus == 1.0

    assert type(m.mem) is float
    assert m.mem == 64.0

    assert type(m.disk) is float
    assert m.disk == 15.0

    assert type(m.gpus) is int
    assert m.gpus == 6

    try:
        m = m.set(name='a' * 256)
        assert False, 'Task id longer than 255 characters was accepted'
    except InvariantException as e:
        print(e)
        assert True
