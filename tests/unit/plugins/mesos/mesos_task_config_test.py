from task_processing.plugins.mesos.mesos_executor import MesosTaskConfig


def test_mesos_task_config_factories():
    m = MesosTaskConfig(cpus=1, mem=64, disk=15, gpus=6.0, image='fake_image')

    assert type(m.cpus) is float
    assert m.cpus == 1.0

    assert type(m.mem) is float
    assert m.mem == 64.0

    assert type(m.disk) is float
    assert m.disk == 15.0

    assert type(m.gpus) is int
    assert m.gpus == 6
