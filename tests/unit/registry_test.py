from task_processing.registry import Registry


def test_load_plugin():
    r = Registry()
    r.load_plugin('tests.mock_plugin')

    assert 'mock_plugin' in r.plugin_registry
    assert 'dummy' in r.task_executor_registry


def test_executor_from_config():
    r = Registry()
    r.load_plugin('tests.mock_plugin')

    executor = r.executor_from_config(
        provider='dummy',
        provider_config={
            'arg': 'foobar'
        }
    )
    assert executor.arg == 'foobar'
    executor.run(None)
    executor.kill(None)
