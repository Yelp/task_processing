import importlib

import pyrsistent

from task_processing.interfaces import TaskExecutor


class Registry(object):
    """Registry for Task Processing plugins

    This is the entrypoint to registering plugins (which know how to
    actually run tasks) and creating instances of TaskExecutors. Note that
    since most customers of this library will be importing this class into
    their code, let's try to make it Python2/3 compatible

    Typical use would be:

        reg = Registry()
        # registers 'myplugin'
        reg.load_plugin('my.plugin')
        # ... within the my.plugin.register_plugin method:
        registry.add_task_executor('foobar', FooBarExecutor)

        # register 'another'
        reg.load_plugin('another.plugin')

        # Later...
        executor = reg.executor_from_config(
            provider='foobar',
            provider_config={'specific': 'config', 'for', 'plugin'}
        )
        executor.run()
    """

    plugin_registry = pyrsistent.m()
    """
    A map of plugin names (str) to that plugins entry point (a module)

    We use this to allow the registry and plugins to dynamically
    change their behaviors. Currently just the plugins registering executors
    """

    task_executor_registry = pyrsistent.m()
    """
    A map of task executor names (str) to a class definition which implements
    :class:`task_processing.interfaces.TaskExecutor`
    """

    def load_plugin(self, provider_module):
        module = importlib.import_module(provider_module)
        name = module.register_plugin(self)
        self.plugin_registry = self.plugin_registry.set(name, module)

    def register_task_executor(self, name, task_executor_impl):
        """Called by plugins during load_plugin to register TaskExcutors

        :param str provider: The name that clients will use to request this
        particular task_executor

        :param class task_executor_impl: A class which implements the
        :class:`task_processing.interfaces.TaskExecutor` interface.

        """
        if not issubclass(task_executor_impl, TaskExecutor):
            raise TypeError(
                'TaskExecutors must implement the TaskeExecutor interface'
            )
        if name in self.task_executor_registry:
            raise ValueError('{0} already registered'.format(name))

        new_registry = self.task_executor_registry.set(
            name, task_executor_impl
        )
        self.task_executor_registry = new_registry

    def executor_from_config(self, provider, provider_config=None):
        """Called by users of TaskProcessing to obtain :class:`TaskExecutor`s

        :param str provider: The kind of executor the user wants to run.
        :param dict provider_config: The arguments needed to instantiate
            the provider.
        """
        if provider not in self.task_executor_registry:
            raise ValueError(
                '{0} provider not registered; available providers: {1}'.format(
                    provider, self.task_executor_registry.keys().tolist()
                )
            )
        if provider_config is None:
            provider_config = dict()

        executor_cls = self.task_executor_registry[provider]
        return executor_cls(**provider_config)
