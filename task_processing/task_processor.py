import importlib
import logging

from pyrsistent import field
from pyrsistent import m
from pyrsistent import PMap
from pyrsistent import pmap
from pyrsistent import PRecord

from task_processing.interfaces import TaskExecutor

log = logging.getLogger(__name__)


class Registry(PRecord):
    plugin_modules = field(type=PMap, initial=m(), factory=pmap)
    """
    A map of plugin names (str) to that plugins entry point (a module)

    We use this to allow the registry and plugins to dynamically
    change their behaviors. Currently just the plugins registering executors
    """

    def register_task_executor(self, name, task_executor_cls):
        """Helper method for adding an executor"""
        return self.transform(
            ('task_executors', name), lambda _: task_executor_cls
        )

    def register_deprecated_task_executor(self, name, task_executor_cls):
        """Helper method for adding an deprecated executor"""
        return self.transform(
            ('deprecated_task_executors', name), lambda _: task_executor_cls
        )

    def _executor_invariant(task_executors):
        """Invariant that task_executors must be TaskExecutors"""
        return (
            all(issubclass(v, TaskExecutor) for v in task_executors.values()),
            'task_executors must always be a TaskExecutor'
        )

    task_executors = field(
        type=PMap, initial=m(), factory=pmap,
        invariant=_executor_invariant,
    )
    deprecated_task_executors = field(
        type=PMap, initial=m(), factory=pmap,
        invariant=_executor_invariant,
    )
    """
    A map of task executor names (str) to a class definition which implements
    :class:`task_processing.interfaces.TaskExecutor`
    """


class TaskProcessor:
    """Main Entry Point for Task Processing plugins

    This is the entrypoint to registering plugins (which know how to
    actually run tasks) and creating instances of TaskExecutors. Note that
    since most customers of this library will be importing this class into
    their code, let's try to make it Python2/3 compatible.

    Typical use would be:

        tp = TaskProcessor()
        # registers 'myplugin'
        tp.load_plugin('my.plugin')
        # ... within the my.plugin.register_plugin method:
        registry.register_task_executor('foobar', FooBarExecutor)

        # register 'another'
        tp.load_plugin('another.plugin')

        # Later...
        executor = tp.executor_from_config(
            provider='foobar',
            provider_config={'specific': 'config', 'for', 'plugin'}
        )
        executor.run()
    """

    registry = Registry()

    def load_plugin(self, provider_module):
        module = importlib.import_module(provider_module)
        plugin_name = module.TASK_PROCESSING_PLUGIN

        name_update = Registry(plugin_modules=m().set(plugin_name, module))
        plugin_update = module.register_plugin(Registry())

        def conflict_check(old, new):
            # Any PMap in the registry ought have no duplicate keys,
            # if they do that means two plugins tried to define the same
            # executor or something
            if type(old) == PMap:
                conflicts = set(old.keys()) & set(new.keys())
                if conflicts:
                    raise ValueError(
                        '{0} is trying to register elements that already '
                        'exist in the registry: {1}'.format(
                            plugin_name, conflicts
                        )
                    )
            return old.update(new)

        self.registry = self.registry.update_with(
            conflict_check, plugin_update, name_update
        )

    def executor_cls(self, provider):
        """Called by users of TaskProcessing to obtain :class:`TaskExecutor`s

        :param str provider: The kind of executor the user wants to run.
        :param dict provider_config: The arguments needed to instantiate
            the provider.
        """
        if provider in self.registry.task_executors:
            return self.registry.task_executors[provider]
        elif provider in self.registry.deprecated_task_executors:
            log.warning(
                f'{provider} is a deprecated executor and will be removed in the future')
            return self.registry.deprecated_task_executors[provider]
        else:
            raise ValueError(
                '{0} provider not registered; available providers: {1}'.format(
                    provider, self.registry.task_executors.keys().tolist()
                )
            )

    def executor_from_config(self, provider, provider_config=None):
        if provider_config is None:
            provider_config = dict()

        return self.executor_cls(provider)(**provider_config)
