# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import abc
from collections import namedtuple

import six

TaskConfig = namedtuple(
    'TaskConfig',
    ['image', 'cmd', 'cpus', 'mem', 'disk', 'volumes', 'ports', 'cap_add',
     'ulimit', 'docker_parameters'],
)


def make_task_config(image="ubuntu:xenial", cmd="/bin/true", cpus=0.1,
                     mem=32, disk=1000, volumes=None, ports=[], cap_add=[],
                     ulimit=[], docker_parameters=[]):
    if volumes is None:
        volumes = {}
    if ports is None:
        ports = []
    if cap_add is None:
        cap_add = []
    if ulimit is None:
        ulimit = []
    if docker_parameters:
        docker_parameters = []

    return TaskConfig(image, cmd, cpus, mem, disk, volumes, ports, cap_add,
                      ulimit, docker_parameters)


@six.add_metaclass(abc.ABCMeta)
class TaskExecutor(object):
    @abc.abstractmethod
    def run(self, task_config):
        pass

    @abc.abstractmethod
    def kill(self, task_id):
        pass
