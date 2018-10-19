#!/usr/bin/env python
# Copyright 2015-2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import find_packages
from setuptools import setup

import task_processing


setup(
    name='task_processing',
    version=task_processing.__version__,
    provides=['task_processing'],
    author='Task Processing',
    author_email='team-taskproc@yelp.com',
    description='Framework for task processing executors and configuration',
    packages=find_packages(exclude=('tests')),
    include_package_data=True,
    install_requires=[
        'pyrsistent',
    ],
    extras_require={
        # We can add the Mesos specific dependencies here
        'mesos_executor': ['addict', 'pymesos>=0.2.14', 'requests'],
        'metrics': ['yelp-meteorite'],
        'persistence': ['boto3'],
    }
)
