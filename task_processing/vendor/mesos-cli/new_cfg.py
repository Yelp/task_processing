# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import errno
import json
import os


_cfg_name = ".mesos.json"


class NewConfig(object):

    _default_profile = "default"
    DEFAULTS = {
        "debug": "false",
        "log_file": None,
        "log_level": "warning",
        "max_workers": 5,
        "scheme": "http",
        "response_timeout": 20,
    }

    def __init__(self, master):
        self.DEFAULTS.update({'master': master})
        self.__items = {self._default_profile: self.DEFAULTS}
        self["profile"] = self._default_profile

    def __str__(self):
        return json.dumps(self.__items, indent=4)

    @property
    def _profile_key(self):
        return self.__items.get("profile", self._default_profile)

    @property
    def _profile(self):
        return self.__items.get(self._profile_key, {})

    def __getitem__(self, item):
        if item == "profile":
            return self.__items[item]
        return self._profile.get(item, self.DEFAULTS[item])

    def __setitem__(self, k, v):
        if k == "profile":
            self.__items[k] = v
            return

        profile = self._profile
        profile[k] = v
        self.__items[self._profile_key] = profile
