# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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


class RobinStrategy:
    i = 0

    def __init__(self, config):
        self.endpoints = config["endpoints"]

    def get_endpoint(self, request):
        if self.i >= len(self.endpoints):
            self.i = 0
        result = self.endpoints[self.i]
        self.i = self.i + 1
        return result
