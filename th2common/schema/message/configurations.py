#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
# # Licensed under the Apache License, Version 2.0 (the "License");
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

from abc import ABC, abstractmethod


class Configuration(ABC):
    pass


class FilterableConfiguration(Configuration):
    pass


class QueueConfiguration(Configuration):

    def __init__(self, name: str, exchange: str, attributes: list, filters: list, canRead=True,
                 canWrite=True) -> None:
        self.name = name
        self.exchange = exchange
        self.attributes = attributes
        self.filters = filters
        self.canRead = canRead
        self.canWrite = canWrite


class MessageRouterConfiguration(FilterableConfiguration):
    def __init__(self, queues) -> None:
        self.queues = queues
        self.attributes = dict()
        for queue_alias in queues.keys():
            queue_configuration = QueueConfiguration(**queues[queue_alias])
            for attr in queue_configuration.attributes:
                if not self.attributes.__contains__(attr):
                    self.attributes[attr] = set()
                self.attributes[attr].add(queue_alias)


class FieldFilterConfiguration(Configuration):

    def __init__(self, value, operation) -> None:
        self.value = value
        self.operation = operation


class RouterFilter(Configuration):

    @abstractmethod
    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        pass

    @abstractmethod
    def get_message(self) -> {str: FieldFilterConfiguration}:
        pass
