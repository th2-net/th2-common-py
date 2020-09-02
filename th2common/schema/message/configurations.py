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

from abc import ABC, abstractmethod


class Configuration(ABC):
    pass


class FilterableConfiguration(Configuration, ABC):
    pass


class QueueConfiguration(Configuration):

    def __init__(self, name: str, exchange: str, attributes: list, filters: list, prefetch_count: int = 1, canRead=True,
                 canWrite=True) -> None:
        self.name = name
        self.exchange = exchange
        self.prefetch_count = prefetch_count
        self.attributes = attributes
        self.filters = [MqRouterFilterConfiguration(**filter_schema) for filter_schema in filters]
        self.canRead = canRead
        self.canWrite = canWrite


class MessageRouterConfiguration(FilterableConfiguration):
    def __init__(self, queues: dict) -> None:
        self.queues = queues
        self.attributes = dict()
        for queue_alias in queues.keys():
            queue_configuration = QueueConfiguration(**queues[queue_alias])
            self.queues[queue_alias] = queue_configuration
            for attr in queue_configuration.attributes:
                if not self.attributes.__contains__(attr):
                    self.attributes[attr] = set()
                self.attributes[attr].add(queue_alias)

    def get_queue_by_alias(self, queue_alias):
        return self.queues[queue_alias]

    def get_queues_alias_by_attribute(self, attributes: list):
        result = set()
        for queue_alias in self.queues.keys():
            result.add(queue_alias)
        for attr in attributes:
            result = result.intersection(self.attributes[attr] if self.attributes.__contains__(attr) else set())
        return result


class FieldFilterConfiguration(Configuration):

    def __init__(self, value, operation) -> None:
        self.value = value
        self.operation = operation


class RouterFilter(Configuration, ABC):

    @abstractmethod
    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        pass

    @abstractmethod
    def get_message(self) -> {str: FieldFilterConfiguration}:
        pass


class MqRouterFilterConfiguration(RouterFilter):

    def __init__(self, metadata=None, message=None) -> None:
        if metadata is None:
            metadata = dict()
        if message is None:
            message = dict()
        self.metadata = {key: FieldFilterConfiguration(**metadata[key]) for key in metadata.keys()}
        self.message = {key: FieldFilterConfiguration(**message[key]) for key in message.keys()}

    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        return self.metadata

    def get_message(self) -> {str: FieldFilterConfiguration}:
        return self.message
