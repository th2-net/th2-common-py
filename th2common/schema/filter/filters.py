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

from google.protobuf.message import Message

from th2common.schema.exception import FilterCheckError
from th2common.schema.filter.strategy import DefaultFilterStrategy, FilterStrategy
from th2common.schema.grpc.configurations import GrpcRawFilterStrategy
from th2common.schema.message.configurations import MessageRouterConfiguration


class Filter(ABC):

    @abstractmethod
    def check(self, message: Message) -> str:
        pass


class MqMsgFilter(Filter):

    def __init__(self, configuration: MessageRouterConfiguration,
                 filter_strategy: FilterStrategy = DefaultFilterStrategy()) -> None:
        self.configuration = configuration
        self.filter_strategy = filter_strategy

    def check(self, message: Message) -> str:
        queue_alias_result = ""

        for queue_alias in self.configuration.queues.keys():
            queue_config = self.configuration.queues[queue_alias]

            filter_result = self.filter_strategy.verify(message=message, router_filters=queue_config.filters)

            if filter_result:
                if len(queue_alias_result) == 0:
                    queue_alias_result = queue_alias
                else:
                    raise FilterCheckError("Two queues match one message according to configuration filters")

        if len(queue_alias_result) == 0:
            raise FilterCheckError(f"No filters correspond to message: {message}")

        return queue_alias_result


class GrpcMsgFilter(Filter):

    def __init__(self, configuration: GrpcRawFilterStrategy,
                 filter_strategy: FilterStrategy = DefaultFilterStrategy()) -> None:
        self.configuration = configuration
        self.filter_strategy = filter_strategy

    def check(self, message: Message) -> str:
        endpoint_alias = ""

        for fields_filter in self.configuration.filters:
            if self.filter_strategy.verify(message=message, router_filter=fields_filter):
                if len(endpoint_alias) != 0:
                    raise FilterCheckError("Two endpoints match one message according to configuration filters")
                endpoint_alias = fields_filter.endpoint

        if len(endpoint_alias) == 0:
            raise FilterCheckError(f"No filters correspond to message: {message}")

        return endpoint_alias
