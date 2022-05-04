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
from typing import Optional, Dict

from google.protobuf.message import Message

from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.filter.strategy.impl.default_grpc_filter_strategy import DefaultGrpcFilterStrategy
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcRawFilterStrategy
from th2_common.schema.message.configuration.message_configuration import FieldFilterConfiguration, ServiceConfiguration
from th2_common.schema.strategy.route.routing_strategy import RoutingStrategy


class Filter(RoutingStrategy):

    def __init__(self, service_configuration: ServiceConfiguration) -> None:
        self.__filter_strategy = DefaultGrpcFilterStrategy()
        self.service_configuration = service_configuration
        self.filters = []
        for filter_objects in service_configuration.filters:
            for field_filter_cfg in filter_objects.properties:
                self.filters.append(field_filter_cfg)

    def get_endpoint(self, message: Message, properties: Optional[Dict[str, str]] = None):
        return self.__filter(properties)

    def __filter(self, message: Optional[Dict[str, str]]) -> {str}:
        for fields_filter in self.filters:
            if self.__filter_strategy.verify(message=message, router_filter=fields_filter):
                return self.service_configuration.strategy.endpoints[0]
        raise Exception('No property filters were passed')
