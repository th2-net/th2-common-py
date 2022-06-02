#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from typing import Any, Callable, Dict, Generator, List, Optional, Tuple

import google.protobuf.message
import grpc
from grpc import _channel
from th2_common.schema.exception.grpc_router_error import GrpcRouterError
from th2_common.schema.filter.strategy.impl.default_grpc_filter_strategy import DefaultGrpcFilterStrategy
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcConfiguration, GrpcConnectionConfiguration, \
    GrpcEndpointConfiguration, ServiceConfiguration
from th2_common.schema.grpc.router.abstract_grpc_router import AbstractGrpcRouter


class DefaultGrpcRouter(AbstractGrpcRouter):

    def __init__(self,
                 grpc_configuration: GrpcConfiguration,
                 grpc_router_configuration: GrpcConnectionConfiguration) -> None:
        super().__init__(grpc_configuration, grpc_router_configuration)

    def get_service(self, cls: Callable) -> Callable:
        return cls(self)  # type: ignore

    class Connection:

        def __init__(self,
                     services: List[ServiceConfiguration],
                     stub_class: Callable,
                     channels: Dict[str, _channel.Channel],
                     options: List[Tuple[str, Any]]) -> None:
            self.services = services
            self.stubClass = stub_class
            self.channels = channels
            self.options = options
            self.stubs: Dict[str, Callable] = {}
            self._grpc_filter_strategy = DefaultGrpcFilterStrategy()
            self._endpoint_generator: Optional[Generator] = None

        def __create_stub_if_not_exists(self, endpoint_name: str, config: GrpcEndpointConfiguration) -> None:
            socket = f'{config.host}:{config.port}'
            if socket not in self.channels:
                self.channels[socket] = grpc.insecure_channel(socket, options=self.options)

            if endpoint_name not in self.stubs:
                self.stubs[endpoint_name] = self.stubClass(self.channels[socket])

        def create_request(self,
                           request_name: str,
                           request: google.protobuf.message.Message,
                           timeout: int,
                           properties: Optional[Dict[str, str]]) -> Optional[google.protobuf.message.Message]:
            service = self._filter_services(properties)
            endpoint = self._get_next_endpoint(service)
            endpoint_config = service.endpoints[endpoint]

            if endpoint_config is not None:
                self.__create_stub_if_not_exists(endpoint, endpoint_config)
            stub = self.stubs[endpoint]

            if stub is not None:
                return getattr(stub, request_name)(request, timeout=timeout)  # type: ignore

            return None

        def _filter_services(self, properties: Optional[Dict[str, str]]) -> ServiceConfiguration:
            if not properties:
                services = self.services
            else:
                services = [
                    service for service in self.services
                    for service_filter in service.filters
                    if self._grpc_filter_strategy.verify(properties, router_filters=service_filter.properties)
                ]

            if len(services) != 1:
                raise GrpcRouterError('Number of services matching properties should be 1. '
                                      'Check your gRPC configuration')

            return services[0]

        def _get_next_endpoint(self, service: ServiceConfiguration) -> str:
            service_endpoints = list(service.endpoints)
            if self._endpoint_generator is None:
                self._endpoint_generator = self._get_endpoint_generator(service_endpoints)

            return next(self._endpoint_generator)  # type: ignore

        def _get_endpoint_generator(self, endpoints: List[str]) -> Generator:
            current_index = 0
            endpoints_length = len(endpoints)
            while True:
                yield endpoints[current_index % endpoints_length]
                current_index += 1

    def get_connection(self, service_class: Callable, stub_class: Callable) -> Optional[Connection]:
        if self.grpc_configuration.services:
            find_services = list(filter(  # noqa: ECE001
                lambda service_cfg: (service_cfg.service_class.split('.')[-1] == service_class.__name__),
                self.grpc_configuration.services.values()
            ))

            if find_services:
                return self.Connection(find_services,
                                       stub_class,
                                       self.channels,
                                       self.grpc_router_configuration.retry_policy.options)
            return None
        else:
            raise GrpcRouterError("Services list are empty in 'grpc.json'. Check your links")
