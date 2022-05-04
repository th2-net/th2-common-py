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


from importlib import import_module
from pathlib import Path
from pkgutil import iter_modules
from typing import Optional, Dict

import grpc

from th2_common.schema.exception.grpc_router_error import GrpcRouterError
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcConfiguration, GrpcRouterConfiguration, \
    GrpcRetryPolicy
from th2_common.schema.grpc.router.abstract_grpc_router import AbstractGrpcRouter
import th2_common.schema.strategy.route.impl as route
from th2_common.schema.message.configuration.message_configuration import ServiceConfiguration


class DefaultGrpcRouter(AbstractGrpcRouter):

    def __init__(self, grpc_configuration: GrpcConfiguration,
                 grpc_router_configuration: GrpcRouterConfiguration) -> None:
        super().__init__(grpc_configuration, grpc_router_configuration)
        self.strategies = dict()
        self.__load_strategies()

    def get_service(self, cls):
        return cls(self)

    class Connection:

        def __init__(self, service: ServiceConfiguration, strategy_obj, stub_class, channels, options):
            self.service = service
            self.strategy_obj = strategy_obj
            self.stubClass = stub_class
            self.channels = channels
            self.options = options
            self.stubs = {}

        def __create_stub_if_not_exists(self, endpoint_name, config):
            socket = f"{config.host}:{config.port}"
            if socket not in self.channels:
                self.channels[socket] = grpc.insecure_channel(socket, options=self.options)

            if endpoint_name not in self.stubs:
                self.stubs[endpoint_name] = self.stubClass(self.channels[socket])

        def create_request(self, request_name, request, timeout, properties: Optional[Dict[str, str]]):
            endpoint = self.strategy_obj.get_endpoint(request, properties)
            endpoint_config = self.service.endpoints[endpoint]
            if endpoint_config is not None:
                self.__create_stub_if_not_exists(endpoint, endpoint_config)
            stub = self.stubs[endpoint]
            if stub is not None:
                return getattr(stub, request_name)(request, timeout=timeout, properties=properties)

    def get_connection(self, service_class, stub_class):
        find_service = None
        if self.grpc_configuration.services:
            for service_cfg in self.grpc_configuration.services.values():
                if service_cfg.service_class.split('.')[-1] == service_class.__name__:
                    find_service = service_cfg
                    break
        else:
            raise GrpcRouterError("Services list are empty in 'grpc.json'. Check your links")

        strategy_name = find_service.strategy.name
        strategy_class = self.strategies[strategy_name]
        if strategy_class is None:
            return None
        strategy_obj = strategy_class(find_service)
        return self.Connection(find_service, strategy_obj, stub_class, self.channels, self.grpc_router_configuration.retry_policy.options)

    def __load_strategies(self):
        package_dir = str(Path(route.__file__).resolve().parent)

        for _, module_name, _ in iter_modules([package_dir]):
            module = import_module(f'{route.__name__}.{module_name}')
            for name in dir(module):
                if not name.startswith('__'):
                    attr = getattr(module, name)
                    if 'get_endpoint' in dir(attr):
                        self.strategies[name.lower()] = attr

        self.strategies.pop('routingstrategy', None)
