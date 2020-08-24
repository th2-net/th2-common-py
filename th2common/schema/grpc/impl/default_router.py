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

import grpc

from th2common.schema import strategy
from th2common.schema.grpc.abstract_router import AbstractGrpcRouter
from th2common.schema.grpc.configurations import GrpcRouterConfiguration


class DefaultGrpcRouter(AbstractGrpcRouter):

    def __init__(self, configuration: GrpcRouterConfiguration) -> None:
        super().__init__(configuration)
        self.strategies = dict()
        self.__load_strategies()

    def get_service(self, cls):
        return cls(self)

    class Connection:

        stubs = {}

        def __init__(self, service, strategy_obj, stub_class):
            self.service = service
            self.strategy_obj = strategy_obj
            self.stubClass = stub_class

        def __create_stub_if_not_exists(self, endpoint_name, config):
            if endpoint_name not in self.stubs:
                self.stubs[endpoint_name] = self.stubClass(
                    grpc.insecure_channel(config["host"] + ":" + str(config["port"])))

        def create_request(self, request_name, request):
            endpoint = self.strategy_obj.get_endpoint(request)
            endpoint_config = self.service["endpoints"][endpoint]
            if endpoint_config is not None:
                self.__create_stub_if_not_exists(endpoint, endpoint_config)
            stub = self.stubs[endpoint]
            if stub is not None:
                return getattr(stub, request_name)(request)

    def get_connection(self, service_class, stub_class):
        find_service = None
        for service in self.configuration.services:
            if self.configuration.services[service]["service-class"] == service_class.__name__:
                find_service = self.configuration.services[service]
                break
        strategy_name = find_service['strategy']['name']
        strategy_class = self.strategies[strategy_name]
        if strategy_class is None:
            return None
        strategy_obj = strategy_class(find_service['strategy'])
        return self.Connection(find_service, strategy_obj, stub_class)

    def __load_strategies(self):
        for attr in dir(strategy):
            if not attr.startswith("__"):
                if dir(getattr(strategy, attr)).__contains__("get_endpoint"):
                    self.strategies[attr.lower()] = getattr(strategy, attr)
