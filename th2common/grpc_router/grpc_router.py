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

import importlib.util
import json
from concurrent.futures.thread import ThreadPoolExecutor
from os import listdir
from os.path import isfile, join
from pathlib import Path

import grpc


class GrpcRouter:
    class Connection:

        stubs = {}

        def __init__(self, service, strategy, stub_class):
            self.service = service
            self.strategy = strategy
            self.stubClass = stub_class

        def __create_stub_if_not_exists(self, endpoint_name, config):
            if endpoint_name not in self.stubs:
                self.stubs[endpoint_name] = self.stubClass(
                    grpc.insecure_channel(config["host"] + ":" + str(config["port"])))

        def create_request(self, request_name, request):
            endpoint = self.strategy.get_endpoint(request)
            endpoint_config = self.service["endpoints"][endpoint]
            if endpoint_config is not None:
                self.__create_stub_if_not_exists(endpoint, endpoint_config)
            stub = self.stubs[endpoint]
            if stub is not None:
                return getattr(stub, request_name)(request)

    def __init__(self, config_file):
        self.strategies = {}
        self.__load_strategies()

        with open(config_file, 'r') as f:
            self.conf = json.load(f)

    def create_server(self):
        server = grpc.server(ThreadPoolExecutor(max_workers=self.conf['server']['workers']))
        server.add_insecure_port(f"{self.conf['server']['host']}:{self.conf['server']['port']}")
        return server

    def get_service(self, cls):
        return cls(self)

    def get_connection(self, service_class, stub_class):
        find_service = None
        for service in self.conf['services']:
            if self.conf['services'][service]["service-class"] == service_class.__name__:
                find_service = self.conf['services'][service]
                break
        strategy_name = find_service['strategy']['name']
        strategy_class = self.strategies[strategy_name]
        if strategy_class is None:
            return None
        strategy = strategy_class(find_service['strategy'])
        return self.Connection(find_service, strategy, stub_class)

    def __load_strategies(self):
        path = 'grpc_router/strategies'
        only_files = [f for f in listdir(path) if isfile(join(path, f))]
        for file in only_files:
            last_index_point = file.rindex(".")
            module_spec = importlib.util.spec_from_file_location(file[:last_index_point],
                                                                 str(Path(path, file).resolve()))
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            for attr in dir(module):
                if not attr.startswith("__"):
                    self.strategies[file[:last_index_point]] = getattr(module, attr)
                    break
