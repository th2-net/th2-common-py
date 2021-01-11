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


from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor

import grpc

from th2_common.schema.grpc.configuration.grpc_router_configuration import GrpcRouterConfiguration
from th2_common.schema.grpc.router.grpc_router import GrpcRouter


class AbstractGrpcRouter(GrpcRouter, ABC):

    def __init__(self, configuration: GrpcRouterConfiguration) -> None:
        self.configuration = configuration
        self.servers = []
        self.channels = {}

    def start_server(self, *services) -> grpc.Server:
        server = grpc.server(ThreadPoolExecutor(max_workers=self.configuration.serverConfiguration.workers))

        if self.configuration.serverConfiguration.host is None:
            server.add_insecure_port(f'[::]:{self.configuration.serverConfiguration.port}')
        else:
            server.add_insecure_port(
                f'{self.configuration.serverConfiguration.host}:{self.configuration.serverConfiguration.port}')

        self.servers.append(server)

        return server

    def close(self, grace=None):
        for server in self.servers:
            server.stop(grace)

        for channel in self.channels.values():
            channel.close()
