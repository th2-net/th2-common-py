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

from abc import ABC
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Dict, List, Optional

import grpc
from prometheus_client import Counter
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcConfiguration, GrpcConnectionConfiguration
from th2_common.schema.grpc.grpc_interceptors import MetricInterceptor
from th2_common.schema.grpc.router.grpc_router import GrpcRouter
import th2_common.schema.metrics.common_metrics as common_metrics


class AbstractGrpcRouter(GrpcRouter, ABC):

    GRPC_INVOKE_CALL_TOTAL = Counter('th2_grpc_invoke_call_total',
                                     'Total number of calling particular gRPC method',
                                     common_metrics.GRPC_LABELS)

    GRPC_RECEIVE_CALL_TOTAL = Counter('th2_grpc_receive_call_total',
                                      'Total number of consuming particular gRPC method',
                                      common_metrics.GRPC_LABELS)

    GRPC_INVOKE_REQUEST_BYTES = Counter('th2_grpc_invoke_call_request_bytes',
                                        'Number of bytes sent to particular gRPC call',
                                        common_metrics.GRPC_LABELS)

    GRPC_RECEIVE_CALL_REQUEST_BYTES = Counter('th2_grpc_receive_call_request_bytes',
                                              'Number of bytes received from particular gRPC call',
                                              common_metrics.GRPC_LABELS)

    GRPC_INVOKE_RESPONSE_BYTES = Counter('th2_grpc_invoke_call_response_bytes',
                                         'Number of bytes received from particular gRPC call',
                                         common_metrics.GRPC_LABELS)

    GRPC_RECEIVE_CALL_RESPONSE_BYTES = Counter('th2_grpc_receive_call_response_bytes',
                                               'Number of bytes sent to particular gRPC call',
                                               common_metrics.GRPC_LABELS)

    def __init__(self,
                 grpc_configuration: GrpcConfiguration,
                 grpc_router_configuration: GrpcConnectionConfiguration) -> None:
        self.grpc_configuration = grpc_configuration
        self.grpc_router_configuration = grpc_router_configuration
        self.servers: List[grpc.Server] = []
        self.channels: Dict[str, grpc._channel.Channel] = {}

    def __add_insecure_port(self, server: grpc.Server) -> None:
        if self.grpc_configuration.server.host is None:
            server.add_insecure_port(f'[::]:{self.grpc_configuration.server.port}')
        else:
            server.add_insecure_port(
                f'{self.grpc_configuration.server.host}:{self.grpc_configuration.server.port}'
            )

    @property
    def server(self) -> grpc.Server:
        """
        Creates gRPC server.

            Returns:
                grpc.Server: A server object.
        """
        interceptor = [
            MetricInterceptor(self.GRPC_RECEIVE_CALL_TOTAL,
                              self.GRPC_RECEIVE_CALL_REQUEST_BYTES,
                              self.GRPC_RECEIVE_CALL_RESPONSE_BYTES)
        ]
        server = grpc.server(ThreadPoolExecutor(max_workers=self.grpc_router_configuration.workers),
                             options=self.grpc_router_configuration.request_size_limit,
                             interceptors=interceptor)
        self.__add_insecure_port(server)
        self.servers.append(server)

        return server

    @property
    def async_server(self) -> grpc.aio.Server:
        """
        Creates asynchronous gRPC server.

            Returns:
                grpc.aio.Server: An asynchronous server object.
        """
        server = grpc.aio.server()
        self.__add_insecure_port(server)
        self.servers.append(server)

        return server

    def close(self, grace: Optional[int] = None) -> None:
        for server in self.servers:
            server.stop(grace)

        for channel in self.channels.values():
            channel.close()
