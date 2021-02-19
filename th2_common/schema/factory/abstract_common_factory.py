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


import json
import logging
import os
from abc import ABC, abstractmethod
from threading import Lock

from th2_common.schema.cradle.cradle_configuration import CradleConfiguration
from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.grpc.configuration.grpc_router_configuration import GrpcRouterConfiguration
from th2_common.schema.grpc.router.grpc_router import GrpcRouter
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.configuration.message_router_configuration import MessageRouterConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter
from th2_common.schema.message.message_router import MessageRouter
from th2_common.schema.metrics.prometheus_configuration import PrometheusConfiguration
from th2_common.schema.metrics.prometheus_server import PrometheusServer

logger = logging.getLogger()


class AbstractCommonFactory(ABC):

    def __init__(self,
                 message_parsed_batch_router_class=RabbitParsedBatchRouter,
                 message_raw_batch_router_class=RabbitRawBatchRouter,
                 event_batch_router_class=EventBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:

        self.rabbit_mq_configuration = self._create_rabbit_mq_configuration()
        self.message_router_configuration = self._create_message_router_configuration()
        self.grpc_router_configuration = None

        self.message_parsed_batch_router_class = message_parsed_batch_router_class
        self.message_raw_batch_router_class = message_raw_batch_router_class
        self.event_batch_router_class = event_batch_router_class
        self.grpc_router_class = grpc_router_class

        self._message_parsed_batch_router = None
        self._message_raw_batch_router = None
        self._event_batch_router = None
        self._grpc_router = None

        self._connection_manager = ConnectionManager(self.rabbit_mq_configuration)

        self.prometheus_config = PrometheusConfiguration()
        self.prometheus = PrometheusServer(self.prometheus_config.port, self.prometheus_config.host)
        if self.prometheus_config.enabled is True:
            self.prometheus.run()

    @property
    def message_parsed_batch_router(self) -> MessageRouter:
        """
        Created MessageRouter which work with MessageBatch
        """
        if self._message_parsed_batch_router is None:
            self._message_parsed_batch_router = self.message_parsed_batch_router_class(self._connection_manager,
                                                                                       self.message_router_configuration
                                                                                       )

        return self._message_parsed_batch_router

    @property
    def message_raw_batch_router(self) -> MessageRouter:
        """
        Created MessageRouter which work with RawMessageBatch
        """
        if self._message_raw_batch_router is None:
            self._message_raw_batch_router = self.message_raw_batch_router_class(self._connection_manager,
                                                                                 self.message_router_configuration)
        return self._message_raw_batch_router

    @property
    def event_batch_router(self) -> MessageRouter:
        """
        Created MessageRouter which work with EventBatch
        """
        if self._event_batch_router is None:
            self._event_batch_router = self.event_batch_router_class(self._connection_manager,
                                                                     self.message_router_configuration)

        return self._event_batch_router

    @property
    def grpc_router(self) -> GrpcRouter:
        if self._grpc_router is None:
            if self.grpc_router_configuration is None:
                self.grpc_router_configuration = self._create_grpc_router_configuration()
            self._grpc_router = self.grpc_router_class(self.grpc_router_configuration)

        return self._grpc_router

    def close(self):
        logger.info('Closing Common Factory')

        if self._message_raw_batch_router is not None:
            try:
                self._message_raw_batch_router.close()
            except Exception:
                logger.exception('Error during closing Message Router (Message Raw Batch)')

        if self._message_parsed_batch_router is not None:
            try:
                self._message_parsed_batch_router.close()
            except Exception:
                logger.exception('Error during closing Message Router (Message Parsed Batch)')

        if self._event_batch_router is not None:
            try:
                self._event_batch_router.close()
            except Exception:
                logger.exception('Error during closing Message Router (Event Batch)')

        if self._grpc_router is not None:
            try:
                self._grpc_router.close()
            except Exception:
                logger.exception('Error during closing gRPC Router')

        if self._connection_manager is not None:
            self._connection_manager.close()

        if self.prometheus.stopped is False:
            self.prometheus.stop()

    @staticmethod
    def read_configuration(filepath):
        with open(filepath, 'r') as file:
            config_json = file.read()
            config_json_expanded = os.path.expandvars(config_json)
            config_dict = json.loads(config_json_expanded)

        return config_dict

    def create_cradle_configuration(self) -> CradleConfiguration:
        return CradleConfiguration(**self.read_configuration(self._path_to_cradle_configuration()))

    def create_prometheus_configuration(self) -> PrometheusConfiguration:
        return PrometheusConfiguration(**self.read_configuration(self._path_to_prometheus_configuration()))

    def create_custom_configuration(self) -> dict:
        return self.read_configuration(self._path_to_custom_configuration())

    def _create_rabbit_mq_configuration(self) -> RabbitMQConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if not hasattr(self, 'rabbit_mq_configuration'):
                config_dict = self.read_configuration(self._path_to_rabbit_mq_configuration())
                self.rabbit_mq_configuration = RabbitMQConfiguration(**config_dict)
        finally:
            lock.release()
        return self.rabbit_mq_configuration

    def _create_message_router_configuration(self) -> MessageRouterConfiguration:
        lock = Lock()
        with lock:
            if not hasattr(self, 'message_router_configuration'):
                config_dict = self.read_configuration(self._path_to_message_router_configuration())
                self.message_router_configuration = MessageRouterConfiguration(**config_dict)
        return self.message_router_configuration

    def _create_grpc_router_configuration(self) -> GrpcRouterConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            config_dict = self.read_configuration(self._path_to_grpc_router_configuration())
            self.grpc_router_configuration = GrpcRouterConfiguration(**config_dict)
        finally:
            lock.release()
        return self.grpc_router_configuration

    @abstractmethod
    def _path_to_rabbit_mq_configuration(self) -> str:
        pass

    @abstractmethod
    def _path_to_message_router_configuration(self) -> str:
        pass

    @abstractmethod
    def _path_to_grpc_router_configuration(self) -> str:
        pass

    @abstractmethod
    def _path_to_cradle_configuration(self) -> str:
        pass

    @abstractmethod
    def _path_to_prometheus_configuration(self) -> str:
        pass

    @abstractmethod
    def _path_to_custom_configuration(self) -> str:
        pass
