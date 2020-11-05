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
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter
from th2_common.schema.message.message_router import MessageRouter


logger = logging.getLogger()


class AbstractCommonFactory(ABC):

    def __init__(self,
                 message_router_raw_batch_class=RabbitRawBatchRouter,
                 message_router_parsed_batch_class=RabbitParsedBatchRouter,
                 event_router_batch_class=EventBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:
        self.rabbit_mq_configuration = None
        self.message_router_configuration = None
        self.grpc_router_configuration = None
        self.message_router_raw_batch_class = message_router_raw_batch_class
        self.message_router_parsed_batch_class = message_router_parsed_batch_class
        self.event_router_batch_class = event_router_batch_class
        self.grpc_router_class = grpc_router_class

        self.message_router_raw_batch = None
        self.message_router_parsed_batch = None
        self.event_router_batch = None
        self.grpc_router = None

    def create_message_router_raw_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with RawMessageBatch
        """
        self.message_router_raw_batch = self.message_router_raw_batch_class(self._create_rabbit_mq_configuration(),
                                                                            self._create_message_router_configuration())
        return self.message_router_raw_batch

    def create_message_router_parsed_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with MessageBatch
        """
        self.message_router_parsed_batch = self.message_router_parsed_batch_class(
            self._create_rabbit_mq_configuration(),
            self._create_message_router_configuration())

        return self.message_router_parsed_batch

    def create_event_router_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with EventBatch
        """
        self.event_router_batch = self.event_router_batch_class(self._create_rabbit_mq_configuration(),
                                                                self._create_message_router_configuration())

        return self.event_router_batch

    def create_grpc_router(self) -> GrpcRouter:
        self.grpc_router = self.grpc_router_class(self._create_grpc_router_configuration())

        return self.grpc_router

    def close(self):
        logger.info('Closing Common Factory')

        if self.message_router_raw_batch is not None:
            try:
                self.message_router_raw_batch.close()
            except Exception as e:
                logger.error('Error during closing Message Router (Message Raw Batch)', e)

        if self.message_router_parsed_batch is not None:
            try:
                self.message_router_parsed_batch.close()
            except Exception as e:
                logger.error('Error during closing Message Router (Message Parsed Batch)', e)

        if self.event_router_batch is not None:
            try:
                self.event_router_batch.close()
            except Exception as e:
                logger.error('Error during closing Message Router (Event Batch)', e)

        if self.grpc_router is not None:
            try:
                self.grpc_router.close()
            except Exception as e:
                logger.error('Error during closing gRPC Router', e)

    @staticmethod
    def read_configuration(filepath):
        with open(filepath, 'r') as file:
            config_json = file.read()
            config_json_expanded = os.path.expandvars(config_json)
            config_dict = json.loads(config_json_expanded)

        return config_dict

    def create_cradle_configuration(self) -> CradleConfiguration:
        config_dict = self.read_configuration(self._path_to_cradle_configuration())
        return CradleConfiguration(**config_dict)

    def create_custom_configuration(self) -> dict:
        config_dict = self.read_configuration(self._path_to_custom_configuration())
        return config_dict

    def _create_rabbit_mq_configuration(self) -> RabbitMQConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if self.rabbit_mq_configuration is None:
                config_dict = self.read_configuration(self._path_to_rabbit_mq_configuration())
                self.rabbit_mq_configuration = RabbitMQConfiguration(**config_dict)
        finally:
            lock.release()
        return self.rabbit_mq_configuration

    def _create_message_router_configuration(self) -> MessageRouterConfiguration:
        lock = Lock()
        with lock:
            if self.message_router_configuration is None:
                config_dict = self.read_configuration(self._path_to_message_router_configuration())
                self.message_router_configuration = MessageRouterConfiguration(**config_dict)
        return self.message_router_configuration

    def _create_grpc_router_configuration(self) -> GrpcRouterConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if self.grpc_router_configuration is None:
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
    def _path_to_custom_configuration(self) -> str:
        pass
