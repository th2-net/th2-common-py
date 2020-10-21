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
import os
from abc import ABC, abstractmethod
from threading import Lock

from th2common.schema.cradle.cradle_configuration import CradleConfiguration
from th2common.schema.event.event_batch_router import EventBatchRouter
from th2common.schema.grpc.configuration.grpc_router_configuration import GrpcRouterConfiguration
from th2common.schema.grpc.router.grpc_router import GrpcRouter
from th2common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2common.schema.message.configuration.message_router_configuration import MessageRouterConfiguration
from th2common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter
from th2common.schema.message.message_router import MessageRouter


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

    def create_message_router_raw_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with RawMessageBatch
        """
        return self.message_router_raw_batch_class(self._create_rabbit_mq_configuration(),
                                                   self._create_message_router_configuration())

    def create_message_router_parsed_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with MessageBatch
        """
        return self.message_router_parsed_batch_class(self._create_rabbit_mq_configuration(),
                                                      self._create_message_router_configuration())

    def create_event_router_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with EventBatch
        """
        return self.event_router_batch_class(self._create_rabbit_mq_configuration(),
                                             self._create_message_router_configuration())

    def create_grpc_router(self) -> GrpcRouter:
        return self.grpc_router_class(self._create_grpc_router_configuration())

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
