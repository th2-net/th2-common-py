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

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from threading import Lock

import click

from th2common.schema.cradle import CradleConfiguration
from th2common.schema.grpc.abstract_router import GrpcRouter
from th2common.schema.grpc.configurations import GrpcRouterConfiguration
from th2common.schema.grpc.impl import default_router
from th2common.schema.grpc.impl.default_router import DefaultGrpcRouter
from th2common.schema.message.configurations import MessageRouterConfiguration
from th2common.schema.message.impl.rabbitmq import parsed, raw
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.impl.rabbitmq.parsed import RabbitParsedBatchRouter
from th2common.schema.message.impl.rabbitmq.raw import RabbitRawBatchRouter
from th2common.schema.message.interfaces import MessageRouter


class AbstractFactory(ABC):
    rabbit_mq_configuration = None
    message_router_configuration = None
    grpc_router_configuration = None

    def __init__(self, message_router_parsed_batch_class=RabbitParsedBatchRouter,
                 message_router_raw_batch_class=RabbitRawBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:
        self.message_router_parsed_batch_class = message_router_parsed_batch_class
        self.message_router_raw_batch_class = message_router_raw_batch_class
        self.grpc_router_class = grpc_router_class

    def create_message_router_parsed_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with MessageBatch
        """
        return self.message_router_parsed_batch_class(self._create_rabbit_mq_configuration(),
                                                      self._create_message_router_configuration())

    def create_message_router_raw_batch(self) -> MessageRouter:
        """
        Created MessageRouter which work with RawMessageBatch
        """
        return self.message_router_raw_batch_class(self._create_rabbit_mq_configuration(),
                                                   self._create_message_router_configuration())

    def create_grpc_router(self) -> GrpcRouter:
        return self.grpc_router_class(self._create_grpc_router_configuration())

    def _create_cradle_configuration(self) -> CradleConfiguration:
        file = open(self._path_to_cradle_configuration(), 'r')
        config_json = file.read()
        config_dict = json.loads(config_json)
        return CradleConfiguration(**config_dict)

    def _create_rabbit_mq_configuration(self) -> RabbitMQConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if self.rabbit_mq_configuration is None:
                file = open(self._path_to_rabbit_mq_configuration(), 'r')
                config_json = file.read()
                config_dict = json.loads(config_json)
                self.rabbit_mq_configuration = RabbitMQConfiguration(**config_dict)
        finally:
            lock.release()
        return self.rabbit_mq_configuration

    def _create_message_router_configuration(self) -> MessageRouterConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if self.message_router_configuration is None:
                file = open(self._path_to_message_router_configuration(), 'r')
                config_json = file.read()
                config_dict = json.loads(config_json)
                self.message_router_configuration = MessageRouterConfiguration(**config_dict)
        finally:
            lock.release()
        return self.message_router_configuration

    def _create_grpc_router_configuration(self) -> GrpcRouterConfiguration:
        lock = Lock()
        try:
            lock.acquire()
            if self.grpc_router_configuration is None:
                file = open(self._path_to_grpc_router_configuration(), 'r')
                config_json = file.read()
                config_dict = json.loads(config_json)
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


class CommonFactory(AbstractFactory):
    CONFIG_DEFAULT_PATH = "/var/th2/config/"

    RABBIT_MQ_FILE_NAME = "rabbitMQ.json"
    ROUTER_MQ_FILE_NAME = "mq.json"
    ROUTER_GRPC_FILE_NAME = "grpc.json"
    CRADLE_FILE_NAME = "cradle.json"
    CUSTOM_FILE_NAME = "custom.json"

    def __init__(self, message_router_parsed_batch_class=parsed.RabbitParsedBatchRouter,
                 message_router_raw_batch_class=raw.RabbitRawBatchRouter,
                 grpc_router_class=default_router.DefaultGrpcRouter,
                 rabbit_mq=CONFIG_DEFAULT_PATH + RABBIT_MQ_FILE_NAME,
                 router_mq=CONFIG_DEFAULT_PATH + ROUTER_MQ_FILE_NAME,
                 router_grpc=CONFIG_DEFAULT_PATH + ROUTER_GRPC_FILE_NAME,
                 cradle=CONFIG_DEFAULT_PATH + CRADLE_FILE_NAME,
                 custom=CONFIG_DEFAULT_PATH + CUSTOM_FILE_NAME) -> None:
        super().__init__(message_router_parsed_batch_class, message_router_raw_batch_class, grpc_router_class)
        self.rabbit_mq = rabbit_mq
        self.router_mq = router_mq
        self.router_grpc = router_grpc
        self.cradle = cradle
        self.custom = custom

    @staticmethod
    def calculate_path(path_cmd, path_default) -> str:
        if path_cmd is None or len(path_cmd) == 0:
            return path_default
        return path_cmd

    @staticmethod
    @click.command()
    @click.option('--rabbitConfiguration', help='path to json file with RabbitMQ configuration')
    @click.option('--messageRouterConfiguration', help='path to json file with configuration for MessageRouter')
    @click.option('--grpcRouterConfiguration', help='path to json file with configuration for GrpcRouter')
    @click.option('--cradleConfiguration', help='path to json file with configuration for cradle')
    @click.option('--customConfiguration', help='path to json file with custom configuration')
    def create_from_arguments(rabbitconfiguration, messagerouterconfiguration, grpcrouterconfiguration,
                              cradleconfiguration, customconfiguration):
        return CommonFactory(
            rabbit_mq=CommonFactory.calculate_path(rabbitconfiguration, CommonFactory.RABBIT_MQ_FILE_NAME),
            router_mq=CommonFactory.calculate_path(messagerouterconfiguration, CommonFactory.ROUTER_MQ_FILE_NAME),
            router_grpc=CommonFactory.calculate_path(grpcrouterconfiguration, CommonFactory.ROUTER_GRPC_FILE_NAME),
            cradle=CommonFactory.calculate_path(cradleconfiguration, CommonFactory.CRADLE_FILE_NAME),
            custom=CommonFactory.calculate_path(customconfiguration, CommonFactory.CUSTOM_FILE_NAME),
        )

    def _path_to_rabbit_mq_configuration(self) -> str:
        return self.rabbit_mq

    def _path_to_message_router_configuration(self) -> str:
        return self.router_mq

    def _path_to_grpc_router_configuration(self) -> str:
        return self.router_grpc

    def _path_to_cradle_configuration(self) -> str:
        return self.cradle

    def _path_to_custom_configuration(self) -> str:
        return self.custom
