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


import argparse

from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.factory.abstract_common_factory import AbstractCommonFactory
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter


class CommonFactory(AbstractCommonFactory):
    CONFIG_DEFAULT_PATH = '/var/th2/config/'

    RABBIT_MQ_FILE_NAME = 'rabbitMQ.json'
    ROUTER_MQ_FILE_NAME = 'mq.json'
    ROUTER_GRPC_FILE_NAME = 'grpc.json'
    CRADLE_FILE_NAME = 'cradle.json'
    CUSTOM_FILE_NAME = 'custom.json'

    def __init__(self,
                 rabbit_mq=CONFIG_DEFAULT_PATH + RABBIT_MQ_FILE_NAME,
                 router_mq=CONFIG_DEFAULT_PATH + ROUTER_MQ_FILE_NAME,
                 router_grpc=CONFIG_DEFAULT_PATH + ROUTER_GRPC_FILE_NAME,
                 cradle=CONFIG_DEFAULT_PATH + CRADLE_FILE_NAME,
                 custom=CONFIG_DEFAULT_PATH + CUSTOM_FILE_NAME,

                 message_parsed_batch_router_class=RabbitParsedBatchRouter,
                 message_raw_batch_router_class=RabbitRawBatchRouter,
                 event_batch_router_class=EventBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:

        self.rabbit_mq = rabbit_mq
        self.router_mq = router_mq
        self.router_grpc = router_grpc
        self.cradle = cradle
        self.custom = custom

        super().__init__(message_parsed_batch_router_class, message_raw_batch_router_class,
                         event_batch_router_class, grpc_router_class)

    @staticmethod
    def calculate_path(parsed_args, name_attr, path_default) -> str:
        if parsed_args.__contains__(name_attr):
            return parsed_args.__getattribute__(name_attr)
        else:
            return CommonFactory.CONFIG_DEFAULT_PATH + path_default

    @staticmethod
    def create_from_arguments(args):
        parser = argparse.ArgumentParser()
        parser.add_argument('--rabbitConfiguration',
                            help='path to json file with RabbitMQ configuration')
        parser.add_argument('--messageRouterConfiguration',
                            help='path to json file with configuration for MessageRouter')
        parser.add_argument('--grpcRouterConfiguration',
                            help='path to json file with configuration for GrpcRouter')
        parser.add_argument('--cradleConfiguration',
                            help='path to json file with configuration for cradle')
        parser.add_argument('--customConfiguration',
                            help='path to json file with custom configuration')
        result = parser.parse_args(args)

        return CommonFactory(
            rabbit_mq=CommonFactory.calculate_path(result, 'rabbitConfiguration', CommonFactory.RABBIT_MQ_FILE_NAME),
            router_mq=CommonFactory.calculate_path(result, 'messageRouterConfiguration',
                                                   CommonFactory.ROUTER_MQ_FILE_NAME),
            router_grpc=CommonFactory.calculate_path(result, 'grpcRouterConfiguration',
                                                     CommonFactory.ROUTER_GRPC_FILE_NAME),
            cradle=CommonFactory.calculate_path(result, 'cradleConfiguration', CommonFactory.CRADLE_FILE_NAME),
            custom=CommonFactory.calculate_path(result, 'customConfiguration', CommonFactory.CUSTOM_FILE_NAME),
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
