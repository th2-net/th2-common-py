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
import sys

from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.factory.abstract_common_factory import AbstractCommonFactory
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter


class CommonFactory(AbstractCommonFactory):
    CONFIG_DEFAULT_PATH = '/var/th2/config/'

    RABBIT_MQ_CONFIG_FILENAME = 'rabbitMQ.json'
    MQ_ROUTER_CONFIG_FILENAME = 'mq.json'
    GRPC_ROUTER_CONFIG_FILENAME = 'grpc.json'
    CRADLE_CONFIG_FILENAME = 'cradle.json'
    CUSTOM_CONFIG_FILENAME = 'custom.json'

    def __init__(self,
                 config_path=None,
                 rabbit_mq_config_filepath=CONFIG_DEFAULT_PATH + RABBIT_MQ_CONFIG_FILENAME,
                 mq_router_config_filepath=CONFIG_DEFAULT_PATH + MQ_ROUTER_CONFIG_FILENAME,
                 grpc_router_config_filepath=CONFIG_DEFAULT_PATH + GRPC_ROUTER_CONFIG_FILENAME,
                 cradle_config_filepath=CONFIG_DEFAULT_PATH + CRADLE_CONFIG_FILENAME,
                 custom_config_filepath=CONFIG_DEFAULT_PATH + CUSTOM_CONFIG_FILENAME,

                 message_parsed_batch_router_class=RabbitParsedBatchRouter,
                 message_raw_batch_router_class=RabbitRawBatchRouter,
                 event_batch_router_class=EventBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:

        if config_path is not None:
            CommonFactory.CONFIG_DEFAULT_PATH = config_path
            rabbit_mq_config_filepath = config_path + CommonFactory.RABBIT_MQ_CONFIG_FILENAME
            mq_router_config_filepath = config_path + CommonFactory.MQ_ROUTER_CONFIG_FILENAME
            grpc_router_config_filepath = config_path + CommonFactory.GRPC_ROUTER_CONFIG_FILENAME
            cradle_config_filepath = config_path + CommonFactory.CRADLE_CONFIG_FILENAME
            custom_config_filepath = config_path + CommonFactory.CUSTOM_CONFIG_FILENAME

        self.rabbit_mq_config_filepath = rabbit_mq_config_filepath
        self.mq_router_config_filepath = mq_router_config_filepath
        self.grpc_router_config_filepath = grpc_router_config_filepath
        self.cradle_config_filepath = cradle_config_filepath
        self.custom_config_filepath = custom_config_filepath

        super().__init__(message_parsed_batch_router_class, message_raw_batch_router_class,
                         event_batch_router_class, grpc_router_class)

    @staticmethod
    def calculate_path(parsed_args, name_attr, path_default) -> str:
        if parsed_args.__contains__(name_attr):
            return parsed_args.__getattribute__(name_attr)
        else:
            return CommonFactory.CONFIG_DEFAULT_PATH + path_default

    @staticmethod
    def create_from_arguments(args=None):
        if args is None:
            args = sys.argv[1:]

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
            rabbit_mq_config_filepath=CommonFactory.calculate_path(result, 'rabbitConfiguration',
                                                                   CommonFactory.RABBIT_MQ_CONFIG_FILENAME),
            mq_router_config_filepath=CommonFactory.calculate_path(result, 'messageRouterConfiguration',
                                                                   CommonFactory.MQ_ROUTER_CONFIG_FILENAME),
            grpc_router_config_filepath=CommonFactory.calculate_path(result, 'grpcRouterConfiguration',
                                                                     CommonFactory.GRPC_ROUTER_CONFIG_FILENAME),
            cradle_config_filepath=CommonFactory.calculate_path(result, 'cradleConfiguration',
                                                                CommonFactory.CRADLE_CONFIG_FILENAME),
            custom_config_filepath=CommonFactory.calculate_path(result, 'customConfiguration',
                                                                CommonFactory.CUSTOM_CONFIG_FILENAME)
        )

    def _path_to_rabbit_mq_configuration(self) -> str:
        return self.rabbit_mq_config_filepath

    def _path_to_message_router_configuration(self) -> str:
        return self.mq_router_config_filepath

    def _path_to_grpc_router_configuration(self) -> str:
        return self.grpc_router_config_filepath

    def _path_to_cradle_configuration(self) -> str:
        return self.cradle_config_filepath

    def _path_to_custom_configuration(self) -> str:
        return self.custom_config_filepath
