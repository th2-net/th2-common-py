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
import base64
import logging
import os
import sys
from json import loads, dump
from os import mkdir, getcwd

from kubernetes import client, config

from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.factory.abstract_common_factory import AbstractCommonFactory
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_router import RabbitParsedBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_router import RabbitRawBatchRouter

logger = logging.getLogger()


class CommonFactory(AbstractCommonFactory):
    CONFIG_DEFAULT_PATH = '/var/th2/config/'

    RABBIT_MQ_CONFIG_FILENAME = 'rabbitMQ.json'
    MQ_ROUTER_CONFIG_FILENAME = 'mq.json'
    GRPC_ROUTER_CONFIG_FILENAME = 'grpc.json'
    CRADLE_CONFIG_FILENAME = 'cradle.json'
    PROMETHEUS_CONFIG_FILENAME = 'prometheus.json'
    CUSTOM_CONFIG_FILENAME = 'custom.json'
    DICTIONARY_FILENAME = 'dictionary.json'
    BOX_FILE_NAME = 'box.json'

    RABBITMQ_SECRET_NAME = 'rabbitmq'
    CASSANDRA_SECRET_NAME = 'cassandra'
    RABBITMQ_PASSWORD_KEY = 'rabbitmq-password'
    CASSANDRA_PASSWORD_KEY = 'cassandra-password'

    KEY_RABBITMQ_PASS = 'RABBITMQ_PASS'
    KEY_CASSANDRA_PASS = 'CASSANDRA_PASS'

    #   FIX: Add path to dictionary as a parameter
    def __init__(self,
                 config_path=None,
                 rabbit_mq_config_filepath=CONFIG_DEFAULT_PATH + RABBIT_MQ_CONFIG_FILENAME,
                 mq_router_config_filepath=CONFIG_DEFAULT_PATH + MQ_ROUTER_CONFIG_FILENAME,
                 grpc_router_config_filepath=CONFIG_DEFAULT_PATH + GRPC_ROUTER_CONFIG_FILENAME,
                 cradle_config_filepath=CONFIG_DEFAULT_PATH + CRADLE_CONFIG_FILENAME,
                 prometheus_config_filepath=CONFIG_DEFAULT_PATH + PROMETHEUS_CONFIG_FILENAME,
                 custom_config_filepath=CONFIG_DEFAULT_PATH + CUSTOM_CONFIG_FILENAME,

                 message_parsed_batch_router_class=RabbitParsedBatchRouter,
                 message_raw_batch_router_class=RabbitRawBatchRouter,
                 message_group_batch_router_class=RabbitMessageGroupBatchRouter,
                 event_batch_router_class=EventBatchRouter,
                 grpc_router_class=DefaultGrpcRouter) -> None:

        if config_path is not None:
            CommonFactory.CONFIG_DEFAULT_PATH = config_path
            rabbit_mq_config_filepath = config_path + CommonFactory.RABBIT_MQ_CONFIG_FILENAME
            mq_router_config_filepath = config_path + CommonFactory.MQ_ROUTER_CONFIG_FILENAME
            grpc_router_config_filepath = config_path + CommonFactory.GRPC_ROUTER_CONFIG_FILENAME
            cradle_config_filepath = config_path + CommonFactory.CRADLE_CONFIG_FILENAME
            prometheus_config_filepath = config_path + CommonFactory.PROMETHEUS_CONFIG_FILENAME
            custom_config_filepath = config_path + CommonFactory.CUSTOM_CONFIG_FILENAME

        self.rabbit_mq_config_filepath = rabbit_mq_config_filepath
        self.mq_router_config_filepath = mq_router_config_filepath
        self.grpc_router_config_filepath = grpc_router_config_filepath
        self.cradle_config_filepath = cradle_config_filepath
        self.prometheus_config_filepath = prometheus_config_filepath
        self.custom_config_filepath = custom_config_filepath

        super().__init__(message_parsed_batch_router_class, message_raw_batch_router_class,
                         message_group_batch_router_class, event_batch_router_class, grpc_router_class)

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
        parser.add_argument('--prometheusConfiguration',
                            help='path to json file with configuration for prometheus metrics server')
        parser.add_argument('--namespace',
                            help='namespace in Kubernetes to find config maps related to the target')
        parser.add_argument('--boxName',
                            help='name of the target th2 box placed in the specified namespace in Kubernetes')
        parser.add_argument('--contextName',
                            help='context name to choose the context from Kube config')
        parser.add_argument('--customConfiguration',
                            help='path to json file with custom configuration')
        result = parser.parse_args(args)

        if hasattr(result, 'namespace') and hasattr(result, 'boxName'):
            if hasattr(result, 'contextName'):
                return CommonFactory.create_from_kubernetes(result.namespace, result.boxName, result.contextName)
            else:
                return CommonFactory.create_from_kubernetes(result.namespace, result.boxName)

        else:

            return CommonFactory(
                rabbit_mq_config_filepath=CommonFactory.calculate_path(result, 'rabbitConfiguration',
                                                                       CommonFactory.RABBIT_MQ_CONFIG_FILENAME),
                mq_router_config_filepath=CommonFactory.calculate_path(result, 'messageRouterConfiguration',
                                                                       CommonFactory.MQ_ROUTER_CONFIG_FILENAME),
                grpc_router_config_filepath=CommonFactory.calculate_path(result, 'grpcRouterConfiguration',
                                                                         CommonFactory.GRPC_ROUTER_CONFIG_FILENAME),
                cradle_config_filepath=CommonFactory.calculate_path(result, 'cradleConfiguration',
                                                                    CommonFactory.CRADLE_CONFIG_FILENAME),
                prometheus_config_filepath=CommonFactory.calculate_path(result, 'prometheusConfiguration',
                                                                        CommonFactory.PROMETHEUS_CONFIG_FILENAME),
                custom_config_filepath=CommonFactory.calculate_path(result, 'customConfiguration',
                                                                    CommonFactory.CUSTOM_CONFIG_FILENAME)
            )

    @staticmethod
    def create_from_kubernetes(namespace, box_name, context_name=None):

        config.load_kube_config(context=context_name)

        v1 = client.CoreV1Api()

        config_maps = v1.list_namespaced_config_map(namespace)
        config_maps_dict = config_maps.to_dict()

        config_dir = 'generated_configs'

        grpc_path = config_dir + '/' + CommonFactory.GRPC_ROUTER_CONFIG_FILENAME
        custom_path = config_dir + '/' + CommonFactory.CUSTOM_CONFIG_FILENAME
        mq_path = config_dir + '/' + CommonFactory.MQ_ROUTER_CONFIG_FILENAME
        cradle_path = config_dir + '/' + CommonFactory.CRADLE_CONFIG_FILENAME
        rabbit_path = config_dir + '/' + CommonFactory.RABBIT_MQ_CONFIG_FILENAME
        dictionary_path = config_dir + '/' + CommonFactory.DICTIONARY_FILENAME
        prometheus_path = config_dir + '/' + CommonFactory.PROMETHEUS_CONFIG_FILENAME
        box_configuration_path = config_dir + '/' + CommonFactory.BOX_FILE_NAME

        rabbit_mq_encoded_password = v1.read_namespaced_secret(CommonFactory.RABBITMQ_SECRET_NAME, namespace).data\
            .get(CommonFactory.RABBITMQ_PASSWORD_KEY)

        cassandra_encoded_password = v1.read_namespaced_secret(CommonFactory.CASSANDRA_SECRET_NAME, namespace).data\
            .get(CommonFactory.CASSANDRA_PASSWORD_KEY)

        os.environ[CommonFactory.KEY_RABBITMQ_PASS] = CommonFactory._decode_from_base64(rabbit_mq_encoded_password)
        os.environ[CommonFactory.KEY_CASSANDRA_PASS] = CommonFactory._decode_from_base64(cassandra_encoded_password)

        try:
            mkdir(config_dir)
            logger.info('Directory %s is created at %s' % (config_dir, getcwd()))
        except OSError:
            logger.info('All configuration in the %s folder are overridden' % (getcwd() + '/' + config_dir))

        CommonFactory._get_config(config_maps_dict, box_name + '-app-config',
                                  CommonFactory.GRPC_ROUTER_CONFIG_FILENAME, grpc_path)

        CommonFactory._get_config(config_maps_dict, box_name + '-app-config',
                                  CommonFactory.CUSTOM_CONFIG_FILENAME, custom_path)

        CommonFactory._get_config(config_maps_dict, box_name + '-app-config',
                                  CommonFactory.MQ_ROUTER_CONFIG_FILENAME, mq_path)

        CommonFactory._get_config(config_maps_dict, 'cradle-external', CommonFactory.CRADLE_CONFIG_FILENAME, cradle_path)

        CommonFactory._get_config(config_maps_dict, 'rabbit-mq-external-app-config',
                                  CommonFactory.RABBIT_MQ_CONFIG_FILENAME, rabbit_path)

        CommonFactory._get_config(config_maps_dict, box_name + '-app-config',
                                  CommonFactory.PROMETHEUS_CONFIG_FILENAME, prometheus_path)

        CommonFactory._get_dictionary(box_name, v1.list_config_map_for_all_namespaces(), dictionary_path)

        CommonFactory._get_box_config(config_maps_dict, box_name + '-app-config',
                                      CommonFactory.BOX_FILE_NAME, box_configuration_path)

        return CommonFactory(
            rabbit_mq_config_filepath=rabbit_path,
            mq_router_config_filepath=mq_path,
            grpc_router_config_filepath=grpc_path,
            cradle_config_filepath=cradle_path,
            prometheus_config_filepath=prometheus_path,
            custom_config_filepath=custom_path
        )

    @staticmethod
    def _decode_from_base64(data):
        data_bytes = data.encode("ascii")
        data_string_bytes = base64.b64decode(data_bytes)
        return data_string_bytes.decode("ascii")

    @staticmethod
    def _get_dictionary(box_name, config_maps, dictionary_path):
        if 'items' in config_maps.to_dict()['items']:
            try:
                for config_map in config_maps.to_dict()['items']:
                    if config_map['metadata']['name'].startswith(box_name) & \
                            config_map['metadata']['name'].endswith('-dictionary'):
                        with open(dictionary_path, 'w') as dictionary_file:
                            dump(config_map, dictionary_file)
            except KeyError:
                logger.error(f'dictionary config map\'s metadata not valid. Some keys are absent.')
            except IOError:
                logger.error('Failed to write file for dictionary.')

    @staticmethod
    def _get_config(config_maps_dict, name, config_file_name, path):
        try:
            if 'items' in config_maps_dict:
                for config_map in config_maps_dict['items']:
                    if config_map['metadata']['name'] == name:
                        box_data = config_map['data']
                        config_data = loads(box_data[config_file_name])

                        with open(path, 'w') as file:
                            dump(config_data, file)
        except KeyError:
            logger.error(f'{name}\'s data not valid. Some keys are absent.')
        except IOError:
            logger.error(f'Failed to write ${name} config.')

    @staticmethod
    def _get_box_config(config_maps_dict, name, config_file_name, path):
        try:
            if 'items' in config_maps_dict:
                for config_map in config_maps_dict['items']:
                    if config_map['metadata']['name'] == name:
                        box_data = config_map['data']
                        config_data = loads(box_data[config_file_name])

                        with open(path, 'w') as file:
                            dump(config_data, file)
        except KeyError:
            try:
                with open(path, 'w') as file:
                    dump({'boxName': name}, file)
            except IOError:
                logger.error(f'Failed to write ${name} config.')
        except IOError:
            logger.error(f'Failed to write ${name} config.')

    def _path_to_rabbit_mq_configuration(self) -> str:
        return self.rabbit_mq_config_filepath

    def _path_to_message_router_configuration(self) -> str:
        return self.mq_router_config_filepath

    def _path_to_grpc_router_configuration(self) -> str:
        return self.grpc_router_config_filepath

    def _path_to_cradle_configuration(self) -> str:
        return self.cradle_config_filepath

    def _path_to_prometheus_configuration(self) -> str:
        return self.prometheus_config_filepath

    def _path_to_custom_configuration(self) -> str:
        return self.custom_config_filepath
