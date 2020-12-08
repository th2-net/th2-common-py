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
from kubernetes import client, config
from json import loads, dumps
from os import mkdir, getcwd
import logging

LOGGER = logging.getLogger(__name__)


class CommonFactory(AbstractCommonFactory):
    CONFIG_DEFAULT_PATH = '/var/th2/config/'

    RABBIT_MQ_CONFIG_FILENAME = 'rabbitMQ.json'
    MQ_ROUTER_CONFIG_FILENAME = 'mq.json'
    GRPC_ROUTER_CONFIG_FILENAME = 'grpc.json'
    CRADLE_CONFIG_FILENAME = 'cradle.json'
    CUSTOM_CONFIG_FILENAME = 'custom.json'

    #   FIX: Add path to prometheus_config and dictionary as parameters
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

        if hasattr(result, "namespace") and hasattr(result, "box_name"):

            return CommonFactory.create_from_kubernetes(result.namespace, result.box_name)

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
                custom_config_filepath=CommonFactory.calculate_path(result, 'customConfiguration',
                                                                    CommonFactory.CUSTOM_CONFIG_FILENAME)
            )

    @staticmethod
    def create_from_kubernetes(namespace, box_name):
        global grpc, custom, mq, cradle, rabbit

        config.load_kube_config()
        kube_contexts = config.kube_config.list_kube_config_contexts()

        v1 = client.CoreV1Api()

        config_maps = v1.list_namespaced_config_map(namespace)
        config_maps_dict = config_maps.to_dict()

        service_in_namespace_service = v1.list_namespaced_service("service").to_dict()

        services = v1.list_namespaced_service(namespace)
        services_dict = services.to_dict()

        config_dir = "generated_configs"

        grpc_path = config_dir + "/" + CommonFactory.GRPC_ROUTER_CONFIG_FILENAME
        custom_path = config_dir + "/" + CommonFactory.CUSTOM_CONFIG_FILENAME
        mq_path = config_dir + "/" + CommonFactory.MQ_ROUTER_CONFIG_FILENAME
        cradle_path = config_dir + "/" + CommonFactory.CRADLE_CONFIG_FILENAME
        rabbit_path = config_dir + "/" + CommonFactory.RABBIT_MQ_CONFIG_FILENAME
        dictionary_path = config_dir + "/" + "dictionary.json"
        prometheus_path = config_dir + "/" + "prometheus.json"

        for config_map in config_maps_dict['items']:

            if config_map['metadata']['name'] == box_name + '-app-config':
                box_data = config_map['data']

                grpc = loads(box_data['grpc.json'])
                custom = loads(box_data['custom.json'])
                mq = loads(box_data['mq.json'])

                for grpc_service in grpc['services']:

                    endpoints = grpc['services'][grpc_service]['endpoints']

                    for endpoint in endpoints:

                        endpoint_port = endpoints[endpoint]['port']

                        for service in services_dict['items']:

                            if service['metadata']['name'] == box_name:

                                ports = service['spec']['ports']

                                for port in ports:

                                    if port['target_port'] == endpoint_port:
                                        grpc['services'][grpc_service]['endpoints'][endpoint]['port'] = \
                                            port['node_port']

                                        grpc['services'][grpc_service]['endpoints'][endpoint]['host'] = \
                                            kube_contexts[1]['name']
                                    break
                            break

        for cassandra_config_map in config_maps_dict['items']:

            if cassandra_config_map['metadata']['name'] == 'cradle':

                cradle = loads(cassandra_config_map['data']['cradle.json'])

                cradle['host'] = kube_contexts[1]['name']

                for item in service_in_namespace_service['items']:

                    if item['metadata']['name'] == 'cassandra-schema':

                        for port in item['spec']['ports']:

                            if port['port'] == int(cradle['port']):
                                cradle['port'] = port['node_port']
                                break
                break

        for rabbit_config_map in config_maps_dict['items']:
            if rabbit_config_map['metadata']['name'] == 'rabbit-mq-app-config':
                rabbit = loads(rabbit_config_map['data']['rabbitMQ.json'])
                rabbit['host'] = kube_contexts[1]['name']
                rabbit['username'] = kube_contexts[1]['context']['user']

                for item in service_in_namespace_service['items']:

                    if item['metadata']['name'] == 'rabbitmq-schema':

                        for port in item['spec']['ports']:

                            if port['port'] == int(rabbit['port']):
                                rabbit['port'] = port['node_port']
                                break
                break

        dictionary = CommonFactory.get_dictionary(box_name, v1.list_config_map_for_all_namespaces())

        try:
            mkdir(config_dir)
            LOGGER.info("Directory %s is created at %s" % (config_dir, getcwd()))
        except OSError:
            LOGGER.info("All configuration in the %s folder are overridden" % (getcwd() + "/" + config_dir))

        try:
            grpc_file = open(grpc_path, "w")
            grpc_file.write(dumps(grpc))

            custom_file = open(custom_path, "w")
            custom_file.write(dumps(custom))

            mq_file = open(mq_path, "w")
            mq_file.write(dumps(mq))

            cradle_file = open(cradle_path, "w")
            cradle_file.write(dumps(cradle))

            rabbit_file = open(rabbit_path, "w")
            rabbit_file.write(dumps(rabbit))

            dictionary_file = open(dictionary_path, "w")
            dictionary_file.write(dumps(dictionary))

            prometheus_file = open(prometheus_path, "w")
            prometheus_file.write("{\"host\":\"\",\"port\":25565,\"enabled\":false}")

        finally:
            grpc_file.close()
            custom_file.close()
            mq_file.close()
            cradle_file.close()
            rabbit_file.close()
            dictionary_file.close()
            prometheus_file.close()

        return CommonFactory(
            rabbit_mq_config_filepath=rabbit_path,
            mq_router_config_filepath=mq_path,
            grpc_router_config_filepath=grpc_path,
            cradle_config_filepath=cradle_path,
            custom_config_filepath=custom_path
        )

    @staticmethod
    def get_dictionary(box_name, config_maps):
        for config_map in config_maps.to_dict()['items']:
            if config_map['metadata']['name'].startswith(box_name) & config_map['metadata']['name'].endswith(
                    '-dictionary'):
                return config_map

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
