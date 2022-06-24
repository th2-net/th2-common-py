#   Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

from pathlib import Path
from typing import List

from th2_common.schema.factory.common_factory import CommonFactory
from th2_common.schema.filter.strategy.filter_strategy import FilterStrategy
from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.filter.strategy.impl.default_grpc_filter_strategy import DefaultGrpcFilterStrategy
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcConfiguration, GrpcServiceConfiguration
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter


class TestCommonFactory(CommonFactory):

    CONFIG_DEFAULT_PATH = Path(__file__).parent / 'configuration' / 'json_configuration'

    RABBIT_MQ_CONFIG_FILENAME = 'rabbitMQ.json'
    MQ_ROUTER_CONFIG_FILENAME = 'mq.json'
    CONNECTION_MANAGER_CONFIG_FILENAME = 'mq_router.json'
    GRPC_CONFIG_FILENAME = 'grpc.json'
    GRPC_ROUTER_CONFIG_FILENAME = 'grpc_router.json'
    CRADLE_CONFIG_FILENAME = 'cradle.json'
    PROMETHEUS_CONFIG_FILENAME = 'prometheus.json'

    def __init__(self) -> None:
        self.rabbit_mq_config_filepath = self.CONFIG_DEFAULT_PATH / self.RABBIT_MQ_CONFIG_FILENAME
        self.mq_router_config_filepath = self.CONFIG_DEFAULT_PATH / self.MQ_ROUTER_CONFIG_FILENAME
        self.connection_manager_config_filepath = self.CONFIG_DEFAULT_PATH / self.CONNECTION_MANAGER_CONFIG_FILENAME
        self.grpc_config_filepath = self.CONFIG_DEFAULT_PATH / self.GRPC_CONFIG_FILENAME
        self.grpc_router_config_filepath = self.CONFIG_DEFAULT_PATH / self.GRPC_ROUTER_CONFIG_FILENAME
        self.cradle_config_filepath = self.CONFIG_DEFAULT_PATH / self.CRADLE_CONFIG_FILENAME
        self.prometheus_config_filepath = self.CONFIG_DEFAULT_PATH / self.PROMETHEUS_CONFIG_FILENAME

        self.rabbit_mq_configuration = self._create_rabbit_mq_configuration()
        self.message_router_configuration = self._create_message_router_configuration()
        self.conn_manager_configuration = self._create_conn_manager_configuration()
        self.grpc_configuration = self._create_grpc_configuration()
        self.grpc_router_configuration = self._create_grpc_router_configuration()
        self.cradle_configuration = self.create_cradle_configuration()
        self.prometheus_configuration = self._create_prometheus_configuration()


class TestRabbitMessageGroupBatchRouter(RabbitMessageGroupBatchRouter):

    def __init__(self) -> None:
        self._filter_strategy: FilterStrategy = DefaultFilterStrategy()


class TestDefaultGrpcRouter(DefaultGrpcRouter):

    def __init__(self, grpc_configuration: GrpcConfiguration) -> None:
        self.grpc_configuration = grpc_configuration


class TestConnection(DefaultGrpcRouter.Connection):

    def __init__(self, services: List[GrpcServiceConfiguration]) -> None:
        self.services = services
        self._grpc_filter_strategy = DefaultGrpcFilterStrategy()


test_common_factory = TestCommonFactory()
rabbit_message_router = TestRabbitMessageGroupBatchRouter()
grpc_router = TestDefaultGrpcRouter(grpc_configuration=test_common_factory.grpc_configuration)  # type: ignore
