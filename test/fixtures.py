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
from test.test_filter_strategy.resources.messages_for_filtering import message_group_batch
from typing import Any, Dict
from unittest.mock import Mock

import pytest

from th2_common.schema.factory.common_factory import CommonFactory
from th2_common.schema.grpc.router.impl.default_grpc_router import DefaultGrpcRouter
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter


@pytest.fixture(scope='session')
def factory() -> CommonFactory:  # type: ignore
    config_path = Path(__file__).parent / 'test_configuration' / 'resources' / 'json_configuration'
    factory = CommonFactory(config_path=config_path)

    yield factory

    factory.close()


@pytest.fixture(scope='session')
def filtered_messages(factory) -> Dict[str, Any]:  # type: ignore
    message_router_configuration = factory._create_message_router_configuration()
    rabbit_message_router = RabbitMessageGroupBatchRouter(connection_manager=Mock(),
                                                          configuration=message_router_configuration,
                                                          box_configuration=factory.box_configuration)

    filtered_messages = rabbit_message_router.split_and_filter(
        queue_aliases_to_configs=message_router_configuration.queues,
        batch=message_group_batch
    )

    yield filtered_messages


@pytest.fixture(scope='session')
def grpc_router(factory) -> DefaultGrpcRouter:  # type: ignore
    grpc_configuration = factory._create_grpc_configuration()
    grpc_router = DefaultGrpcRouter(grpc_configuration=grpc_configuration, grpc_router_configuration=Mock())

    yield grpc_router
