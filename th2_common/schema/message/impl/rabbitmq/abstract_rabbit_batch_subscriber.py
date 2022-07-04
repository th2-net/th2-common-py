#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from abc import ABC, abstractmethod

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_subscriber import AbstractRabbitSubscriber
from th2_common.schema.message.impl.rabbitmq.configuration.subscribe_target import SubscribeTarget
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_grpc_common.common_pb2 import Direction, MessageGroupBatch


class Metadata:

    def __init__(self,
                 sequence: str,
                 message_type: str,
                 direction: Direction,
                 session_alias: str) -> None:
        self.sequence = sequence
        self.message_type = message_type
        self.direction = direction
        self.session_alias = session_alias


class AbstractRabbitBatchSubscriber(AbstractRabbitSubscriber, ABC):

    def __init__(self,
                 connection_manager: ConnectionManager,
                 subscribe_target: SubscribeTarget,
                 queue_configuration: QueueConfiguration,
                 filter_strategy: DefaultFilterStrategy,
                 th2_pin: str = '') -> None:
        super().__init__(connection_manager, subscribe_target, queue_configuration, th2_pin=th2_pin)
        self.filters = queue_configuration.filters
        self.filter_strategy = filter_strategy

    def filter(self, batch: MessageGroupBatch) -> bool:  # noqa: A003
        messages = [
            message_group for message_group in self.get_messages(batch) if
            self.filter_strategy.verify(message=message_group, router_filters=self.filters)
        ]
        return len(messages) > 0

    @abstractmethod
    def get_messages(self, batch: MessageGroupBatch) -> RepeatedCompositeFieldContainer:
        pass

    @abstractmethod
    def extract_metadata(self, message: MessageGroupBatch) -> Metadata:
        pass
