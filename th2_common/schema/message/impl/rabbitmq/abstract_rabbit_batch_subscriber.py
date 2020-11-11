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


from abc import ABC, abstractmethod

from th2_grpc_common.common_pb2 import Direction

from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_subscriber import AbstractRabbitSubscriber
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration


class Metadata:

    def __init__(self, sequence, message_type: str, session_alias: str, direction: Direction) -> None:
        self.sequence = sequence
        self.message_type = message_type
        self.session_alias = session_alias
        self.direction = direction


class AbstractRabbitBatchSubscriber(AbstractRabbitSubscriber, ABC):

    def __init__(self, connection, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration,
                 filter_strategy=DefaultFilterStrategy(), *subscribe_targets) -> None:
        super().__init__(connection, configuration, queue_configuration, *subscribe_targets)
        self.filters = queue_configuration.filters
        self.filter_strategy = filter_strategy

    def filter(self, batch) -> bool:
        messages = [msg for msg in self.get_messages(batch) if
                    self.filter_strategy.verify(message=msg, router_filters=self.filters)]
        return len(messages) > 0

    @abstractmethod
    def get_messages(self, batch) -> list:
        pass

    @abstractmethod
    def extract_metadata(self, message) -> Metadata:
        pass
