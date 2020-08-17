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

from th2common.gen.infra_pb2 import MessageBatch
from th2common.schema.filter.strategy import DefaultFilterStrategy
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_router import AbstractRabbitBatchMessageRouter, \
    AbstractRabbitQueue, AbstractRabbitSender, AbstractRabbitBatchSubscriber, Metadata
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.interfaces import MessageQueue, MessageSubscriber, \
    MessageSender


class RabbitParsedBatchSender(AbstractRabbitSender):

    def value_to_bytes(self, value: MessageBatch):
        return value.SerializeToString()


class RabbitParsedBatchSubscriber(AbstractRabbitBatchSubscriber):

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str, filters: list,
                 filter_strategy, *queue_tags) -> None:
        super().__init__(configuration, exchange_name, filters, filter_strategy, *queue_tags)

    def get_messages(self, batch: MessageBatch) -> list:
        return batch.messages

    def extract_metadata(self, message) -> Metadata:
        metadata = message.metadata
        return Metadata(message_type=metadata.message_type,
                        direction=metadata.id.direction,
                        sequence=metadata.id.sequence,
                        session_alias=metadata.id.connection_id.session_alias)

    def value_from_bytes(self, body):
        return MessageBatch.ParseFromString(body)


class RabbitParsedBatchQueue(AbstractRabbitQueue):

    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        sender = RabbitParsedBatchSender(configuration, queue_configuration.exchange, queue_configuration.name)
        try:
            sender.start()
        except Exception as e:
            raise RuntimeError(f"Can not start sender on queue with exchange '{queue_configuration.exchange}' "
                               f"and name '{queue_configuration.name}'", e)
        return sender

    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        subscriber = RabbitParsedBatchSubscriber(configuration,
                                                 queue_configuration.exchange,
                                                 queue_configuration.filters,
                                                 DefaultFilterStrategy(),
                                                 queue_configuration.name)
        try:
            subscriber.start()
        except Exception as e:
            raise RuntimeError(f"Can not start subscriber on queue with exchange '{queue_configuration.exchange}' "
                               f"and name '{queue_configuration.name}'", e)
        return subscriber


class RabbitParsedBatchRouter(AbstractRabbitBatchMessageRouter):

    def __init__(self, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)

    def get_messages(self, batch):
        return batch.messages

    def create_batch(self):
        return MessageBatch()

    def add_message(self, batch: MessageBatch, message):
        batch.messages.append(message)

    def create_queue(self, configuration: RabbitMQConfiguration,
                     queue_configuration: QueueConfiguration) -> MessageQueue:
        return RabbitParsedBatchQueue(configuration, queue_configuration)
