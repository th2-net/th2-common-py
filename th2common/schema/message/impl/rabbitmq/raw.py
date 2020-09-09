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
from th2common.gen.infra_pb2 import RawMessageBatch, RawMessage
from th2common.schema.filter.strategy import DefaultFilterStrategy
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_router import AbstractRabbitBatchMessageRouter, \
    AbstractRabbitBatchSubscriber, Metadata, AbstractRabbitSender, AbstractRabbitQueue
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration, SubscribeTarget
from th2common.schema.message.interfaces import MessageSubscriber, MessageSender, \
    MessageQueue


class RabbitRawBatchSubscriber(AbstractRabbitBatchSubscriber):
    __MESSAGE_TYPE = 'raw'

    def __init__(self, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration,
                 filter_strategy, *subscribe_targets) -> None:
        super().__init__(configuration, queue_configuration, filter_strategy, *subscribe_targets)

    def get_messages(self, batch: RawMessageBatch) -> list:
        return batch.messages

    def value_from_bytes(self, body):
        message_batch = RawMessageBatch()
        message_batch.ParseFromString(body)
        return message_batch

    def extract_metadata(self, message: RawMessage) -> Metadata:
        metadata = message.metadata
        return Metadata(message_type=self.__MESSAGE_TYPE,
                        direction=metadata.id.direction,
                        sequence=metadata.id.sequence,
                        session_alias=metadata.id.connection_id.session_alias)


class RabbitRawBatchSender(AbstractRabbitSender):

    def value_to_bytes(self, value: RawMessageBatch):
        return value.SerializeToString()


class RabbitRawBatchQueue(AbstractRabbitQueue):

    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        return RabbitRawBatchSender(configuration, queue_configuration.exchange, queue_configuration.name)

    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        subscribe_target = SubscribeTarget(routing_key=queue_configuration.name, queue=queue_configuration.queue)
        return RabbitRawBatchSubscriber(configuration,
                                        queue_configuration.exchange,
                                        queue_configuration.filters,
                                        DefaultFilterStrategy(),
                                        queue_configuration.prefetch_count,
                                        subscribe_target)


class RabbitRawBatchRouter(AbstractRabbitBatchMessageRouter):

    def __init__(self, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)

    def _get_messages(self, batch: RawMessageBatch) -> list:
        return batch.messages

    def _create_batch(self):
        return RawMessageBatch()

    def _add_message(self, batch: RawMessageBatch, message: RawMessage):
        batch.messages.append(message)

    def _create_queue(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        return RabbitRawBatchQueue(configuration, queue_configuration)
