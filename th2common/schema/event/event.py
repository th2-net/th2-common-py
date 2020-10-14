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

from th2common.gen.infra_pb2 import EventBatch
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_router import AbstractRabbitSender, \
    AbstractRabbitSubscriber, AbstractRabbitQueue, AbstractRabbitMessageRouter
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration, SubscribeTarget
from th2common.schema.message.interfaces import MessageSubscriber, MessageSender, MessageQueue


class EventBatchSender(AbstractRabbitSender):
    def value_to_bytes(self, value):
        return value.SerializeToString()


class EventBatchSubscriber(AbstractRabbitSubscriber):
    def value_from_bytes(self, body):
        event_batch = EventBatch()
        event_batch.ParseFromString(body)
        return event_batch

    def filter(self, value) -> bool:
        return True


class EventBatchQueue(AbstractRabbitQueue):
    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        return EventBatchSender(configuration, queue_configuration.exchange, queue_configuration.name)

    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        subscribe_target = SubscribeTarget(routing_key=queue_configuration.name, queue=queue_configuration.queue)
        return EventBatchSubscriber(configuration, queue_configuration, subscribe_target)


class EventBatchRouter(AbstractRabbitMessageRouter):
    def _create_queue(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        return EventBatchQueue(configuration, queue_configuration)

    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        return {k: msg for k in queues.keys()}
