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
from collections import defaultdict

from th2_grpc_common.common_pb2 import EventBatch

from th2_common.schema.event.event_batch_sender import EventBatchSender
from th2_common.schema.event.event_batch_subscriber import EventBatchSubscriber
from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_message_router import AbstractRabbitMessageRouter
from th2_common.schema.message.impl.rabbitmq.configuration.subscribe_target import SubscribeTarget
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.message_sender import MessageSender
from th2_common.schema.message.message_subscriber import MessageSubscriber
from th2_common.schema.message.queue_attribute import QueueAttribute

DEFAULT_SCOPE_NAME = 'th2-scope'


class EventBatchRouter(AbstractRabbitMessageRouter):

    @staticmethod
    def check_scope(message):
        for event in message.events:
            if not event.id.scope:
                event.id.scope = DEFAULT_SCOPE_NAME
        return message

    def check_book_name(self, message):
        for event in message.events:
            book = event.id.book_name
            if not book:
                if self.box_configuration.book_name is None:
                    raise Exception('Book name is undefined both explicitly in message IDs and implicitly in box configuration')
                book = self.box_configuration.book_name
                event.id.book_name = book
            if any(message_id.book_name != book for message_id in event.attached_message_ids):
                raise Exception('MessageID attached to event has different book name')
        return message

    def send(self, message, *queue_attr):
        super().send(self.check_book_name(self.check_scope(message)), *queue_attr)

    def send_all(self, message, *queue_attr):
        super().send_all(self.check_book_name(self.check_scope(message)), *queue_attr)

    def _get_messages(self, batch) -> list:
        return batch.events

    def _create_batch(self):
        return EventBatch()

    def _add_message(self, batch, message):
        batch.events.append(message)

    def update_dropped_metrics(self, batch, pin):
        pass

    def split_and_filter(self, queue_aliases_to_configs, batch) -> dict:
        result = defaultdict(EventBatch)
        for message in self._get_messages(batch):
            for queue_alias in queue_aliases_to_configs.keys():
                self._add_message(result[queue_alias], message)
        return result

    @property
    def required_subscribe_attributes(self):
        return {QueueAttribute.SUBSCRIBE.value, QueueAttribute.EVENT.value}

    @property
    def required_send_attributes(self):
        return {QueueAttribute.PUBLISH.value, QueueAttribute.EVENT.value}

    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        return {key: msg for key in queues.keys()}

    def create_sender(self, connection_manager: ConnectionManager,
                      queue_configuration: QueueConfiguration, th2_pin) -> MessageSender:
        return EventBatchSender(connection_manager, queue_configuration.exchange, queue_configuration.routing_key,
                                th2_pin=th2_pin)

    def create_subscriber(self, connection_manager: ConnectionManager,
                          queue_configuration: QueueConfiguration, th2_pin) -> MessageSubscriber:
        subscribe_target = SubscribeTarget(queue_configuration.queue, queue_configuration.routing_key)
        return EventBatchSubscriber(connection_manager, queue_configuration, subscribe_target, th2_pin=th2_pin)
