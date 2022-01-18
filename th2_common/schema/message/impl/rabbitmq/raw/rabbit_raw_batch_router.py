#   Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

from th2_grpc_common.common_pb2 import AnyMessage, MessageGroup, MessageGroupBatch, RawMessageBatch

from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.subscribe_target import SubscribeTarget
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_sender import RabbitRawBatchSender
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_subscriber import RabbitRawBatchSubscriber
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.message_sender import MessageSender
from th2_common.schema.message.message_subscriber import MessageSubscriber
from th2_common.schema.message.queue_attribute import QueueAttribute
from th2_common.schema.message.subscriber_monitor import SubscriberMonitor


class RabbitRawBatchRouter(RabbitMessageGroupBatchRouter):

    @property
    def required_subscribe_attributes(self):
        return {QueueAttribute.SUBSCRIBE.value, QueueAttribute.RAW.value}

    @property
    def required_send_attributes(self):
        return {QueueAttribute.PUBLISH.value, QueueAttribute.RAW.value}

    def create_sender(self, connection_manager: ConnectionManager,
                      queue_configuration: QueueConfiguration, th2_pin) -> MessageSender:
        return RabbitRawBatchSender(connection_manager, queue_configuration.exchange, queue_configuration.routing_key,
                                    th2_pin=th2_pin)

    def create_subscriber(self, connection_manager: ConnectionManager,
                          queue_configuration: QueueConfiguration, th2_pin) -> MessageSubscriber:
        subscribe_target = SubscribeTarget(queue_configuration.queue, queue_configuration.routing_key)
        return RabbitRawBatchSubscriber(connection_manager,
                                        queue_configuration,
                                        self.filter_strategy,
                                        subscribe_target,
                                        th2_pin=th2_pin)

    def send(self, message, *queue_attr):
        super().send(self.to_group_batch(message), *queue_attr)

    def send_all(self, message, *queue_attr):
        super().send_all(self.to_group_batch(message), *queue_attr)

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        return super().subscribe(raw_converter(callback), *queue_attr)

    def subscribe_all(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        return super().subscribe_all(raw_converter(callback), *queue_attr)

    @staticmethod
    def to_group_batch(message):
        messages = [AnyMessage(raw_message=msg) for msg in message.messages]
        group = MessageGroup(messages=messages)
        value = MessageGroupBatch(groups=[group])
        return value

    @staticmethod
    def from_group_batch(message):
        result = RawMessageBatch()
        for group in message.groups:
            for anymsg in group.messages:
                if anymsg.HasField('raw_message'):
                    result.messages.append(anymsg.raw_message)
        return result


def raw_converter(listener: MessageListener):
    old_handler = listener.handler
    def new_handler(attributes, message):
        old_handler(attributes, message)
        msg = RabbitRawBatchRouter.to_group_batch(message)
        message = msg
    listener.handler = new_handler
    return listener
