#   Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
from typing import Any, Callable

import google.protobuf.message
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.subscriber_monitor import SubscriberMonitor
from th2_grpc_common.common_pb2 import MessageGroupBatch


class RabbitMessageGroupBatchRouterAdapter(RabbitMessageGroupBatchRouter, ABC):

    def send(self, message: MessageGroupBatch, *queue_attr: str) -> None:
        super().send(self.to_group_batch(message), *queue_attr)

    def send_all(self, message: MessageGroupBatch, *queue_attr: str) -> None:
        super().send_all(self.to_group_batch(message), *queue_attr)

    def subscribe(self, callback: MessageListener, *queue_attr: str) -> SubscriberMonitor:
        return super().subscribe(self.get_converter(callback), *queue_attr)

    def subscribe_all(self, callback: MessageListener, *queue_attr: str) -> SubscriberMonitor:
        return super().subscribe_all(self.get_converter(callback), *queue_attr)

    @staticmethod
    @abstractmethod
    def to_group_batch(message: Any) -> MessageGroupBatch:
        pass

    @staticmethod
    @abstractmethod
    def from_group_batch(message: MessageGroupBatch) -> google.protobuf.message.Message:
        pass

    def get_converter(self, callback: MessageListener) -> MessageListener:
        old_handler: Callable = callback.handler
        setattr(
            callback,
            'handler',
            lambda attributes, message: old_handler(attributes, self.from_group_batch(message))
        )
        return callback
