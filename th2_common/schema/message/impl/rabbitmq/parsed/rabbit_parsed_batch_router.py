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

from typing import Set

from th2_grpc_common.common_pb2 import AnyMessage, MessageBatch, MessageGroup, MessageGroupBatch

from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router_adapter import \
    RabbitMessageGroupBatchRouterAdapter
from th2_common.schema.message.queue_attribute import QueueAttribute


class RabbitParsedBatchRouter(RabbitMessageGroupBatchRouterAdapter):

    @property
    def required_subscribe_attributes(self) -> Set[str]:
        return {QueueAttribute.SUBSCRIBE, QueueAttribute.PARSED}

    @property
    def required_send_attributes(self) -> Set[str]:
        return {QueueAttribute.PUBLISH, QueueAttribute.PARSED}

    @staticmethod
    def to_group_batch(message: MessageBatch) -> MessageGroupBatch:
        messages = [AnyMessage(message=msg) for msg in message.messages]
        group = MessageGroup(messages=messages)

        return MessageGroupBatch(groups=[group])

    @staticmethod
    def from_group_batch(message: MessageGroupBatch) -> MessageBatch:
        return MessageBatch(messages=[
            anymsg.message for group in message.groups for anymsg in group.messages if
            anymsg.HasField('message')
        ])
