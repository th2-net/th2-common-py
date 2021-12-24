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

from google.protobuf.json_format import MessageToJson
from prometheus_client import Counter
from th2_grpc_common.common_pb2 import MessageBatch, MessageGroupBatch, MessageGroup, AnyMessage

from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_sender import AbstractRabbitSender
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_sender import \
    RabbitMessageGroupBatchSender
from th2_common.schema.util.util import get_debug_string, get_session_alias_and_direction


class RabbitParsedBatchSender(RabbitMessageGroupBatchSender):

    def update_metrics(self, batch):
        labels = (self.th2_pin, ) + get_session_alias_and_direction(batch.messages[0].metadata.id)
        nonraw_count = len(batch.messages)
        self.OUTGOING_MSG_QUANTITY.labels(*labels, 'MESSAGE').inc(nonraw_count)
        self.OUTGOING_MSG_GROUP_QUANTITY.labels(*labels).inc()

    @staticmethod
    def value_to_bytes(value: MessageBatch):
        messages = [AnyMessage(message=msg) for msg in value.messages]
        group = MessageGroup(messages=messages)
        group_batch = MessageGroupBatch(groups=[group])
        return group_batch.SerializeToString()

    def to_trace_string(self, value):
        return MessageToJson(value)

    def to_debug_string(self, value):
        return get_debug_string(self.__class__.__name__, [message.metadata.id for message in self.get_messages(value)])
