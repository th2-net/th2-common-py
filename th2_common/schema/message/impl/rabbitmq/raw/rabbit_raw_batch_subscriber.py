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
from prometheus_client import Counter, Histogram
from th2_grpc_common.common_pb2 import RawMessageBatch, RawMessage, MessageGroupBatch

import th2_common.schema.metrics.common_metrics as common_metrics
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_batch_subscriber import AbstractRabbitBatchSubscriber, \
    Metadata
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_subscriber import \
    RabbitMessageGroupBatchSubscriber
from th2_common.schema.util.util import get_debug_string, get_session_alias_and_direction


class RabbitRawBatchSubscriber(RabbitMessageGroupBatchSubscriber):

    __MESSAGE_TYPE = 'raw'

    def get_messages(self, batch: RawMessageBatch) -> list:
        return batch.messages

    @staticmethod
    def value_from_bytes(body):
        message_group_batch = MessageGroupBatch()
        message_group_batch.ParseFromString(body)

        message_batches = []
        for message_group in message_group_batch.groups:
            messages = []
            for any_message in message_group.messages:
                any_message.HasField('raw_message')
                messages.append(any_message.raw_message)
            message_batches.append(RawMessageBatch(messages=messages))

        return message_batches

    def extract_metadata(self, message: RawMessage) -> Metadata:
        metadata = message.metadata
        return Metadata(message_type=self.__MESSAGE_TYPE,
                        direction=metadata.id.direction,
                        sequence=metadata.id.sequence,
                        session_alias=metadata.id.connection_id.session_alias)

    def to_trace_string(self, value):
        return MessageToJson(value)

    def to_debug_string(self, value):
        return get_debug_string(self.__class__.__name__, [message.metadata.id for message in self.get_messages(value)])
