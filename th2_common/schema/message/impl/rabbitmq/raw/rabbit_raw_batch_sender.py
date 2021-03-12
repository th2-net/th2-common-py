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

from prometheus_client import Counter
from th2_grpc_common.common_pb2 import RawMessageBatch, AnyMessage, MessageGroup, MessageGroupBatch

from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_sender import AbstractRabbitSender


class RabbitRawBatchSender(AbstractRabbitSender):
    OUTGOING_RAW_MSG_BATCH_QUANTITY = Counter('th2_mq_outgoing_raw_msg_batch_quantity',
                                              'Quantity of outgoing raw message batches')
    OUTGOING_RAW_MSG_QUANTITY = Counter('th2_mq_outgoing_raw_msg_quantity',
                                        'Quantity of outgoing raw messages')

    def get_delivery_counter(self) -> Counter:
        return self.OUTGOING_RAW_MSG_BATCH_QUANTITY

    def get_content_counter(self) -> Counter:
        return self.OUTGOING_RAW_MSG_QUANTITY

    def extract_count_from(self, message: RawMessageBatch):
        return len(self.get_messages(message))

    def get_messages(self, batch: RawMessageBatch) -> list:
        return batch.messages

    @staticmethod
    def value_to_bytes(value: RawMessageBatch):
        messages = [AnyMessage(raw_message=msg) for msg in value.messages]
        group = MessageGroup(messages=messages)
        value = MessageGroupBatch(groups=[group])
        return value.SerializeToString()
