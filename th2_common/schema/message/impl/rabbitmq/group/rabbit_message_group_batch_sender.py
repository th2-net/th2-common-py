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
from prometheus_client import Counter, Gauge
from th2_grpc_common.common_pb2 import MessageGroupBatch

from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_sender import AbstractRabbitSender
from th2_common.schema.metrics.common_metrics import DEFAULT_LABELS, DEFAULT_MESSAGE_TYPE_LABEL_NAME
from th2_common.schema.util.util import get_debug_string_group, get_session_alias_and_direction_group, get_sequence


class RabbitMessageGroupBatchSender(AbstractRabbitSender):
    OUTGOING_MSG_QUANTITY = Counter('th2_message_publish_total',
                                    'Amount of individual messages sent',
                                    DEFAULT_LABELS+(DEFAULT_MESSAGE_TYPE_LABEL_NAME, ))
    OUTGOING_MSG_GROUP_QUANTITY = Counter('th2_message_group_publish_total',
                                          'Quantity of outgoing message groups',
                                          DEFAULT_LABELS)
    OUTGOING_GROUP_SEQUENCE = Gauge('th2_message_group_sequence_publish',
                                    'Last sequence sent',
                                    DEFAULT_LABELS)

    def update_metrics(self, batch):
        labels = (self.th2_pin, ) + get_session_alias_and_direction_group(batch.groups[0].messages[0])
        raw_count = sum(1 for group in batch.groups for anymsg in group.messages if anymsg.HasField('raw_message'))
        nonraw_count = sum(len(group.messages) for group in batch.groups) - raw_count
        self.OUTGOING_MSG_QUANTITY.labels(*labels, 'RAW_MESSAGE').inc(raw_count)
        self.OUTGOING_MSG_QUANTITY.labels(*labels, 'MESSAGE').inc(nonraw_count)
        self.OUTGOING_MSG_GROUP_QUANTITY.labels(*labels).inc(len(batch.groups))
        self.OUTGOING_GROUP_SEQUENCE.labels(*labels).set(get_sequence(batch))

    @staticmethod
    def value_to_bytes(value: MessageGroupBatch):
        return value.SerializeToString()

    def to_trace_string(self, value):
        return MessageToJson(value)

    def to_debug_string(self, value):
        return get_debug_string_group(value)
