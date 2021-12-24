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
from th2_grpc_common.common_pb2 import MessageGroupBatch

from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_queue import RabbitMessageGroupBatchQueue
from th2_common.schema.message.impl.rabbitmq.router.abstract_rabbit_batch_message_router import \
    AbstractRabbitBatchMessageRouter
from th2_common.schema.message.message_queue import MessageQueue
from th2_common.schema.message.queue_attribute import QueueAttribute
from th2_common.schema.metrics.common_metrics import DEFAULT_LABELS, DEFAULT_MESSAGE_TYPE_LABEL_NAME
from th2_common.schema.util.util import get_session_alias_and_direction_group


class RabbitMessageGroupBatchRouter(AbstractRabbitBatchMessageRouter):
    OUTGOING_MSG_DROPPED = Counter('th2_message_dropped_publish_total',
                                   'Quantity of messages dropped on sending',
                                   DEFAULT_LABELS + (DEFAULT_MESSAGE_TYPE_LABEL_NAME, ))
    OUTGOING_MSG_GROUP_DROPPED = Counter('th2_message_group_dropped_publish_total',
                                         'Quantity of message groups dropped on sending',
                                         DEFAULT_LABELS)

    def update_dropped_metrics(self, batch, modded_batch):
        labels = (self.th2_pin, ) + get_session_alias_and_direction_group(batch.groups[0].messages[0])
        for group in batch.groups:
            if group not in modded_batch.groups:
                self.OUTGOING_MSG_GROUP_DROPPED.labels(*labels).inc()
                raw_count = sum(1 for message in group.messages if message.HasField('raw_message'))
                nonraw_count = len(group.messages) - raw_count
                self.OUTGOING_MSG_DROPPED.labels(*labels, 'RAW_MESSAGE').inc(raw_count)
                self.OUTGOING_MSG_DROPPED.labels(*labels, 'MESSAGE').inc(nonraw_count)

    @property
    def required_subscribe_attributes(self):
        return {QueueAttribute.SUBSCRIBE.value}

    @property
    def required_send_attributes(self):
        return {QueueAttribute.PUBLISH.value}

    def _get_messages(self, batch):
        return batch.groups

    def _create_batch(self):
        return MessageGroupBatch()

    def _add_message(self, batch: MessageGroupBatch, group):
        batch.groups.append(group)

    def _create_queue(self, connection_manager: ConnectionManager,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        return RabbitMessageGroupBatchQueue(connection_manager, queue_configuration)
