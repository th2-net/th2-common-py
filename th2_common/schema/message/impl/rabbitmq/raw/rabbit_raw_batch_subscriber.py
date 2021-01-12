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
from prometheus_client import Counter, Gauge
from th2_grpc_common.common_pb2 import RawMessageBatch, RawMessage

from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_batch_subscriber import AbstractRabbitBatchSubscriber, \
    Metadata
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration


class RabbitRawBatchSubscriber(AbstractRabbitBatchSubscriber):
    INCOMING_RAW_MSG_BATCH_QUANTITY = Counter('th2_mq_incoming_raw_msg_batch_quantity',
                                              'Quantity of incoming raw message batches')
    INCOMING_RAW_MSG_QUANTITY = Counter('th2_mq_incoming_raw_msg_quantity',
                                        'Quantity of incoming raw messages')
    RAW_MSG_PROCESSING_TIME = Gauge('th2_mq_raw_msg_processing_time',
                                    'Time of processing raw messages')

    __MESSAGE_TYPE = 'raw'

    def get_delivery_counter(self) -> Counter:
        return self.INCOMING_RAW_MSG_BATCH_QUANTITY

    def get_content_counter(self) -> Counter:
        return self.INCOMING_RAW_MSG_QUANTITY

    def get_processing_timer(self) -> Gauge:
        return self.RAW_MSG_PROCESSING_TIME

    def extract_count_from(self, message: RawMessageBatch):
        return len(self.get_messages(message))

    def get_messages(self, batch: RawMessageBatch) -> list:
        return batch.messages

    def value_from_bytes(self, body):
        message_batch = RawMessageBatch()
        message_batch.ParseFromString(body)
        return message_batch

    def extract_metadata(self, message: RawMessage) -> Metadata:
        metadata = message.metadata
        return Metadata(message_type=self.__MESSAGE_TYPE,
                        direction=metadata.id.direction,
                        sequence=metadata.id.sequence,
                        session_alias=metadata.id.connection_id.session_alias)
