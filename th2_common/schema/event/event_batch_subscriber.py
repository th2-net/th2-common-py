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
from th2_grpc_common.common_pb2 import EventBatch

from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_subscriber import AbstractRabbitSubscriber


class EventBatchSubscriber(AbstractRabbitSubscriber):
    INCOMING_EVENT_BATCH_QUANTITY = Counter('th2_mq_incoming_event_batch_quantity',
                                            'Quantity of incoming event batches')
    INCOMING_EVENT_QUANTITY = Counter('th2_mq_incoming_event_quantity',
                                      'Quantity of incoming events')
    EVENT_PROCESSING_TIME = Gauge('th2_mq_event_processing_time',
                                  'Time of processing events')

    def get_delivery_counter(self) -> Counter:
        return self.INCOMING_EVENT_BATCH_QUANTITY

    def get_content_counter(self) -> Counter:
        return self.INCOMING_EVENT_QUANTITY

    def get_processing_timer(self) -> Gauge:
        return self.EVENT_PROCESSING_TIME

    def extract_count_from(self, message: EventBatch):
        return len(self.get_events(message))

    def get_events(self, batch: EventBatch) -> list:
        return batch.events

    def value_from_bytes(self, body):
        event_batch = EventBatch()
        event_batch.ParseFromString(body)
        return event_batch

    def filter(self, value) -> bool:
        return True
