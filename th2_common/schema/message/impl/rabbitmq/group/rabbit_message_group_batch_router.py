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


from th2_grpc_common.common_pb2 import MessageBatch

from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_queue import RabbitParsedBatchQueue
from th2_common.schema.message.impl.rabbitmq.router.abstract_rabbit_batch_message_router import \
    AbstractRabbitBatchMessageRouter
from th2_common.schema.message.message_queue import MessageQueue
from th2_common.schema.message.queue_attribute import QueueAttribute


class RabbitParsedBatchRouter(AbstractRabbitBatchMessageRouter):

    @property
    def required_subscribe_attributes(self):
        return {QueueAttribute.SUBSCRIBE.value, QueueAttribute.PARSED.value}

    @property
    def required_send_attributes(self):
        return {QueueAttribute.PUBLISH.value, QueueAttribute.PARSED.value}

    def _get_messages(self, batch):
        return batch.messages

    def _create_batch(self):
        return MessageBatch()

    def _add_message(self, batch: MessageBatch, message):
        batch.messages.append(message)

    def _create_queue(self, connection, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        return RabbitParsedBatchQueue(connection, configuration, queue_configuration)
