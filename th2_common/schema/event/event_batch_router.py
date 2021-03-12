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

from th2_common.schema.event.event_batch_queue import EventBatchQueue
from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_message_router import AbstractRabbitMessageRouter
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.message_queue import MessageQueue
from th2_common.schema.message.queue_attribute import QueueAttribute


class EventBatchRouter(AbstractRabbitMessageRouter):

    @property
    def required_subscribe_attributes(self):
        return {QueueAttribute.SUBSCRIBE.value, QueueAttribute.EVENT.value}

    @property
    def required_send_attributes(self):
        return {QueueAttribute.PUBLISH.value, QueueAttribute.EVENT.value}

    def _create_queue(self, connection_manager: ConnectionManager,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        return EventBatchQueue(connection_manager, queue_configuration)

    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        return {key: msg for key in queues.keys()}
