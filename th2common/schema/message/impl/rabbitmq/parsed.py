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
#
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
# # Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from th2common.gen.infra_pb2 import MessageBatch
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_router import AbstractRabbitBatchMessageRouter, \
    AbstractRabbitQueue, AbstractRabbitSender, AbstractRabbitBatchSubscriber
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.interfaces import MessageListener, SubscriberMonitor, MessageQueue, MessageSubscriber, \
    MessageSender


class RabbitParsedBatchSender(AbstractRabbitSender):

    def value_to_bytes(self, value: MessageBatch):
        return value.SerializeToString()

class RabbitParsedBatchSubscriber(AbstractRabbitBatchSubscriber):
    pass


class RabbitParsedBatchQueue(AbstractRabbitQueue):

    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        sender = RabbitParsedBatchSender(configuration, queue_configuration.exchange, queue_configuration.name)
        sender.start()
        return sender

    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        subscriber = RabbitParsedBatchSubscriber()


class RabbitParsedBatchRouter(AbstractRabbitBatchMessageRouter):

    def __init__(self, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        pass

    def unsubscribe_all(self):
        pass

    def send(self, message, *queue_attr):
        super().send(message, *queue_attr)

    def create_queue(self, configuration: RabbitMQConfiguration,
                     queue_configuration: QueueConfiguration) -> MessageQueue:
        return RabbitParsedBatchQueue

    def get_target_queue_aliases_and_messages_to_send(self, message) -> dict:
        pass
