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

from __future__ import annotations

from abc import ABC, abstractmethod

import pika

from th2common.schema.filter.factory import DefaultFilterFactory
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.interfaces import MessageRouter, MessageListener, SubscriberMonitor, MessageQueue, \
    MessageSender, MessageSubscriber


class AbstractRabbitSender(MessageSender, ABC):
    CLOSE_TIMEOUT = 1_000
    connection = None
    channel = None

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str, send_queue: str) -> None:
        self.send_queue = send_queue
        self.exchange_name = exchange_name
        credentials = pika.PlainCredentials(configuration.username, configuration.password)
        self.connection_parameters = pika.ConnectionParameters(virtual_host=configuration.vHost,
                                                               host=configuration.host,
                                                               port=configuration.port,
                                                               credentials=credentials)

    def start(self):
        if self.send_queue is None or self.exchange_name is None:
            raise Exception("Sender did not init")
        self.connection = pika.BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def is_close(self) -> bool:
        return self.connection is None or not self.connection.is_open()

    def send(self, message):
        if self.channel is None:
            raise Exception("Sender did not init")
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.send_queue,
                                   body=self.value_to_bytes(message))

    def close(self):
        if self.connection is not None and self.connection.is_open():
            self.connection.close()

    @abstractmethod
    def value_to_bytes(self, value):
        pass


class AbstractRabbitSubscriber(MessageSubscriber, ABC):

    def start(self):
        pass

    def is_close(self) -> bool:
        pass

    def add_listener(self, message_listener: MessageListener):
        pass

    @abstractmethod
    def value_from_bytes(self, body):
        pass

    @abstractmethod
    def filter(self, value) -> bool:
        pass

class AbstractRabbitBatchSubscriber(AbstractRabbitSubscriber, ABC):

    def filter(self, value) -> bool:
        pass

    @abstractmethod
    def get_messages(self, batch) -> list:
        pass

    @abstractmethod
    def extract_metadata(self, message) -> Metadata:
        pass


class AbstractRabbitQueue(MessageQueue, ABC):

    def __init__(self, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration) -> None:
        super().__init__(configuration, queue_configuration)

    def get_subscriber(self) -> MessageSubscriber:
        pass

    def get_sender(self) -> MessageSender:
        pass

    def close(self):
        pass

    @abstractmethod
    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        pass

    @abstractmethod
    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        pass


class AbstractRabbitMessageRouter(MessageRouter, ABC):

    def __init__(self, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)
        self.filterFactory = DefaultFilterFactory()

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        pass

    def unsubscribe_all(self):
        pass

    def send(self, message, *queue_attr):
        pass

    @abstractmethod
    def create_queue(self, configuration: RabbitMQConfiguration,
                     queue_configuration: QueueConfiguration) -> MessageQueue:
        pass

    @abstractmethod
    def get_target_queue_aliases_and_messages_to_send(self, message) -> dict:
        pass


class AbstractRabbitBatchMessageRouter(AbstractRabbitMessageRouter, ABC):
    pass
