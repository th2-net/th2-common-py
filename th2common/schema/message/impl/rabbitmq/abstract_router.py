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

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock

import pika

from th2common.gen.infra_pb2 import Direction
from th2common.schema.filter.factory import DefaultFilterFactory
from th2common.schema.filter.strategy import DefaultFilterStrategy
from th2common.schema.message.configurations import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.interfaces import MessageRouter, MessageListener, SubscriberMonitor, MessageQueue, \
    MessageSender, MessageSubscriber

logger = logging.getLogger()


class AbstractRabbitSender(MessageSender, ABC):

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str, send_queue: str) -> None:
        self.CLOSE_TIMEOUT = 1_000
        self.connection = None
        self.channel = None
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
        self.channel.queue_declare(queue=self.send_queue)

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

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str, *queue_tags) -> None:
        if len(queue_tags) < 1:
            raise Exception("Queue tags must be more than 0")

        self.listeners = set()
        self.lock_listeners = Lock()

        self.connection = None
        self.channel = None

        self.exchange_name = exchange_name
        self.subscriber_name = configuration.subscriberName
        self.queue_aliases = queue_tags

        credentials = pika.PlainCredentials(configuration.username, configuration.password)
        self.connection_parameters = pika.ConnectionParameters(virtual_host=configuration.vHost,
                                                               host=configuration.host,
                                                               port=configuration.port,
                                                               credentials=credentials)

    def start(self):
        if self.queue_aliases is None or self.exchange_name is None:
            raise Exception("Subscriber did not init")

        if self.subscriber_name is None:
            self.subscriber_name = "rabbit_mq_subscriber"
            logger.info(f"Using default subscriber name: '{self.subscriber_name}'")

        self.connection = pika.BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

        for queue_tag in self.queue_aliases:
            declare = self.channel.queue_declare(queue=f"{self.subscriber_name}.{datetime.now()}",
                                                 durable=False, exclusive=True, auto_delete=True)
            queue_name = declare.method.queue
            self.channel.queue_bind(queue=queue_name, exchange=self.exchange_name, routing_key=queue_tag)
            self.channel.basic_consume(queue=queue_name, auto_ack=True, on_message_callback=self.handle)

            logger.info(f"Start listening exchangeName='{self.exchange_name}', "
                        f"routing key='{queue_tag}', queue name='{queue_name}'")

    def is_close(self) -> bool:
        return self.connection is None or not self.connection.is_open()

    def add_listener(self, message_listener: MessageListener):
        if message_listener is None:
            return
        with self.lock_listeners:
            self.listeners.add(message_listener)

    def close(self):
        with self.lock_listeners:
            for listener in self.listeners:
                listener.on_close()
            self.listeners.clear()

        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    def handle(self, channel, method, properties, body):
        try:
            value = self.value_from_bytes(body)
            if not self.filter(value):
                return
            with self.lock_listeners:
                for listener in self.listeners:
                    try:
                        listener.handler(method.consumer_tag, value)
                    except Exception as e:
                        logger.warning(f"Message listener from class '{type(listener)}' threw exception {e}")
        except Exception as e:
            logger.error(f"Can not parse value from delivery for: {method.consumer_tag}", e)

    @abstractmethod
    def value_from_bytes(self, body):
        pass

    @abstractmethod
    def filter(self, value) -> bool:
        pass


class Metadata:

    def __init__(self, sequence, message_type: str, session_alias: str, direction: Direction) -> None:
        self.sequence = sequence
        self.message_type = message_type
        self.session_alias = session_alias
        self.direction = direction


class AbstractRabbitBatchSubscriber(AbstractRabbitSubscriber, ABC):

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str,
                 filters: list, filter_strategy=DefaultFilterStrategy(), *queue_tags) -> None:
        super().__init__(configuration, exchange_name, *queue_tags)
        self.filters = filters
        self.filter_strategy = filter_strategy

    def filter(self, batch) -> bool:
        messages = [msg for msg in self.get_messages(batch) if
                    self.filter_strategy.verify(message=msg, router_filters=self.filters)]
        return len(messages) > 0

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
