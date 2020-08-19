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

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from threading import Lock

import pika

from th2common.gen.infra_pb2 import Direction, MessageBatch
from th2common.schema.exception import RouterError
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
        self.subscriber = None
        self.subscriber_lock = Lock()
        self.sender = None
        self.sender_lock = Lock()

    def get_subscriber(self) -> MessageSubscriber:
        if self.configuration is None or self.queue_configuration is None:
            raise RouterError("Queue not yet init")
        if not self.queue_configuration.canRead:
            raise RouterError("Queue can not read")
        with self.subscriber_lock:
            if self.subscriber is None or self.subscriber.isClose():
                self.subscriber = self.create_subscriber(self.configuration, self.queue_configuration)
            return self.subscriber

    def get_sender(self) -> MessageSender:
        if self.configuration is None or self.queue_configuration is None:
            raise RouterError("Queue not yet init")
        if not self.queue_configuration.canWrite:
            raise RouterError("Queue can not write")
        with self.sender_lock:
            if self.sender is None or self.sender.is_close():
                self.sender = self.create_sender(self.configuration, self.queue_configuration)
            return self.sender

    def close(self):
        with self.subscriber_lock:
            if self.subscriber is not None and not self.subscriber.is_close():
                self.subscriber.close()
        with self.sender_lock:
            if self.sender is not None and not self.sender.is_close():
                self.sender.close()

    @abstractmethod
    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        pass

    @abstractmethod
    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        pass


class SubscriberMonitorImpl(SubscriberMonitor):

    def __init__(self, subscriber: MessageSubscriber, lock=Lock()) -> None:
        self.lock = lock
        self.subscriber = subscriber

    def unsubscribe(self):
        with self.lock:
            self.subscriber.close()


class MultiplySubscribeMonitorImpl(SubscriberMonitor):

    def __init__(self, subscriber_monitors: list) -> None:
        self.subscriber_monitors = subscriber_monitors

    def unsubscribe(self):
        for monitor in self.subscriber_monitors:
            monitor.unsubscribe()


class AbstractRabbitMessageRouter(MessageRouter, ABC):

    def __init__(self, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)
        self.filter_factory = DefaultFilterFactory()
        self.queue_connections = dict()
        self.queue_connections_lock = Lock()

    def subscribe_by_alias(self, callback: MessageListener, queue_alias) -> SubscriberMonitor:
        queue = self.get_message_queue(queue_alias)
        subscriber = queue.get_subscriber()
        subscriber.add_listener(callback)
        return SubscriberMonitorImpl(subscriber, queue)

    def subscribe_by_attr(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        queues = self.configuration.get_queues_alias_by_attribute(queue_attr)
        if len(queues) > 1:
            raise RouterError(f"Wrong size of queues aliases for send. Not more then 1")
        return None if len(queues) < 1 else self.subscribe_by_alias(callback, queues[0])

    def subscribe_all(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        subscribers = []
        for queue_alias in self.configuration.queues.keys():
            subscribers.append(self.subscribe_by_alias(callback, queue_alias))
        return None if len(subscribers) == 0 else MultiplySubscribeMonitorImpl(subscribers)

    def unsubscribe_all(self):
        with self.queue_connections_lock:
            for queue in self.queue_connections.values():
                queue.close()
            self.queue_connections.clear()

    def send(self, message):
        self.send_by_aliases_and_messages_to_send(self.get_target_queue_aliases_and_messages_to_send(message))

    def send_by_attr(self, message, *queue_attr):
        queues_aliases_and_messages = self.get_target_queue_aliases_and_messages_to_send_by_attr(message, queue_attr)
        if len(queues_aliases_and_messages) > 1:
            raise RouterError(f"Wrong size of queues aliases for send. Not more than 1")
        self.send_by_aliases_and_messages_to_send(queues_aliases_and_messages)

    def send_all(self, message, *queue_attr):
        self.send_by_aliases_and_messages_to_send(
            self.get_target_queue_aliases_and_messages_to_send_by_attr(message, queue_attr))

    @abstractmethod
    def create_queue(self, configuration: RabbitMQConfiguration,
                     queue_configuration: QueueConfiguration) -> MessageQueue:
        pass

    @abstractmethod
    def get_target_queue_aliases_and_messages_to_send(self, message) -> dict:
        pass

    def get_target_queue_aliases_and_messages_to_send_by_attr(self, message, *queue_attr) -> dict:
        filtered_aliases = self.get_target_queue_aliases_and_messages_to_send(message)
        queue_alias = self.configuration.get_queues_alias_by_attribute(queue_attr)
        filtered_aliases = {alias: filtered_aliases[alias] for alias in filtered_aliases.keys()
                            if queue_alias.__contains__(alias)}
        return filtered_aliases

    def send_by_aliases_and_messages_to_send(self, aliases_and_messages_to_send: dict):
        for queue_alias in aliases_and_messages_to_send.keys():
            message = aliases_and_messages_to_send[queue_alias]
            self.get_message_queue(queue_alias).get_sender().send(message)

    def get_message_queue(self, queue_alias):
        with self.queue_connections_lock:
            if not self.queue_connections.__contains__(queue_alias):
                self.queue_connections[queue_alias] = self.create_queue(self.rabbit_mq_configuration,
                                                                        self.configuration.get_queue_by_alias(
                                                                            queue_alias))
            return self.queue_connections[queue_alias]


class AbstractRabbitBatchMessageRouter(AbstractRabbitMessageRouter, ABC):

    def get_target_queue_aliases_and_messages_to_send(self, batch) -> dict:
        message_filter = self.filter_factory.create_filter(self.configuration)
        result = dict()
        for message in self.get_messages(batch):
            queue_alias = message_filter.check(message)
            if not result.__contains__(queue_alias):
                result[queue_alias] = self.create_batch()
            self.add_message(result[queue_alias], message)
        return result

    @abstractmethod
    def get_messages(self, batch):
        pass

    @abstractmethod
    def create_batch(self):
        pass

    @abstractmethod
    def add_message(self, batch: MessageBatch, message):
        pass
