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

import _thread
import datetime
import functools
import logging
from abc import ABC, abstractmethod
from threading import Lock

import pika

from th2common.gen.infra_pb2 import Direction, Message
from th2common.schema.exception import RouterError
from th2common.schema.filter.strategy import DefaultFilterStrategy, FilterStrategy
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
            raise Exception("Sender can not start. Sender did not init")
        if self.connection is None:
            self.connection = pika.BlockingConnection(self.connection_parameters)
        if self.channel is None:
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def is_close(self) -> bool:
        return self.connection is None or not self.connection.is_open

    def send(self, message):
        if self.channel is None:
            raise Exception("Can not send. Sender did not started")
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.send_queue,
                                   body=self.value_to_bytes(message))

    def close(self):
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    @abstractmethod
    def value_to_bytes(self, value):
        pass


class AbstractRabbitSubscriber(MessageSubscriber, ABC):

    def __init__(self, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration,
                 *subscribe_targets) -> None:
        if len(subscribe_targets) < 1:
            raise Exception("Subscribe targets must be more than 0")

        self.listeners = set()
        self.lock_listeners = Lock()

        self.connection = None
        self.channel = None

        self.prefetch_count = queue_configuration.prefetch_count
        self.exchange_name = queue_configuration.exchange
        self.attributes = tuple(set(queue_configuration.attributes))
        self.subscriber_name = configuration.subscriberName
        self.subscribe_targets = subscribe_targets

        credentials = pika.PlainCredentials(configuration.username, configuration.password)
        self.connection_parameters = pika.ConnectionParameters(virtual_host=configuration.vHost,
                                                               host=configuration.host,
                                                               port=configuration.port,
                                                               credentials=credentials)

    def start(self):
        if self.subscribe_targets is None or self.exchange_name is None:
            raise Exception("Subscriber did not init")

        if self.subscriber_name is None:
            self.subscriber_name = "rabbit_mq_subscriber"
            logger.info(f"Using default subscriber name: '{self.subscriber_name}'")

        if self.connection is None:
            self.connection = pika.BlockingConnection(self.connection_parameters)

        if self.channel is None:
            self.channel = self.connection.channel()

            for subscribe_target in self.subscribe_targets:
                queue = subscribe_target.get_queue()
                routing_key = subscribe_target.get_routing_key()
                self.channel.basic_qos(prefetch_count=self.prefetch_count)
                consumer_tag = f"{self.subscriber_name}.{datetime.datetime.now()}"
                self.channel.basic_consume(queue=queue, consumer_tag=consumer_tag,
                                           on_message_callback=self.handle)

                logger.info(f"Start listening exchangeName='{self.exchange_name}', "
                            f"routing key='{routing_key}', queue name='{queue}', consumer_tag={consumer_tag}")
        _thread.start_new_thread(self.channel.start_consuming, ())

    def is_close(self) -> bool:
        return self.connection is None or not self.connection.is_open

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
            self.connection.add_callback_threadsafe(functools.partial(self.connection.close))

    def handle(self, channel, method, properties, body):
        try:
            value = self.value_from_bytes(body)
            if not self.filter(value):
                channel.basic_ack(delivery_tag=method.delivery_tag)
                return
            with self.lock_listeners:
                for listener in self.listeners:
                    try:
                        listener.handler(self.attributes, value)
                    except Exception as e:
                        logger.warning(f"Message listener from class '{type(listener)}' threw exception {e}")
        except Exception as e:
            logger.error(f"Can not parse value from delivery for: {method.consumer_tag}", e)
        channel.basic_ack(delivery_tag=method.delivery_tag)

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

    def __init__(self, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration,
                 filter_strategy=DefaultFilterStrategy(), *subscribe_targets) -> None:
        super().__init__(configuration, queue_configuration, *subscribe_targets)
        self.filters = queue_configuration.filters
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
            if self.subscriber is None or self.subscriber.is_close():
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
        self._filter_strategy = DefaultFilterStrategy()
        self.queue_connections = dict()
        self.queue_connections_lock = Lock()

    def _subscribe_by_alias(self, callback: MessageListener, queue_alias) -> SubscriberMonitor:
        queue = self._get_message_queue(queue_alias)
        subscriber = queue.get_subscriber()
        subscriber.add_listener(callback)
        try:
            subscriber.start()
        except Exception as e:
            raise RuntimeError(f"Can not start subscriber", e)
        return SubscriberMonitorImpl(subscriber, queue.subscriber_lock)

    def subscribe_by_attr(self, callback: MessageListener, queue_attr) -> SubscriberMonitor:
        queues = self.configuration.find_queues_by_attr(queue_attr)
        if len(queues) > 1:
            raise RouterError(f"Wrong size of queues aliases for send. Not more then 1")
        return None if len(queues) < 1 else self._subscribe_by_alias(callback, queues.keys().__iter__().__next__())

    def subscribe_all_by_attr(self, callback: MessageListener, queue_attr) -> SubscriberMonitor:
        subscribers = []
        for queue_alias in self.configuration.find_queues_by_attr(queue_attr).keys():
            subscribers.append(self._subscribe_by_alias(callback, queue_alias))
        return None if len(subscribers) == 0 else MultiplySubscribeMonitorImpl(subscribers)

    def subscribe_all(self, callback: MessageListener) -> SubscriberMonitor:
        subscribers = []
        for queue_alias in self.configuration.queues.keys():
            subscribers.append(self._subscribe_by_alias(callback, queue_alias))
        return None if len(subscribers) == 0 else MultiplySubscribeMonitorImpl(subscribers)

    def unsubscribe_all(self):
        with self.queue_connections_lock:
            for queue in self.queue_connections.values():
                queue.close()
            self.queue_connections.clear()

    def send(self, message):
        self._send_by_aliases_and_messages_to_send(self._find_by_filter(self.configuration.queues, message))

    def send_by_attr(self, message, queue_attr):
        filtered_by_attr = self.configuration.find_queues_by_attr(queue_attr)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) != 1:
            raise Exception("Wrong size of queues for send. Should be equal to 1")
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def send_all(self, message, queue_attr):
        filtered_by_attr = self.configuration.find_queues_by_attr(queue_attr)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) == 0:
            raise Exception("Wrong size of queues for send. Can't be equal to 0")
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def set_filter_strategy(self, filter_strategy: FilterStrategy):
        self._filter_strategy = filter_strategy

    @abstractmethod
    def _create_queue(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        pass

    @abstractmethod
    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        pass

    def _send_by_aliases_and_messages_to_send(self, aliases_and_messages_to_send: dict):
        for queue_alias in aliases_and_messages_to_send.keys():
            message = aliases_and_messages_to_send[queue_alias]
            try:
                sender = self._get_message_queue(queue_alias).get_sender()
                sender.start()
                sender.send(message)
            except Exception as e:
                raise RouterError("Can not start sender")

    def _get_message_queue(self, queue_alias):
        with self.queue_connections_lock:
            if not self.queue_connections.__contains__(queue_alias):
                self.queue_connections[queue_alias] = self._create_queue(self.rabbit_mq_configuration,
                                                                         self.configuration.get_queue_by_alias(
                                                                             queue_alias))
            return self.queue_connections[queue_alias]


class AbstractRabbitBatchMessageRouter(AbstractRabbitMessageRouter, ABC):

    def _find_by_filter(self, queues: {str: QueueConfiguration}, batch) -> dict:
        result = dict()
        for message in self._get_messages(batch):
            for queue_alias in self._filter(queues, message):
                if not result.__contains__(queue_alias):
                    result[queue_alias] = self._create_batch()
                    self._add_message(result[queue_alias], message)
        return result

    def _filter(self, queues: {str: QueueConfiguration}, message: Message) -> {str}:
        aliases = set()
        for queue_alias in queues.keys():
            filters = queues[queue_alias].filters

            if len(filters) == 0 or self.filter_strategy.verify(message, filters):
                aliases.add(queue_alias)
        return aliases

    @abstractmethod
    def _get_messages(self, batch) -> list:
        pass

    @abstractmethod
    def _create_batch(self):
        pass

    @abstractmethod
    def _add_message(self, batch, message):
        pass
