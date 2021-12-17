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

from abc import ABC, abstractmethod
from threading import Lock

from th2_common.schema.exception.router_error import RouterError
from th2_common.schema.filter.strategy.filter_strategy import FilterStrategy
from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.message_router import MessageRouter
from th2_common.schema.message.message_sender import MessageSender
from th2_common.schema.message.message_subscriber import MessageSubscriber
from th2_common.schema.message.subscriber_monitor import SubscriberMonitor


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

    def __init__(self, connection_manager, configuration) -> None:
        super().__init__(connection_manager, configuration)
        self._filter_strategy = DefaultFilterStrategy()
        self.queue_connections = list()
        self.queue_connections_lock = Lock()
        self.subscriber = dict()
        self.sender = dict()

    @property
    @abstractmethod
    def required_subscribe_attributes(self):
        pass

    @property
    @abstractmethod
    def required_send_attributes(self):
        pass

    def add_subscribe_attributes(self, queue_attr):
        return self.required_subscribe_attributes.union(queue_attr)

    def add_send_attributes(self, queue_attr):
        return self.required_send_attributes.union(queue_attr)

    def _subscribe_by_alias(self, callback: MessageListener, queue_alias) -> SubscriberMonitor:
        subscriber: MessageSubscriber = self.get_subscriber(queue_alias)
        subscriber.add_listener(callback)
        try:
            subscriber.start()
        except Exception as e:
            raise RuntimeError('Can not start subscriber', e)
        return SubscriberMonitorImpl(subscriber, Lock())

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        attrs = self.add_subscribe_attributes(queue_attr)
        queues = self.configuration.find_queues_by_attr(attrs)
        if len(queues) != 1:
            raise RouterError(
                f'Wrong amount of queues for subscribe. '
                f'Found {len(queues)} queues, but must be only 1. Search was done by {queue_attr} attributes')
        return self._subscribe_by_alias(callback, queues.keys().__iter__().__next__())

    def subscribe_all(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        attrs = self.add_subscribe_attributes(queue_attr)
        subscribers = []
        for queue_alias in self.configuration.find_queues_by_attr(attrs).keys():
            subscribers.append(self._subscribe_by_alias(callback, queue_alias))
        if len(subscribers) == 0:
            raise RouterError(f'Wrong amount of queues for subscribe_all. Must not be empty. '
                              f'Search was done by {attrs} attributes')
        return MultiplySubscribeMonitorImpl(subscribers)

    def unsubscribe_all(self):
        with self.queue_connections_lock:
            for queue in self.queue_connections:
                self.close_queue(queue)
            self.queue_connections.clear()

    def close(self):
        self.unsubscribe_all()

    def send(self, message, *queue_attr):
        attrs = self.add_send_attributes(queue_attr)
        filtered_by_attr = self.configuration.find_queues_by_attr(attrs)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) != 1:
            raise Exception(f'Wrong amount of queues for send. Must be equal to 1. '
                            f'Found {len(filtered_by_attr_and_filter)} queues, but must be only 1. '
                            f'Search was done by {attrs} attributes')
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def send_all(self, message, *queue_attr):
        attrs = self.add_send_attributes(queue_attr)
        filtered_by_attr = self.configuration.find_queues_by_attr(attrs)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) == 0:
            raise Exception(f'Wrong amount of queues for send_all. Must not be equal to 0. '
                            f'Search was done by {attrs} attributes')
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def set_filter_strategy(self, filter_strategy: FilterStrategy):
        self._filter_strategy = filter_strategy

    @abstractmethod
    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        pass

    def _send_by_aliases_and_messages_to_send(self, aliases_and_messages_to_send: dict):
        for queue_alias, message in aliases_and_messages_to_send.items():
            try:
                sender = self.get_sender(queue_alias)
                sender.start()
                sender.send(message)
            except Exception:
                raise RouterError('Can not start sender')

    def get_subscriber(self, queue_alias) -> MessageSubscriber:
        queue_configuration = self.configuration.get_queue_by_alias(queue_alias)
        with self.queue_connections_lock:
            if queue_alias not in self.queue_connections:
                self.queue_connections.append(queue_alias)
        if self.connection_manager is None or queue_configuration is None:
            raise RouterError('Queue not yet init')
        if not queue_configuration.can_read:
            raise RouterError('Queue can not read')
        with self.subscriber_lock:
            if queue_alias not in self.subscriber or self.subscriber[queue_alias].is_close():
                self.subscriber[queue_alias] = self.create_subscriber(self.connection_manager, queue_configuration)
            return self.subscriber[queue_alias]

    def get_sender(self, queue_alias) -> MessageSender:
        queue_configuration = self.configuration.get_queue_by_alias(queue_alias)
        with self.queue_connections_lock:
            if queue_alias not in self.queue_connections:
                self.queue_connections.append(queue_alias)
        if self.connection_manager is None or queue_configuration is None:
            raise RouterError('Queue not yet init')
        if not queue_configuration.can_write:
            raise RouterError('Queue can not write')
        with self.sender_lock:
            if queue_alias not in self.sender or self.sender[queue_alias].is_close():
                self.sender[queue_alias] = self.create_sender(self.connection_manager, queue_configuration)
            return self.sender[queue_alias]

    def close_queue(self, queue_alias):
        with self.subscriber_lock:
            if queue_alias in self.subscriber and not self.subscriber[queue_alias].is_close():
                self.subscriber[queue_alias].close()
        with self.sender_lock:
            if queue_alias in self.sender and not self.sender[queue_alias].is_close():
                self.sender[queue_alias].close()

    @abstractmethod
    def create_sender(self, connection_manager: ConnectionManager,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        pass

    @abstractmethod
    def create_subscriber(self, connection_manager: ConnectionManager,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        pass
