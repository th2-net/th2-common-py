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


from abc import ABC, abstractmethod
from threading import Lock

from th2_common.schema.exception.router_error import RouterError
from th2_common.schema.filter.strategy.filter_strategy import FilterStrategy
from th2_common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.message_queue import MessageQueue
from th2_common.schema.message.message_router import MessageRouter
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

    def __init__(self, connection, rabbit_mq_configuration, configuration) -> None:
        super().__init__(rabbit_mq_configuration, configuration)
        self.connection = connection
        self._filter_strategy = DefaultFilterStrategy()
        self.queue_connections = dict()
        self.queue_connections_lock = Lock()

    def _subscribe_by_alias(self, callback: MessageListener, queue_alias) -> SubscriberMonitor:
        queue = self._get_message_queue(self.connection, queue_alias)
        subscriber = queue.get_subscriber()
        subscriber.add_listener(callback)
        try:
            subscriber.start()
        except Exception as e:
            raise RuntimeError('Can not start subscriber', e)
        return SubscriberMonitorImpl(subscriber, queue.subscriber_lock)

    def subscribe_by_attr(self, callback: MessageListener, queue_attr) -> SubscriberMonitor:
        queues = self.configuration.find_queues_by_attr(queue_attr)
        if len(queues) != 1:
            raise RouterError(
                f'Wrong amount of queues for subscribe_by_attr. '
                f'Found {len(queues)} queues, but must be only 1. Search was done by {queue_attr} attributes')
        return self._subscribe_by_alias(callback, queues.keys().__iter__().__next__())

    def subscribe_all_by_attr(self, callback: MessageListener, queue_attr) -> SubscriberMonitor:
        subscribers = []
        for queue_alias in self.configuration.find_queues_by_attr(queue_attr).keys():
            subscribers.append(self._subscribe_by_alias(callback, queue_alias))
        if len(subscribers) == 0:
            raise RouterError(f'Wrong amount of queues for subscribe_all_by_attr. Must not be empty. '
                              f'Search was done by {queue_attr} attributes')
        return MultiplySubscribeMonitorImpl(subscribers)

    def subscribe_all(self, callback: MessageListener) -> SubscriberMonitor:
        subscribers = []
        for queue_alias in self.configuration.queues.keys():
            subscribers.append(self._subscribe_by_alias(callback, queue_alias))
        if len(subscribers) == 0:
            raise RouterError(f'Wrong amount of queues for subscribe_all. Must not be empty')
        return MultiplySubscribeMonitorImpl(subscribers)

    def unsubscribe_all(self):
        with self.queue_connections_lock:
            for queue in self.queue_connections.values():
                queue.close()
            self.queue_connections.clear()

    def close(self):
        self.unsubscribe_all()

    def send(self, message):
        self._send_by_aliases_and_messages_to_send(self._find_by_filter(self.configuration.queues, message))

    def send_by_attr(self, message, queue_attr):
        filtered_by_attr = self.configuration.find_queues_by_attr(queue_attr)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) != 1:
            raise Exception('Wrong amount of queues for send_by_attr. Must be equal to 1')
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def send_all(self, message, queue_attr):
        filtered_by_attr = self.configuration.find_queues_by_attr(queue_attr)
        filtered_by_attr_and_filter = self._find_by_filter(filtered_by_attr, message)
        if len(filtered_by_attr_and_filter) == 0:
            raise Exception('Wrong amount of queues for send_all. Must not be equal to 0')
        self._send_by_aliases_and_messages_to_send(filtered_by_attr_and_filter)

    def set_filter_strategy(self, filter_strategy: FilterStrategy):
        self._filter_strategy = filter_strategy

    @abstractmethod
    def _create_queue(self, connection, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageQueue:
        pass

    @abstractmethod
    def _find_by_filter(self, queues: {str: QueueConfiguration}, msg) -> dict:
        pass

    def _send_by_aliases_and_messages_to_send(self, aliases_and_messages_to_send: dict):
        for queue_alias in aliases_and_messages_to_send.keys():
            message = aliases_and_messages_to_send[queue_alias]
            try:
                sender = self._get_message_queue(self.connection, queue_alias).get_sender()
                sender.start()
                sender.send(message)
            except Exception as e:
                raise RouterError('Can not start sender', e)

    def _get_message_queue(self, connection, queue_alias):
        with self.queue_connections_lock:
            if not self.queue_connections.__contains__(queue_alias):
                self.queue_connections[queue_alias] = self._create_queue(connection, self.rabbit_mq_configuration,
                                                                         self.configuration.get_queue_by_alias(
                                                                             queue_alias))
            return self.queue_connections[queue_alias]
