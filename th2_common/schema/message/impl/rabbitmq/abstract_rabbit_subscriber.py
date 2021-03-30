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
import functools
import logging
import time
from abc import ABC, abstractmethod
from threading import Lock

from google.protobuf.message import DecodeError
from prometheus_client import Histogram, Counter

from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.subscribe_target import SubscribeTarget
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.impl.rabbitmq.connection.reconnecting_consumer import ReconnectingConsumer
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.message_subscriber import MessageSubscriber

logger = logging.getLogger()


class AbstractRabbitSubscriber(MessageSubscriber, ABC):

    def __init__(self, connection_manager: ConnectionManager, queue_configuration: QueueConfiguration,
                 subscribe_target: SubscribeTarget) -> None:

        self.__subscribe_target = subscribe_target
        self.__attributes = tuple(set(queue_configuration.attributes))

        self.listeners = set()
        self.__lock_listeners = Lock()

        self.__consumer: ReconnectingConsumer = connection_manager.consumer
        self.__consumer_tag = None
        self.__closed = True

    def start(self):
        if self.__subscribe_target is None:
            raise Exception('Subscriber did not init')

        if self.__consumer_tag is None:
            queue = self.__subscribe_target.get_queue()
            self.__consumer_tag = self.__consumer.add_subscriber(queue=queue,
                                                                 on_message_callback=self.handle)
            self.__closed = False

    def handle(self, channel, method, properties, body):
        process_timer = self.get_processing_timer()
        start_time = time.time()
        try:

            values = self.value_from_bytes(body)

            for value in values:
                if value is None:
                    raise ValueError('Received value is null')

                counter = self.get_delivery_counter()
                counter.inc()
                content_counter = self.get_content_counter()
                content_counter.inc(self.extract_count_from(value))

                if not self.filter(value):
                    return

                self.handle_with_listener(value, channel, method)

        except DecodeError as e:
            logger.exception(
                f'Can not parse value from delivery for: {method.consumer_tag} due to DecodeError: {e}\n'
                f'  body: {body}\n'
                f'  self: {self}\n')
            return
        except Exception as e:
            logger.error(f'Can not parse value from delivery for: {method.consumer_tag}', e)
            return
        finally:
            process_timer.observe(time.time() - start_time)
            cb = functools.partial(self.ack_message, channel, method.delivery_tag)
            self.__consumer.add_callback_threadsafe(cb)

    def ack_message(self, channel, delivery_tag):
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            logger.error('Message acknowledgment failed due to the channel being closed')

    def handle_with_listener(self, value, channel, method):
        with self.__lock_listeners:
            for listener in self.listeners:
                try:
                    listener.handler(self.__attributes, value)
                except Exception as e:
                    logger.warning(f"Message listener from class '{type(listener)}' threw exception {e}")

    def add_listener(self, message_listener: MessageListener):
        if message_listener is None:
            return
        with self.__lock_listeners:
            self.listeners.add(message_listener)

    def is_close(self) -> bool:
        return self.__closed

    def close(self):
        with self.__lock_listeners:
            for listener in self.listeners:
                listener.on_close()
            self.listeners.clear()
        self.__consumer.remove_subscriber(self.__consumer_tag)
        self.__closed = True

    @abstractmethod
    def value_from_bytes(self, body):
        pass

    @abstractmethod
    def filter(self, value) -> bool:
        pass

    @abstractmethod
    def get_delivery_counter(self) -> Counter:
        pass

    @abstractmethod
    def get_content_counter(self) -> Counter:
        pass

    @abstractmethod
    def get_processing_timer(self) -> Histogram:
        pass

    @abstractmethod
    def extract_count_from(self, message):
        pass
