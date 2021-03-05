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

import datetime
import logging
import time
from abc import ABC, abstractmethod
from threading import Lock
from typing import Optional

from google.protobuf.message import DecodeError
from pika.channel import Channel
from prometheus_client import Histogram, Counter

from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.message_subscriber import MessageSubscriber

logger = logging.getLogger()


class AbstractRabbitSubscriber(MessageSubscriber, ABC):

    def __init__(self, connection_manager: ConnectionManager, queue_configuration: QueueConfiguration,
                 *subscribe_targets) -> None:
        if len(subscribe_targets) < 1:
            raise Exception('Subscribe targets must be more than 0')

        self.listeners = set()
        self.lock_listeners = Lock()

        self.connection_manager: ConnectionManager = connection_manager
        self.channel: Optional[Channel] = None
        self.channel_is_open = True

        self.subscribe_targets = subscribe_targets
        self.subscriber_name = connection_manager.configuration.subscriber_name

        self.prefetch_count = queue_configuration.prefetch_count
        self.exchange_name = queue_configuration.exchange
        self.attributes = tuple(set(queue_configuration.attributes))

    def start(self):
        if self.subscribe_targets is None or self.exchange_name is None:
            raise Exception('Subscriber did not init')
        self.check_and_open_channel()

    def subscribe_to_targets(self):
        if self.subscriber_name is None:
            self.subscriber_name = 'rabbit_mq_subscriber'
            logger.info(f"Using default subscriber name: '{self.subscriber_name}'")

        for subscribe_target in self.subscribe_targets:
            queue = subscribe_target.get_queue()
            routing_key = subscribe_target.get_routing_key()
            self.channel.basic_qos(prefetch_count=self.prefetch_count)
            consumer_tag = f'{self.subscriber_name}.{datetime.datetime.now()}'
            self.channel.basic_consume(queue=queue, consumer_tag=consumer_tag,
                                       on_message_callback=self.handle)

            logger.info(f"Start listening exchangeName='{self.exchange_name}', "
                        f"routing key='{routing_key}', queue name='{queue}', consumer_tag={consumer_tag}")

    def check_and_open_channel(self):
        self.connection_manager.wait_connection_readiness()
        if self.channel is None or not self.channel.is_open:
            self.channel = self.connection_manager.connection.channel()
            self.channel.add_on_close_callback(self.channel_close_callback)
        self.wait_channel_readiness()
        self.subscribe_to_targets()

    def channel_close_callback(self, channel, reason):
        logger.info(f"Channel '{channel}' is close, reason: {reason}")
        if self.channel_is_open:
            self.connection_manager.reopen_connection()
            self.check_and_open_channel()

    def wait_channel_readiness(self):
        while not self.channel.is_open:
            time.sleep(ConnectionManager.CHANNEL_READINESS_TIMEOUT)

    def is_close(self) -> bool:
        return self.channel is None or not self.channel.is_open

    def close(self):
        with self.lock_listeners:
            for listener in self.listeners:
                listener.on_close()
            self.listeners.clear()

        if self.channel is not None and self.channel.is_open:
            self.channel.close()
            self.channel_is_open = False
            logger.info(f"Close channel: {self.channel} for subscriber[{self.exchange_name}]")

    def add_listener(self, message_listener: MessageListener):
        if message_listener is None:
            return
        with self.lock_listeners:
            self.listeners.add(message_listener)

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

    def handle(self, channel, method, properties, body):
        try:
            process_timer = self.get_processing_timer()
            start_time = time.time()

            value = self.value_from_bytes(body)

            if value is None:
                raise ValueError('Received value is null')

            counter = self.get_delivery_counter()
            counter.inc()
            content_counter = self.get_content_counter()
            content_counter.inc(self.extract_count_from(value))

            if not self.filter(value):
                return

            self.handle_with_listener(value, channel, method)

            end_time = time.time()
            process_timer.observe(end_time - start_time)

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
            if channel.is_open:
                channel.basic_ack(method.delivery_tag)
            else:
                logger.error('Message acknowledgment failed due to the channel being closed')

    def handle_with_listener(self, value, channel, method):
        with self.lock_listeners:
            for listener in self.listeners:
                try:
                    listener.handler(self.attributes, value)
                except Exception as e:
                    logger.warning(f"Message listener from class '{type(listener)}' threw exception {e}")

    @abstractmethod
    def value_from_bytes(self, body):
        pass

    @abstractmethod
    def filter(self, value) -> bool:
        pass
