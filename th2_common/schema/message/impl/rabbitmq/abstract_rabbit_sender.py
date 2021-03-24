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

import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Optional

from pika.channel import Channel
from prometheus_client import Counter

from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.message_sender import MessageSender

logger = logging.getLogger()


class AbstractRabbitSender(MessageSender, ABC):

    def __init__(self, connection_manager: ConnectionManager, exchange_name: str, send_queue: str) -> None:
        self.connection_manager: ConnectionManager = connection_manager
        self.channel: Optional[Channel] = None
        self.channel_need_open = True
        self.exchange_name: str = exchange_name
        self.send_queue: str = send_queue
        self.channel_lock = threading.Lock()

    def start(self):
        if self.send_queue is None or self.exchange_name is None:
            raise Exception('Sender can not start. Sender did not init')
        if self.channel is None:
            self.check_and_open_channel()

    def check_and_open_channel(self):
        self.connection_manager.wait_connection_readiness()
        with self.channel_lock:
            if self.channel is None or not self.channel.is_open:
                self.channel = self.connection_manager.connection.channel()
                self.channel.add_on_close_callback(self.channel_close_callback)
                self.wait_channel_readiness()
                logger.info(f"Channel #{self.channel.channel_number} is open for send queue='{self.send_queue}'")

    def channel_close_callback(self, channel, reason):
        logger.info(f"Channel #{channel.channel_number} is close, reason: {reason}")
        with self.connection_manager.channels_lock:
            if self.channel_need_open:
                if not self.connection_manager.connection.is_open:
                    self.connection_manager.reopen_connection()
                self.check_and_open_channel()

    def wait_channel_readiness(self):
        while not self.channel.is_open:
            time.sleep(ConnectionManager.CHANNEL_READINESS_TIMEOUT)

    def is_close(self) -> bool:
        return self.channel is None or not self.channel.is_open

    def close(self):
        if self.channel is not None and self.channel.is_open:
            self.channel.close()
            self.channel_need_open = False
            logger.info(f"Close channel: {self.channel} for sender[{self.exchange_name}:{self.send_queue}]")

    def send(self, message):
        if message is None:
            raise ValueError('Value for send can not be null')
        try:
            self.check_and_open_channel()
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.send_queue,
                                       body=self.value_to_bytes(message))

            counter = self.get_delivery_counter()
            counter.inc()
            content_counter = self.get_content_counter()
            content_counter.inc(self.extract_count_from(message))

            logger.debug(f"Sent message:\n{message}"
                         f"Exchange: '{self.exchange_name}', routing key: '{self.send_queue}'")

        except Exception:
            logger.exception('Can not send')
            raise

    @abstractmethod
    def value_to_bytes(self, value):
        pass

    @abstractmethod
    def get_delivery_counter(self) -> Counter:
        pass

    @abstractmethod
    def get_content_counter(self) -> Counter:
        pass

    @abstractmethod
    def extract_count_from(self, message):
        pass
