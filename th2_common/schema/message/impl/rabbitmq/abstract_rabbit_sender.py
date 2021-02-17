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
import logging
import time
from abc import ABC, abstractmethod

from prometheus_client import Counter

from th2_common.schema.exception.router_error import RouterError
from th2_common.schema.message.message_sender import MessageSender

logger = logging.getLogger()


class AbstractRabbitSender(MessageSender, ABC):

    def __init__(self, connection, exchange_name: str, send_queue: str) -> None:
        self.CLOSE_TIMEOUT = 1_000
        self.connection = connection
        self.channel = None
        self.exchange_name = exchange_name
        self.send_queue = send_queue

    def start(self):
        if self.send_queue is None or self.exchange_name is None:
            raise Exception('Sender can not start. Sender did not init')
        if self.channel is None:
            self.channel = self.connection.channel()
            CHANNEL_OPEN_TIMEOUT = 60
            for x in range(int(CHANNEL_OPEN_TIMEOUT / 5)):
                if not self.channel.is_open:
                    time.sleep(5)
            if not self.channel.is_open:
                raise RouterError(f"The channel has not been opened for {CHANNEL_OPEN_TIMEOUT} seconds")
            logger.info(f"Create channel: {self.channel} for sender[{self.exchange_name}:{self.send_queue}]")

    def is_close(self) -> bool:
        return self.channel is None or not self.channel.is_open

    def close(self):
        if self.channel is not None and self.channel.is_open:
            logger.info(f"Close channel: {self.channel} for sender[{self.exchange_name}:{self.send_queue}]")
            self.channel.close()

    @abstractmethod
    def get_delivery_counter(self) -> Counter:
        pass

    @abstractmethod
    def get_content_counter(self) -> Counter:
        pass

    @abstractmethod
    def extract_count_from(self, message):
        pass

    def send(self, message):

        if message is None:
            raise ValueError('Value for send can not be null')

        counter = self.get_delivery_counter()
        counter.inc()
        content_counter = self.get_content_counter()
        content_counter.inc(self.extract_count_from(message))

        try:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.send_queue,
                                       body=self.value_to_bytes(message))
            logger.info(f"Sent message:\n{message}"
                        f"Exchange: '{self.exchange_name}', routing key: '{self.send_queue}'")
        except Exception:
            if self.channel is None:
                raise Exception('Can not send. Sender did not started')
            raise

    @abstractmethod
    def value_to_bytes(self, value):
        pass
