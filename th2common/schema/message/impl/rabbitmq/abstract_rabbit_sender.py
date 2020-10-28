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

import pika

from th2common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2common.schema.message.message_sender import MessageSender


class AbstractRabbitSender(MessageSender, ABC):

    def __init__(self, configuration: RabbitMQConfiguration, exchange_name: str, send_queue: str) -> None:
        self.CLOSE_TIMEOUT = 1_000
        self.connection = None
        self.channel = None
        self.send_queue = send_queue
        self.exchange_name = exchange_name
        credentials = pika.PlainCredentials(configuration.username, configuration.password)
        self.connection_parameters = pika.ConnectionParameters(virtual_host=configuration.vhost,
                                                               host=configuration.host,
                                                               port=configuration.port,
                                                               credentials=credentials)

    def start(self):
        if self.send_queue is None or self.exchange_name is None:
            raise Exception('Sender can not start. Sender did not init')
        if self.connection is None:
            self.connection = pika.BlockingConnection(self.connection_parameters)
        if self.channel is None:
            self.channel = self.connection.channel()

    def is_close(self) -> bool:
        return self.connection is None or not self.connection.is_open

    def send(self, message):
        if self.channel is None:
            raise Exception('Can not send. Sender did not started')
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.send_queue,
                                   body=self.value_to_bytes(message))

    def close(self):
        if self.connection is not None and self.connection.is_open:
            self.connection.close()

    @abstractmethod
    def value_to_bytes(self, value):
        pass
