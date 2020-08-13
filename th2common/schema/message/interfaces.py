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

from th2common.schema.message.configurations import QueueConfiguration, MessageRouterConfiguration
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration


class MessageListener(ABC):

    @abstractmethod
    def handler(self, consumer_tag: str, message):
        pass

    def on_close(self):
        pass


class MessageSender(ABC):

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def is_close(self) -> bool:
        pass

    @abstractmethod
    def send(self, message):
        pass


class MessageSubscriber(ABC):

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def is_close(self) -> bool:
        pass

    @abstractmethod
    def add_listener(self, message_listener: MessageListener):
        pass

    @abstractmethod
    def close(self):
        pass


class MessageQueue(ABC):

    def __init__(self, configuration: RabbitMQConfiguration, queue_configuration: QueueConfiguration) -> None:
        self.configuration = configuration
        self.queue_configuration = queue_configuration

    @abstractmethod
    def get_subscriber(self) -> MessageSubscriber:
        pass

    @abstractmethod
    def get_sender(self) -> MessageSender:
        pass

    @abstractmethod
    def close(self):
        pass


class SubscriberMonitor(ABC):

    @abstractmethod
    def unsubscribe(self):
        pass


class MessageRouter(ABC):
    """
    Interface for send and receive RabbitMQ messages
    """

    def __init__(self, rabbit_mq_configuration: RabbitMQConfiguration,
                 configuration: MessageRouterConfiguration) -> None:
        self.configuration = configuration
        self.rabbit_mq_configuration = rabbit_mq_configuration

    @abstractmethod
    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        """
        RabbitMQ queue by intersection schemas queues attributes
        :param callback: listener
        :param queue_attr: queues attributes
        :return: SubscriberMonitor it start listening. Returns None is can not listen this queue
        """
        pass

    @abstractmethod
    def unsubscribe_all(self):
        """
        Unsubscribe from all queues
        :return:
        """
        pass

    @abstractmethod
    def send(self, message, *queue_attr):
        pass
