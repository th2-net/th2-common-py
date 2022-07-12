#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import logging
from typing import Any

from prometheus_client import Counter
from th2_common.schema.message.impl.rabbitmq.connection.connection_manager import ConnectionManager
from th2_common.schema.message.impl.rabbitmq.connection.publisher import Publisher
from th2_common.schema.message.message_sender import MessageSender
import th2_common.schema.metrics.common_metrics as common_metrics

logger = logging.getLogger(__name__)


class AbstractRabbitSender(MessageSender, ABC):

    OUTGOING_MSG_SIZE = Counter('th2_rabbitmq_message_size_publish_bytes',
                                'Amount of bytes sent',
                                common_metrics.SENDER_LABELS)
    OUTGOING_MSG_QUANTITY_ABSTRACT = Counter('th2_rabbitmq_message_publish_total',
                                             'Amount of batches sent',
                                             common_metrics.SENDER_LABELS)

    _TH2_TYPE = 'unknown'

    def __init__(self,
                 connection_manager: ConnectionManager,
                 exchange_name: str,
                 send_queue: str,
                 th2_pin: str = '') -> None:
        self.__publisher: Publisher = connection_manager.publisher
        self.__exchange_name: str = exchange_name
        self.__send_queue: str = send_queue
        self.__closed = True
        self.th2_pin = th2_pin

    def start(self) -> None:
        if self.__send_queue is None or self.__exchange_name is None:
            raise Exception('Sender can not start. Sender did not init')
        self.__closed = False

    def is_close(self) -> bool:
        return self.__closed

    def close(self) -> None:
        self.__closed = True

    def send(self, message: Any) -> None:
        labels = self.th2_pin, self._TH2_TYPE, self.__exchange_name, self.__send_queue

        if message is None:
            raise ValueError('Value for send can not be null')

        try:
            byted_message = self.value_to_bytes(message)
            self.__publisher.publish_message(exchange_name=self.__exchange_name,
                                             routing_key=self.__send_queue,
                                             message=byted_message)

            self.OUTGOING_MSG_QUANTITY_ABSTRACT.labels(*labels).inc()
            self.OUTGOING_MSG_SIZE.labels(*labels).inc(len(byted_message))

            if logger.isEnabledFor(logging.TRACE):  # type: ignore
                logger.trace('Sending to exchange_name = "%s", '  # type: ignore
                             'routing_key = "%s", '
                             'message = %s'
                             % (self.__exchange_name, self.__send_queue, self.to_trace_string(message)))
            elif logger.isEnabledFor(logging.DEBUG):
                logger.debug('Sending to exchange_name = "%s", '
                             'routing_key = "%s", '
                             'message = %s}'
                             % (self.__exchange_name, self.__send_queue, self.to_debug_string(message)))
        except Exception as e:
            logger.exception(f"Can't send: {e}")

    @staticmethod
    @abstractmethod
    def value_to_bytes(value: Any) -> bytes:
        pass

    @abstractmethod
    def to_trace_string(self, value: Any) -> str:
        pass

    @abstractmethod
    def to_debug_string(self, value: Any) -> str:
        pass
