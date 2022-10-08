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


import asyncio
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import aio_pika
from aio_pika import Message
from aio_pika.robust_channel import RobustChannel
from aio_pika.robust_connection import RobustConnection
from aio_pika.robust_exchange import RobustExchange
import requests
from th2_common.schema.message.configuration.message_configuration import MqConnectionConfiguration

logger = logging.getLogger(__name__)


class FailedMessage:
    def __init__(self, exchange_name: str, routing_key: str, message: bytes, order: int) -> None:
        self.exchange_name: str = exchange_name
        self.routing_key: str = routing_key
        self.message: bytes = message
        self.order: int = order


class Publisher:
    """Creates Publisher

    `Publisher` initializes connection and channel, publishes messages and finally closes connection.

    :param dict connection_parameters: Provides RabbitMQ configuration parameters for connection
    """

    DELAY_FOR_RECONNECTION = 5
    PUBLISHING_COROUTINE_NAME = '_publish_message'

    def __init__(self, connection_manager_configuration: MqConnectionConfiguration,
                 connection_parameters: Dict[str, Any]) -> None:
        self._connection_parameters: Dict[str, Any] = connection_parameters
        self._connection: Optional[RobustConnection] = None
        self._channel: Optional[RobustChannel] = None
        self._exchange: Optional[RobustExchange] = None
        self._exchange_dict: Dict[str, RobustExchange] = {}
        self._connection_event: asyncio.Event = asyncio.Event()
        self._publish_event: threading.Event = threading.Event()
        self._not_sent: List[FailedMessage] = []
        self._connection_exceptions: List[Exception] = []
        self._message_number: int = 0
        self._republishing: bool = False
        self._count_of_messages: int = 0
        self._max_messages: int = connection_manager_configuration.max_messages
        self._check_interval: int = connection_manager_configuration.check_interval
        self._session = requests.Session()

    async def connect(self) -> None:
        """Coroutine that creates connection and channel for publisher"""

        loop = asyncio.get_event_loop()

        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(loop=loop, **self._connection_parameters)
            except Exception as e:
                logger.error(f'Exception was raised while connecting Publisher: {e}')
                time.sleep(Publisher.DELAY_FOR_RECONNECTION)
        logger.info('Connection for Publisher has been created')

        while not self._channel:
            try:
                self._channel = await self._connection.channel()
            except Exception as e:
                logger.error(f'Exception was raised while creating channel for Publisher {e}')
                time.sleep(Publisher.DELAY_FOR_RECONNECTION)
        logger.info('Channel for Publisher has been created')

        self._connection_event.set()
        self._publish_event.set()

    async def _get_exchange_coroutine(self, name: str) -> RobustExchange:
        """Coroutine for getting exchange"""

        return await self._channel.get_exchange(name=name)  # type: ignore

    def _get_exchange(self, exchange_name: str) -> RobustExchange:
        """Returns an exchange object"""

        exchange = self._exchange_dict.get(exchange_name)

        while not exchange:
            try:
                exchange_future = asyncio.run_coroutine_threadsafe(self._get_exchange_coroutine(exchange_name),
                                                                   self._connection.loop)  # type: ignore
                exchange = exchange_future.result()
                self._exchange_dict[exchange_name] = exchange
            except Exception as e:
                logger.error(f'Exception was raised while getting exchange: {e}')

        return exchange

    def get_queues(self) -> list:
        url = f'http://{self._connection_parameters["host"]}:1{self._connection_parameters["port"]}/api/queues'
        return self._session.get(url,
                                 auth=(self._connection_parameters['login'],
                                       self._connection_parameters['password'])
                                 ).json()

    def get_bindings_info(self) -> list:
        url = f'http://{self._connection_parameters["host"]}:1{self._connection_parameters["port"]}/api/definitions'
        return self._session.get(url,
                                 auth=(self._connection_parameters['login'],
                                       self._connection_parameters['password'])
                                 ).json()['bindings']

    def get_queues_info(self, routing_key: str) -> list:
        destination_queues = [
            item['destination'] for item in filter(
                                                    lambda x: x['routing_key'] == routing_key,
                                                    self.get_bindings_info()
                                                                      )
        ]
        return list(filter(lambda x: x['name'] in destination_queues, self.get_queues()))

    def queues_message_count(self, routing_key: str, unacked: bool = True, ready: bool = True) -> int:
        return sum([
            ready * queue['messages_ready'] + unacked * queue['messages_unacknowledged']
            for queue in self.get_queues_info(routing_key)
        ])

    def publish_message(self,
                        exchange_name: str,
                        routing_key: str,
                        message: bytes) -> None:
        """ Publishes messages

        :param str exchange_name: Provides the name of an exchange that will be used to send messages
        :param str routing_key: Used by an exchange to route messages to the queue/queues
        :param bytes message: Message in bytes
        """

        if self._message_number % self._check_interval == 0:
            self._count_of_messages = self.queues_message_count(routing_key)
        if self._count_of_messages >= self._max_messages:
            logger.warning("Queue at maximum capacity! Couldn't send the message!")
            return

        self._publish_event.wait()

        exchange = self._get_exchange(exchange_name)

        asyncio.run_coroutine_threadsafe(self._publish_message(exchange, routing_key, message),
                                         self._connection.loop)  # type: ignore

    def _message_number_update(self) -> int:
        """Updates message number"""

        self._message_number += 1
        return self._message_number

    async def _publish_message(self,
                               exchange: RobustExchange,
                               routing_key: str,
                               data: bytes) -> None:
        """Coroutine for publishing messages"""

        message_number = self._message_number_update()
        message: Message = Message(data)

        try:
            await exchange.publish(message=message, routing_key=routing_key)
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS as e:
            self._connection_event.clear()
            self._publish_event.clear()
            if e.__class__.__name__ not in self._connection_exceptions:
                logger.error(f'Connection issue: {e}. '
                             f'DELIVERY OF ALL ALREADY SENT MESSAGES IS NOT GUARANTEED')
                self._connection_exceptions.append(e.__class__.__name__)
            failed = FailedMessage(exchange.name, routing_key, message.body, message_number)
            self._not_sent.append(failed)

        if self._not_sent and not self._republishing:
            self._republishing = True
            asyncio.create_task(self._republish_messages(), name='republish')

    async def _wait_for_connection(self) -> None:
        """Waits for connection to be restored"""

        while not self._connection_event.is_set():
            await asyncio.sleep(Publisher.DELAY_FOR_RECONNECTION)

            if self._connection.connected.is_set():  # type: ignore
                self._connection_event.set()
                logger.info('Connection was restored')

    async def _republish_messages(self) -> None:
        """Republish messages that were failed due to connection issues"""

        await self._wait_for_connection()

        self._not_sent = sorted(self._not_sent, key=lambda failed_message: failed_message.order, reverse=True)

        while self._not_sent:
            asyncio.create_task(self._publish_message(*self._get_failed_message_parameters()))

        self._republishing = False
        self._publish_event.set()

    def _get_failed_message_parameters(self) -> Tuple[RobustExchange, str, bytes]:
        """Return details of the failed message"""

        failed_message = self._not_sent.pop()
        exchange = self._get_exchange(failed_message.exchange_name)
        routing_key = failed_message.routing_key
        message = failed_message.message

        return exchange, routing_key, message

    async def stop(self) -> None:
        """Coroutine for closing publisher's connection and channel"""

        if not self._connection_event.is_set():
            await self._wait_for_connection()

        publishing_tasks = [
            task for task in asyncio.all_tasks()
            if task.get_coro().__name__ == Publisher.PUBLISHING_COROUTINE_NAME  # type: ignore
        ]

        await asyncio.wait_for(asyncio.gather(*publishing_tasks), timeout=None)

        await self._connection.close()  # type: ignore
