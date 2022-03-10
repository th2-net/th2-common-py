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


import asyncio
import logging
import time
import functools
from typing import List, Tuple

import aio_pika
import pamqp
from aio_pika import Message, DeliveryMode

logger = logging.getLogger(__name__)


class Publisher:
    """Creates Publisher

    `Publisher` initializes connection and channel, publishes messages and finally closes connection.

    :param dict connection_parameters: Provides RabbitMQ configuration parameters for connection
    """

    DELAY_FOR_RECONNECTION = 5

    def __init__(self, connection_parameters: dict) -> None:
        self._connection_parameters: dict = connection_parameters
        self._connection: aio_pika.robust_connection.RobustConnection = None
        self._channel: aio_pika.robust_channel.RobustChannel = None
        self._exchange: aio_pika.robust_exchange.RobustExchange = None
        self._exchange_dict: dict = dict()
        self._asyncio_event: asyncio.Event = asyncio.Event()
        self._not_sent: List[Tuple[str, str, bytes, int]] = []
        self._coro_name: int = 0
        self._ack: int = 0
        self._nack: int = 0

    async def connect(self):
        """Coroutine that creates connection and channel for publisher"""
        loop = asyncio.get_event_loop()

        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(loop=loop, **self._connection_parameters)
                logger.info("Publisher's connection has been opened")
            except Exception as err:
                logger.error(f'Error occurred while connecting {err}')
                time.sleep(Publisher.DELAY_FOR_RECONNECTION)

        while not self._channel:
            try:
                self._channel = await self._connection.channel()
                logger.info(f"Publisher's channel has been created")
            except Exception as exc:
                logger.error(f"Error occurred while creating Publisher's channel {exc}")

        self._asyncio_event.set()

    def publish_message(self,
                        exchange_name: str,
                        routing_key: str,
                        message: bytes) -> None:
        """ Publishes messages

        :param str exchange_name: Provides the name of an exchange that we will use to send messages
        :param str routing_key: Used by an exchange to route messages to the queue/queues
        :param bytes message: Message in bytes
        """

        while not self._asyncio_event.is_set():
            logger.info("Delaying for reconnection")
            time.sleep(Publisher.DELAY_FOR_RECONNECTION)

            if self._connection.connected.is_set():
                self._asyncio_event.set()
                self._not_sent.sort(key=lambda tup: tup[-1], reverse=True)

        if self._not_sent:
            self._republish_message()

        exchange = self._get_exchange(exchange_name)

        asyncio.run_coroutine_threadsafe(self._publish_message(exchange, routing_key, message),
                                         self._connection.loop)

    def _get_exchange(self, exchange_name: str) -> aio_pika.robust_exchange.RobustExchange:

        exchange = self._exchange_dict.get(exchange_name)

        async def get_exchange_coro(name: str) -> aio_pika.robust_exchange.RobustExchange:
            return await self._channel.get_exchange(name=name)

        while not exchange:
            try:
                exchange_future = asyncio.run_coroutine_threadsafe(get_exchange_coro(exchange_name),
                                                                   self._connection.loop)
                exchange = exchange_future.result()
                self._exchange_dict[exchange_name] = exchange
                break
            except Exception as exp:
                logger.error(exp)

        return exchange

    async def _publish_message(self, exchange, routing_key, message) -> None:
        """Coroutine for publishing messages"""
        message = Message(message, delivery_mode=DeliveryMode.PERSISTENT)

        publish = asyncio.create_task(exchange.publish(message=message, routing_key=routing_key),
                                      name=f"{self._coro_ordering()}")

        publish.add_done_callback(functools.partial(self._done_callback, exchange, routing_key, message))

    def _republish_message(self) -> None:
        while self._not_sent:
            failed_message = self._not_sent.pop()
            exchange, routing_key, message = self._get_exchange(failed_message[0]), failed_message[1], failed_message[2]

            asyncio.run_coroutine_threadsafe(self._publish_message(exchange, routing_key, message),
                                             self._connection.loop)

    def _coro_ordering(self) -> int:
        self._coro_name += 1
        return self._coro_name

    def _done_callback(self,
                       exchange: aio_pika.robust_exchange.RobustExchange,
                       routing_key: str,
                       message: Message, task: asyncio.Task) -> None:
        try:
            task_done_result = task.result()
            if isinstance(task_done_result, pamqp.specification.Basic.Ack):
                self._ack += 1
            elif isinstance(task_done_result, (pamqp.specification.Basic.Nack, pamqp.specification.Basic.Reject)):
                self._nack += 1
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS as ex:
            self._asyncio_event.clear()
            self._not_sent.append((exchange.name, routing_key, message.body, int(task.get_name())))

    async def stop(self) -> set:
        """Coroutine for closing publisher's connection and channel

        :return: All unfinished tasks
        :rtype: set(asyncio.Task)
        """

        if self._not_sent:
            self._republish_message()

        publishing_tasks = [task for task in asyncio.all_tasks() if task.get_coro().__name__ == 'publish']
        await asyncio.wait_for(asyncio.gather(*publishing_tasks), timeout=None)
        await self._channel.close()
        await self._connection.close()

        return asyncio.all_tasks()
