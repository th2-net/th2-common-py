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
from typing import List, Union, Optional, Dict

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
    PUBLISHING_COROUTINE_NAME = 'publish'

    class FailedMessage:
        def __init__(self, exchange_name: str, routing_key: str, message: bytes, order: int):
            self.exchange_name: str = exchange_name
            self.routing_key: str = routing_key
            self.message: bytes = message
            self.order: int = order

    def __init__(self, connection_parameters: dict) -> None:
        self._connection_parameters: Dict[str, Union[str, int]] = connection_parameters
        self._connection: Optional[aio_pika.robust_connection.RobustConnection] = None
        self._channel: Optional[aio_pika.robust_channel.RobustChannel] = None
        self._exchange: Optional[aio_pika.robust_exchange.RobustExchange] = None
        self._exchange_dict: Dict[str, aio_pika.robust_exchange.RobustExchange] = {}
        self._asyncio_event: asyncio.Event = asyncio.Event()
        self._not_sent: List[Publisher.FailedMessage] = []
        self._connection_exceptions: List[Exception] = []
        self._task_id: int = 0
        self._message_ack: int = 0
        self._message_nack: int = 0

    async def connect(self):
        """Coroutine that creates connection and channel for publisher"""

        loop = asyncio.get_event_loop()

        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(loop=loop, **self._connection_parameters)
            except Exception as exc:
                logger.error(f'Exception was raised while connecting Publisher: {exc}')
                time.sleep(Publisher.DELAY_FOR_RECONNECTION)
        logger.info("Connection for Publisher has been created")

        while not self._channel:
            try:
                self._channel = await self._connection.channel()
            except Exception as exc:
                logger.error(f"Exception was raised while creating channel for Publisher {exc}")
                time.sleep(Publisher.DELAY_FOR_RECONNECTION)
        logger.info("Channel for Publisher has been created")

        self._asyncio_event.set()

    def _wait_for_reconnection(self):
        """Main thread waits until connection is restored"""

        while not self._asyncio_event.is_set():
            logger.info('Delaying for reconnection')
            time.sleep(Publisher.DELAY_FOR_RECONNECTION)

            if self._connection.connected.is_set():
                self._asyncio_event.set()
                self._not_sent = sorted(self._not_sent, key=lambda failed_message: failed_message.order, reverse=True)

    def publish_message(self,
                        exchange_name: str,
                        routing_key: str,
                        message: bytes) -> None:
        """ Publishes messages

        :param str exchange_name: Provides the name of an exchange that we will use to send messages
        :param str routing_key: Used by an exchange to route messages to the queue/queues
        :param bytes message: Message in bytes
        """

        if not self._asyncio_event.is_set():
            self._wait_for_reconnection()

            while self._not_sent:
                asyncio.run_coroutine_threadsafe(self._publish_message(**self._get_failed_message_parameters()),
                                                 self._connection.loop)

        exchange = self._get_exchange(exchange_name)

        asyncio.run_coroutine_threadsafe(self._publish_message(exchange, routing_key, message),
                                         self._connection.loop)

    def _get_exchange(self, exchange_name: str) -> aio_pika.robust_exchange.RobustExchange:
        """Returns an exchange object"""

        exchange = self._exchange_dict.get(exchange_name)

        while not exchange:
            async def get_exchange_coroutine(name: str) -> aio_pika.robust_exchange.RobustExchange:
                """Coroutine for getting exchange"""

                return await self._channel.get_exchange(name=name)

            try:
                exchange_future = asyncio.run_coroutine_threadsafe(get_exchange_coroutine(exchange_name),
                                                                   self._connection.loop)
                exchange = exchange_future.result()
                self._exchange_dict[exchange_name] = exchange
            except Exception as exc:
                logger.error(f'Exception was raised while getting exchange: {exc}')

        return exchange

    async def _publish_message(self, exchange, routing_key, message) -> None:
        """Coroutine for publishing messages"""

        message = Message(message, delivery_mode=DeliveryMode.PERSISTENT)

        publish = asyncio.create_task(exchange.publish(message=message, routing_key=routing_key),
                                      name=f'{self._task_ordering()}')

        publish.add_done_callback(functools.partial(self._done_callback, exchange, routing_key, message))

    def _task_ordering(self) -> int:
        """Updates id of the task"""

        self._task_id += 1
        return self._task_id

    def _done_callback(self,
                       exchange: aio_pika.robust_exchange.RobustExchange,
                       routing_key: str,
                       message: Message, task: asyncio.Task) -> None:
        """Callback that is run when asyncio.Task is done"""

        try:
            task_done_result = task.result()
            if isinstance(task_done_result, pamqp.specification.Basic.Ack):
                self._message_ack += 1
            elif isinstance(task_done_result, (pamqp.specification.Basic.Nack, pamqp.specification.Basic.Reject)):
                self._message_nack += 1
        except aio_pika.exceptions.CONNECTION_EXCEPTIONS as exc:
            self._asyncio_event.clear()
            if exc.__class__.__name__ not in self._connection_exceptions:
                logger.error(f"Connection issue: {exc}. "
                             f"DELIVERY OF ALL ALREADY SENT MESSAGES IS NOT GUARANTEED")
                self._connection_exceptions.append(exc.__class__.__name__)
            failed_message = self.FailedMessage(exchange.name, routing_key, message.body, int(task.get_name()))
            self._not_sent.append(failed_message)

    def _get_failed_message_parameters(self):
        """Returns details of the failed message"""

        failed_message = self._not_sent.pop()
        exchange = self._get_exchange(failed_message.exchange_name)
        routing_key = failed_message.routing_key
        message = failed_message.message

        return {"exchange": exchange, 'routing_key': routing_key, 'message': message}

    async def stop(self) -> None:
        """Coroutine for closing publisher's connection and channel"""

        while not self._asyncio_event.is_set():
            await asyncio.sleep(0)

            if self._connection.connected.is_set():
                self._asyncio_event.set()
                self._not_sent = sorted(self._not_sent, key=lambda failed_message: failed_message.order, reverse=True)

                while self._not_sent:
                    await self._publish_message(**self._get_failed_message_parameters())

        publishing_tasks = [
            task for task in asyncio.all_tasks() if task.get_coro().__name__ == Publisher.PUBLISHING_COROUTINE_NAME
        ]
        await asyncio.wait_for(asyncio.gather(*publishing_tasks), timeout=None)

        await self._channel.close()
        await self._connection.close()
