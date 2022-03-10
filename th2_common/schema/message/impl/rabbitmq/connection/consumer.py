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
import datetime
import logging
import time
from typing import Dict, Optional, Union, Callable, Any

import aio_pika

from th2_common.schema.message.configuration.message_configuration import ConnectionManagerConfiguration


logger = logging.getLogger(__name__)


class Consumer:
    """Creates Consumer

    `Consumer` implements all methods to create connection and channel for consuming messages, adding subscribers and
    finally closing channel and connection.

    :param :class: `ConnectionManagerConfiguration` connection_manager_configuration:
    :param dict connection_parameters: Provides RabbitMQ configuration parameters for connection
    """

    DELAY_FOR_RECONNECTION = 5

    def __init__(self,
                 connection_manager_configuration: ConnectionManagerConfiguration,
                 connection_parameters: dict) -> None:

        self._subscriber_name: str = connection_manager_configuration.subscriber_name
        self._prefetch_count: str = connection_manager_configuration.prefetch_count
        self._subscribers: dict = dict()
        self._queue_dict: dict = dict()
        self._connection_parameters: Dict[str, Union[str, int]] = connection_parameters
        self._connection: Optional[aio_pika.robust_connection.RobustConnection] = None
        self._channel: Optional[aio_pika.channel.Channel] = None
        self.__consumer_tag_id: int = -1

    async def connect(self) -> None:
        """Coroutine that creates connection, channel for publisher and sets QOS"""

        loop = asyncio.get_running_loop()
        while not self._connection:
            try:
                self._connection = await aio_pika.connect_robust(loop=loop, **self._connection_parameters)
                logger.info("Consumer's connection has been opened")
                self._channel = await self._connection.channel()
                logger.info(f"Consumer's channel has been created")
                await self._channel.set_qos(prefetch_count=self._prefetch_count)
                logger.info(f"QOS set to: {self._prefetch_count}")
            except Exception as ex:
                logger.info(f"Exception in Consumer: {ex}")
                time.sleep(Consumer.DELAY_FOR_RECONNECTION)

    def next_id(self) -> int:
        """Unique id for consumer_tag"""

        self.__consumer_tag_id += 1
        return self.__consumer_tag_id

    def _get_queue(self, consumer_tag) -> aio_pika.Queue:
        """Gets Queue

        :return: Queue object if it exists or None if not
        :rtype: Union[:class: `aio_pika.robust_queue.RobustQueue`, None]
        """
        queue_name = self._subscribers[consumer_tag][0]
        return self._queue_dict.get(queue_name, None)

    def add_subscriber(self, queue: Union[None, aio_pika.Queue],
                       on_message_callback: Callable[[aio_pika.message.IncomingMessage], Any]) -> str:
        """ Adding subscriber

        :param :class: `Union[None, aio_pika.Queue]` queue: Queue object from where messages will be consumed
        :param :class: `Callable[[aio_pika.message.IncomingMessage], Any]` on_message_callback: Called for every
        message consumed

        :return: consumer_tag
        :rtype: str
        """

        if self._subscriber_name is None:
            self._subscriber_name = 'rabbitmq_subscriber'
            logger.info(f"Using default subscriber name: '{self._subscriber_name}'")
        consumer_tag = f'{self._subscriber_name}.{self.next_id()}.{datetime.datetime.now()}'
        self._subscribers[consumer_tag] = (queue, on_message_callback)

        asyncio.run_coroutine_threadsafe(self._start_consuming(consumer_tag), self._connection.loop)

        return consumer_tag

    async def _start_consuming(self, consumer_tag: str) -> None:
        """Coroutine for consuming messages from queue"""

        queue = self._get_queue(consumer_tag)
        callback = self._subscribers[consumer_tag][1]
        if not queue:
            logger.info("Getting Queue...")
            queue_name = self._subscribers[consumer_tag][0]
            queue = await self._channel.get_queue(name=queue_name)
            self._queue_dict[queue_name] = queue
        await queue.consume(callback=callback, consumer_tag=consumer_tag)

    def remove_subscriber(self, consumer_tag: str) -> None:
        """Remove subscriber and cancel consuming from queue"""

        remove_sub = asyncio.run_coroutine_threadsafe(self._stop_consuming(consumer_tag), self._connection.loop)
        remove_sub.result()
        self._subscribers.pop(consumer_tag)

    async def _stop_consuming(self, consumer_tag: str) -> None:
        """Coroutine to cancel consuming from queue"""

        logger.info('Sending a Basic.Cancel RPC command to RabbitMQ for tag: %s', consumer_tag)
        queue = self._get_queue(consumer_tag)
        await queue.cancel(consumer_tag)
        logger.info('RabbitMQ acknowledged the cancellation of the consumer: %s', consumer_tag)

    async def stop(self) -> None:
        """Cancel consuming for every subscriber and close connection"""

        logger.info(f"Closing Consumer's connection")
        for consumer_tag in self._subscribers.keys():
            await self._stop_consuming(consumer_tag)
        try:
            await self._connection.close()
        except Exception as ex:
            logger.error(f"Error while closing: {ex}")
