import asyncio
import logging
from threading import Thread
from typing import Any, Dict, Optional, Callable, Awaitable
from contextlib import suppress

from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue, Exchange
from pydantic import BaseModel, Field


class MessageData(BaseModel):
    user_id: int = Field(..., description="ID пользователя")
    data: str = Field(..., description="Полезная нагрузка сообщения")


ConsumerCallback = Callable[[MessageData], Awaitable[Any]]

logger = logging.getLogger(__name__)


class FastStreamConnectionManager:

    def __init__(self,
                 configuration: RabbitMQConfiguration,
                 connection_manager_configuration: MqConnectionConfiguration) -> None:
        
        SetAllowOversizeProtos(connection_manager_configuration.message_recursion_limit > 100)

        self.__metrics = HealthMetrics(self)
        self.__metrics.enable()
        
        broker_url = f"amqp://{configuration.username}:{configuration.password}@{configuration.host}:{configuration.port}/{configuration.vhost}"
        self._broker = RabbitBroker(broker_url)
        self._app = FastStream(self._broker)
        
        self._prefetch_count = connection_manager_configuration.prefetch_count
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[Thread] = None
    
    def _start_background_loop(self) -> None:
        if self._loop:
            asyncio.set_event_loop(self._loop)
            self._loop.run_forever()
    
    def connect(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("ConnectionManager is already running.")
            return

        self._loop = asyncio.new_event_loop()
        self._thread = Thread(target=self._start_background_loop, daemon=True)
        self._thread.start()
        
        logger.info("Starting FastStream application in background thread.")

        future = asyncio.run_coroutine_threadsafe(self._app.start(), self._loop)
        future.result() 
        
        logger.info("FastStream Broker connected and consumers are running.")

    def add_consumer(self, queue_name: str, on_message_callback: ConsumerCallback) -> None:
        @self._broker.subscriber(
            queue=RabbitQueue(queue_name),
            prefetch_count=self._prefetch_count
        )
        async def faststream_handler(message: MessageData) -> None:
            await on_message_callback(message)
            
        logger.info(f"Consumer registered for queue: {queue_name}")

    def publish(self, exchange_name: str, routing_key: str, message: MessageData) -> None:
        if not self._loop:
            raise RuntimeError("ConnectionManager is not connected. Call .connect() first.")

        async def _publish_async():
            exchange = Exchange(exchange_name)
            await self._broker.publish(
                message, 
                exchange=exchange, 
                routing_key=routing_key
            )
        
        future = asyncio.run_coroutine_threadsafe(_publish_async(), self._loop)
        future.result()

    def close(self) -> None:
        if not self._loop or not self._thread or not self._thread.is_alive():
            logger.warning("ConnectionManager is not running.")
            return

        logger.info("Stopping FastStream application and background thread.")

        async def _shutdown_async():
            await self._app.stop()
            
            for task in asyncio.all_tasks(self._loop):
                if task is not asyncio.current_task(self._loop):
                    task.cancel()
                    with suppress(asyncio.exceptions.CancelledError):
                        await task
            self._loop.call_soon_threadsafe(self._loop.stop)
            
        future = asyncio.run_coroutine_threadsafe(_shutdown_async(), self._loop)
        future.result() 

        self._thread.join()

        self.__metrics.disable()

        logger.info("ConnectionManager closed successfully.")

    def __enter__(self) -> 'FastStreamConnectionManager':
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
