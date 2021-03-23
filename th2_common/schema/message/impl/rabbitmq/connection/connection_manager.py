import logging
import threading
import time
from typing import Optional, Dict

import pika
from pika.channel import Channel

from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration

logger = logging.getLogger()


class ConnectionManager:
    CONNECTION_READINESS_TIMEOUT = 5
    CHANNEL_READINESS_TIMEOUT = 5
    CONNECTION_CLOSE_TIMEOUT = 60

    def __init__(self, config: RabbitMQConfiguration) -> None:
        self.configuration: RabbitMQConfiguration = config
        self.__credentials = pika.PlainCredentials(config.username,
                                                   config.password)
        self.__connection_parameters = pika.ConnectionParameters(virtual_host=config.vhost,
                                                                 host=config.host,
                                                                 port=config.port,
                                                                 credentials=self.__credentials)
        self.connection: Optional[pika.SelectConnection] = None
        self.connection_need_open = True
        self.connection_lock = threading.Lock()
        self.channels_lock = threading.Lock()
        self.open_connection()

        self.__channels: Dict[str, Channel] = {}

    def open_connection(self):
        with self.connection_lock:
            self.connection = pika.SelectConnection(parameters=self.__connection_parameters)
            threading.Thread(target=self.__run_connection_thread).start()
            self.wait_connection_readiness()
            logging.info(f'Connection is open')

    def close_connection(self):
        with self.connection_lock:
            if self.connection.is_open:
                self.connection.ioloop.add_callback_threadsafe(self.connection.ioloop.stop)
                self.connection.close()
                logger.info(f'Connection is close')

    def reopen_connection(self):
        if self.connection.is_open:
            return
        self.close_connection()
        self.open_connection()

    def __run_connection_thread(self):
        try:
            self.connection.ioloop.start()
        except Exception:
            logger.exception(f'Failed starting loop SelectConnection')

    def close(self):
        self.connection_need_open = False
        self.close_connection()

    def wait_connection_readiness(self):
        while not self.connection.is_open:
            time.sleep(ConnectionManager.CONNECTION_READINESS_TIMEOUT)