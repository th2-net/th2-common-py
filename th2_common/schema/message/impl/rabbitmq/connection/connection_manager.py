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
        self.connection_is_open = False
        self.__connection_thread = None
        self.connection_lock = threading.Lock()
        self.open_connection()

        self.__channels: Dict[str, Channel] = {}

    def open_connection(self):
        with self.connection_lock:
            self.connection = pika.SelectConnection(self.__connection_parameters)
            self.connection.add_on_close_callback(self.connection_close_callback)
            self.__connection_thread = threading.Thread(target=self.__run_connection_thread)
            self.__connection_thread.start()
            self.wait_connection_readiness()
            self.connection_is_open = True
            logging.info(f'Connection is open')

    def close_connection(self):
        with self.connection_lock:
            self.connection.close()
            self.__connection_thread.join(ConnectionManager.CONNECTION_CLOSE_TIMEOUT)
            logger.info(f'Connection is close')

    def reopen_connection(self):
        self.close_connection()
        self.open_connection()

    def connection_close_callback(self):
        if self.connection_is_open:
            self.reopen_connection()

    def __run_connection_thread(self):
        try:
            self.connection.ioloop.start()
        except Exception:
            logger.exception(f'Failed starting loop SelectConnection')

    def close(self):
        with self.connection_lock:
            self.connection_is_open = False
        self.close_connection()

    def wait_connection_readiness(self):
        while not self.connection.is_open:
            time.sleep(ConnectionManager.CONNECTION_READINESS_TIMEOUT)
