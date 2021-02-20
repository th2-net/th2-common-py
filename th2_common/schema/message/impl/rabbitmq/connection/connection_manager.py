import logging
import threading
import time

import pika

from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration

logger = logging.getLogger()


class ConnectionManager:

    def __init__(self, config: RabbitMQConfiguration) -> None:
        self.configuration: RabbitMQConfiguration = config
        self.__credentials = pika.PlainCredentials(config.username,
                                                   config.password)
        self.__connection_parameters = pika.ConnectionParameters(virtual_host=config.vhost,
                                                                 host=config.host,
                                                                 port=config.port,
                                                                 credentials=self.__credentials)
        self.connection = None
        self.__connection_thread = None
        self.connection_lock = threading.Lock()
        self.open_connection()

        self.reconnect_attempts = 10

    def open_connection(self):
        with self.connection_lock:
            connection_open_timeout = 60
            self.connection = pika.SelectConnection(self.__connection_parameters)
            self.__connection_thread = threading.Thread(target=self.__run_connection_thread)
            self.__connection_thread.start()
            for x in range(int(connection_open_timeout / 5)):
                if not self.connection.is_open:
                    time.sleep(5)
            if not self.connection.is_open:
                raise ConnectionError(f'The connection has not been opened for {connection_open_timeout} seconds')
            logger.info(f'Connection is open')

    def close_connection(self):
        with self.connection_lock:
            connection_close_timeout = 60
            self.connection.close()
            self.__connection_thread.join(connection_close_timeout)
            logger.info(f'Connection is close')

    def reopen_connection(self):
        self.close_connection()
        self.open_connection()

    def __run_connection_thread(self):
        try:
            self.connection.ioloop.start()
        except Exception:
            logger.exception(f'Failed starting loop SelectConnection')

    def close(self):
        self.close_connection()
