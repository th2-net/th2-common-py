import threading
import time

import pika

from th2_common import logger
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration


class ConnectionManager:

    def __init__(self, config: RabbitMQConfiguration) -> None:
        self.configuration: RabbitMQConfiguration = config
        self.__credentials = pika.PlainCredentials(config.username,
                                                   config.password)
        self.__connection_parameters = pika.ConnectionParameters(virtual_host=config.vhost,
                                                                 host=config.host,
                                                                 port=config.port,
                                                                 credentials=self.__credentials)

        self.publish_connection = self.__create_connection()
        logger.info(f'Create connection for publish: {self.publish_connection}')

        self.subscribe_connection = self.__create_connection()
        logger.info(f'Create connection for subscribe: {self.subscribe_connection}')

    def __create_connection(self):
        connection_open_timeout = 60
        connection = pika.SelectConnection(self.__connection_parameters)
        threading.Thread(target=self.__start_connection, args=(connection,)).start()
        for x in range(int(connection_open_timeout / 5)):
            if not connection.is_open:
                time.sleep(5)
        if not connection.is_open:
            raise ConnectionError(f'The connection has not been opened for {connection_open_timeout} seconds')

    @staticmethod
    def __start_connection(connection):
        try:
            logger.info(f'Start loop SelectConnection for {connection}')
            connection.ioloop.start()
        except Exception:
            logger.exception(f'Failed starting loop SelectConnection for {connection}')

    def close(self):
        if self.publish_connection is not None and self.publish_connection.is_open:
            self.publish_connection.close()
        if self.subscribe_connection is not None and self.subscribe_connection.is_open:
            self.subscribe_connection.close()
