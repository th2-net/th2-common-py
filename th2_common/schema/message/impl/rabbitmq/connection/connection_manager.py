import logging
import threading

import pika

from th2_common.schema.message.configuration.message_configuration import ConnectionManagerConfiguration
from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2_common.schema.message.impl.rabbitmq.connection.reconnecting_consumer import ReconnectingConsumer
from th2_common.schema.message.impl.rabbitmq.connection.reconnecting_publisher import ReconnectingPublisher
from th2_common.schema.metrics.common_metrics import HealthMetrics


logger = logging.getLogger(__name__)


class ConnectionManager:

    def __init__(self, connection: RabbitMQConfiguration,
                 connectionManagerConfiguration: ConnectionManagerConfiguration) -> None:
        self.__credentials = pika.PlainCredentials(connection.username,
                                                   connection.password)
        self.__connection_parameters = pika.ConnectionParameters(virtual_host=connection.vhost,
                                                                 host=connection.host,
                                                                 port=connection.port,
                                                                 credentials=self.__credentials
                                                                 )
        self.__metrics = HealthMetrics(self)

        self.consumer = ReconnectingConsumer(connection, self.__connection_parameters,
                                             connectionManagerConfiguration)
        threading.Thread(target=self.consumer.run).start()

        self.publisher = ReconnectingPublisher(self.__connection_parameters)
        threading.Thread(target=self.publisher.run).start()

        self.__metrics.enable()

    def close(self):
        try:
            self.consumer.stop()
        except Exception:
            logger.exception("Error while stopping Consumer")
        try:
            self.publisher.stop()
        except Exception:
            logger.exception("Error while stopping Publisher")
        self.__metrics.disable()
