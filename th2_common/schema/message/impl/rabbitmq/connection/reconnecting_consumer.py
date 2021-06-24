import datetime
import functools
import logging
import threading
import time
from typing import Dict, Optional

import pika
from pika import SelectConnection
from pika.channel import Channel

from th2_common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration


logger = logging.getLogger(__name__)


class Consumer:

    def __init__(self, parameters: pika.ConnectionParameters, prefetch_count,
                 consuming: Dict[str, bool], subscribers: Dict[str, tuple]):
        self.should_reconnect = False
        self.was_consuming = False

        self._parameters = parameters
        self._prefetch_count = prefetch_count
        self._consuming: Dict[str, bool] = consuming
        self._subscribers: Dict[str, tuple] = subscribers

        self._connection: Optional[SelectConnection] = None
        self._channel: Optional[Channel] = None
        self._closing = False

    def connect(self):
        logger.info('Connecting by ReconnectingConsumer')
        return pika.SelectConnection(
            parameters=self._parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def close_connection(self):
        for consumer_tag in self._consuming.keys():
            self._consuming[consumer_tag] = False
        if self._connection.is_closing or self._connection.is_closed:
            logger.info('Consumer connection is closing or already closed')
        else:
            logger.info('Closing consumer connection')
            self._connection.close()

    def on_connection_open(self, _unused_connection):
        logger.info('Consumer connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        logger.error('Consumer connection open failed: %s', err)
        self.reconnect()

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            logger.warning('Consumer connection closed, reconnect necessary: %s', reason)
            self.reconnect()

    def reconnect(self):
        self.should_reconnect = True
        self.stop()

    def open_channel(self):
        logger.info('Creating a channel for Consumer')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logger.info('Consumer channel opened')
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self.set_qos()

    def on_channel_closed(self, channel, reason):
        if reason.reply_code != 0:
            logger.warning('Channel %i was closed: %s', channel, reason)
        self.close_connection()

    def set_qos(self):
        self._channel.basic_qos(prefetch_count=self._prefetch_count,
                                callback=self.on_basic_qos_ok)

    def on_basic_qos_ok(self, _unused_frame):
        logger.info('QOS set to: %d', self._prefetch_count)
        for consumer_tag in self._subscribers.keys():
            self.start_consuming(consumer_tag)

    def start_consuming(self, consumer_tag):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
        self._channel.basic_consume(queue=self._subscribers[consumer_tag][0],
                                    consumer_tag=consumer_tag,
                                    on_message_callback=self._subscribers[consumer_tag][1])
        self.was_consuming = True
        self._consuming[consumer_tag] = True

    def on_consumer_cancelled(self, method_frame):
        logger.info('Consumer was cancelled remotely, shutting down: %r', method_frame)
        if self._channel:
            self._channel.close()

    def stop_consuming(self, consumer_tag):
        if self._channel:
            logger.info('Sending a Basic.Cancel RPC command to RabbitMQ')
            cb = functools.partial(self.on_cancel_ok, userdata=consumer_tag)
            self._channel.basic_cancel(consumer_tag, cb)

    def on_cancel_ok(self, _unused_frame, userdata):
        self._consuming[userdata] = False
        logger.info('RabbitMQ acknowledged the cancellation of the consumer: %s', userdata)
        for consumer_tag in self._consuming.keys():
            if self._consuming[consumer_tag] is True:
                return
        self.close_channel()

    def close_channel(self):
        logger.info('Closing the Consumer channel')
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        self._connection.ioloop.start()

    def stop(self):
        if not self._closing:
            self._closing = True
            logger.info('Stopping ReconnectingConsumer')
            for consumer_tag in self._consuming.keys():
                if self._consuming:
                    self.stop_consuming(consumer_tag)
            else:
                self._connection.ioloop.stop()
            logger.info('Stopped ReconnectingConsumer')

    def add_callback_threadsafe(self, cb):
        self._connection.ioloop.add_callback_threadsafe(cb)


class ReconnectingConsumer(object):
    def __init__(self, configuration: RabbitMQConfiguration, connection_parameters: pika.ConnectionParameters):
        self._configuration: RabbitMQConfiguration = configuration
        self._connection_parameters: pika.ConnectionParameters = connection_parameters
        self._consuming: Dict[str, bool] = dict()
        self._subscribers: Dict[str, tuple] = dict()
        self._reconnect_delay = 0

        self._subscriber_name = configuration.subscriber_name

        self._consumer = Consumer(connection_parameters, configuration.prefetch_count,
                                  self._consuming, self._subscribers)
        self._is_running = True
        self.__next_id_val = -1
        self.__next_id_lock = threading.Lock()

    def next_id(self):
        with self.__next_id_lock:
            self.__next_id_val += 1
            return self.__next_id_val

    def run(self):
        while self._is_running:
            try:
                self._consumer.run()
            finally:
                self._maybe_reconnect()

    def stop(self):
        self._is_running = False
        self._consumer.should_reconnect = False
        self._consumer.stop()

    def add_subscriber(self, queue, on_message_callback):
        if self._subscriber_name is None:
            self._subscriber_name = 'rabbit_mq_subscriber'
            logger.info(f"Using default subscriber name: '{self._subscriber_name}'")
        consumer_tag = f'{self._subscriber_name}.{self.next_id()}.{datetime.datetime.now()}'
        self._subscribers[consumer_tag] = (queue, on_message_callback)
        self._consuming[consumer_tag] = False
        self._consumer.start_consuming(consumer_tag)
        return consumer_tag

    def remove_subscriber(self, consumer_tag):
        self._consumer.stop_consuming(consumer_tag)
        self._subscribers.pop(consumer_tag)
        self._consuming.pop(consumer_tag)

    def add_callback_threadsafe(self, cb):
        self._consumer.add_callback_threadsafe(cb)

    def _maybe_reconnect(self):
        if self._consumer.should_reconnect:
            self._consumer.stop()
            reconnect_delay = self._get_reconnect_delay()
            logger.info('Reconnecting consumer after %d seconds', reconnect_delay)
            time.sleep(reconnect_delay)
            self._consumer = Consumer(self._connection_parameters, self._configuration.prefetch_count,
                                      self._consuming, self._subscribers)

    def _get_reconnect_delay(self):
        if self._consumer.was_consuming:
            self._reconnect_delay = 0
        else:
            self._reconnect_delay += 1
        if self._reconnect_delay > 30:
            self._reconnect_delay = 30
        return self._reconnect_delay
