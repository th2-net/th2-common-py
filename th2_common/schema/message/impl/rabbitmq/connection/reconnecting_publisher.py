import logging
import threading
import time
from typing import Optional

import pika
from pika import SelectConnection
from pika.channel import Channel

logger = logging.getLogger()


class ReconnectingPublisher(object):

    def __init__(self, connection_parameters: pika.ConnectionParameters):
        self._connection_parameters = connection_parameters

        self._connection: Optional[SelectConnection] = None
        self._channel: Optional[Channel] = None

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None
        self._message_lock = threading.Lock()

        self._stopping = False

    def connect(self):
        logger.info('Connecting by Publisher')
        return pika.SelectConnection(
            self._connection_parameters,
            on_open_callback=self.on_connection_open,
            on_open_error_callback=self.on_connection_open_error,
            on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        logger.info('Publisher connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        logger.error('Publisher connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            logger.warning('Publisher connection closed, reopening in 5 seconds: %s', reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        logger.info('Creating a channel for Publisher')
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        logger.info('Publisher channel opened')
        self._channel = channel
        self._channel.confirm_delivery(self.on_delivery_confirmation)
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        logger.warning('Publisher channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def on_delivery_confirmation(self, method_frame):
        with self._message_lock:
            confirmation_type = method_frame.method.NAME.split('.')[1].lower()
            logger.info('Received %s for delivery tag: %i multi: %s', confirmation_type,
                        method_frame.method.delivery_tag, method_frame.method.multiple)
            cnt = 0
            if method_frame.method.multiple:
                while len(self._deliveries) > 0 and self._deliveries[0] <= method_frame.method.delivery_tag:
                    cnt += 1
                    self._deliveries.pop(0)
            else:
                cnt += 1
                self._deliveries.remove(method_frame.method.delivery_tag)
            if confirmation_type == 'ack':
                self._acked += cnt
            elif confirmation_type == 'nack':
                self._nacked += cnt

            logger.info(
                'Published %i messages, %i have yet to be confirmed, '
                '%i were acked and %i were nacked', self._message_number,
                len(self._deliveries), self._acked, self._nacked)

    def publish_message(self, exchange_name, routing_key, message):
        with self._message_lock:
            while self._channel is None or not self._channel.is_open:
                time.sleep(5)

            self._channel.basic_publish(exchange=exchange_name,
                                        routing_key=routing_key,
                                        body=message)

            self._message_number += 1
            self._deliveries.append(self._message_number)
            logger.info('Published message # %i', self._message_number)

    def run(self):
        while not self._stopping:
            self._connection = None
            with self._message_lock:
                self._deliveries = []
                self._acked = 0
                self._nacked = 0
                self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except Exception:
                logger.info("Error while running Publisher")

        self.stop()
        if self._connection is not None and not self._connection.is_closed:
            self._connection.ioloop.start()
        logger.info('Publisher stopped')

    def stop(self):
        logger.info('Publisher stopping')
        deliveries_size = len(self._deliveries)
        while deliveries_size > 0:
            logger.info(f'Deliveries size != 0, size={deliveries_size}. Wait confirm delivery.')
            time.sleep(1)
            deliveries_size = len(self._deliveries)
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        if self._channel is not None and not self._channel.is_closing and not self._channel.is_closed:
            logger.info('Closing Publisher channel')
            self._channel.close()

    def close_connection(self):
        if self._connection is not None and not self._connection.is_closing and not self._connection.is_closed:
            logger.info('Closing Publisher connection')
            self._connection.close()
