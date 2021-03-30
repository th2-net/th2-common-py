import functools
import logging
import threading
import time
from typing import Optional

import pika
from pika import SelectConnection
from pika.channel import Channel

logger = logging.getLogger()


class ReconnectingPublisher(object):
    TIMEOUT_WAIT_CONFIRM_DELIVERY_BY_CLOSE = 10
    TIMEOUT_STOPPING = 5

    def __init__(self, connection_parameters: pika.ConnectionParameters):
        self._connection_parameters = connection_parameters

        self._connection: Optional[SelectConnection] = None
        self._channel: Optional[Channel] = None

        self._deliveries = []
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
        if not self._stopping and not self._connection.is_closed and not self._connection.is_closing:
            self._connection.close()

    def on_delivery_confirmation(self, method_frame):
        with self._message_lock:
            confirmation_type = method_frame.method.NAME.split('.')[1].lower()
            logger.info('Received %s for delivery tag: %i multi: %s', confirmation_type,
                        method_frame.method.delivery_tag, method_frame.method.multiple)
            cnt = 0
            if method_frame.method.multiple:
                idx = len(self._deliveries) - 1
                while self._deliveries[idx] != method_frame.method.delivery_tag:
                    idx -= 1
                while idx >= 0:
                    self._deliveries.pop(0)
                    cnt += 1
                    idx -= 1
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
        cb = functools.partial(self._basic_publish, exchange_name, routing_key, message)
        while self._connection is None or not self._connection.is_open:
            logger.warning('Cannot send a message because the connection. Try in 1 sec.')
            time.sleep(1)
        self._connection.ioloop.call_later(1, cb)

    def _basic_publish(self, exchange_name, routing_key, message):
        if self._channel is None or not self._channel.is_open:
            logger.warning('Cannot send a message because the connection or channel is closed. Try in 5 sec.')
            cb = functools.partial(self._basic_publish, exchange_name, routing_key, message)
            self._connection.ioloop.call_later(5, cb)
        with self._message_lock:
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
        self._stopping = True
        logger.info(f'Waiting for the remaining messages to be published for'
                    f' {ReconnectingPublisher.TIMEOUT_STOPPING} sec.')
        time.sleep(ReconnectingPublisher.TIMEOUT_STOPPING)
        deliveries_size = len(self._deliveries)
        wait_counter = 0
        while deliveries_size > 0 and wait_counter <= ReconnectingPublisher.TIMEOUT_WAIT_CONFIRM_DELIVERY_BY_CLOSE:
            logger.info(f'Deliveries size != 0, size={deliveries_size}. Wait confirm delivery '
                        f'{ReconnectingPublisher.TIMEOUT_WAIT_CONFIRM_DELIVERY_BY_CLOSE - wait_counter} sec.')
            time.sleep(1)
            wait_counter += 1
            deliveries_size = len(self._deliveries)
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
