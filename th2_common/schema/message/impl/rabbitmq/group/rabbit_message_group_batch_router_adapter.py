from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_router import \
    RabbitMessageGroupBatchRouter
from abc import ABC, abstractmethod

from th2_common.schema.message.message_listener import MessageListener
from th2_common.schema.message.subscriber_monitor import SubscriberMonitor


class RabbitMessageGroupBatchRouterAdapter(RabbitMessageGroupBatchRouter, ABC):

    def send(self, message, *queue_attr):
        super().send(self.to_group_batch(message), *queue_attr)

    def send_all(self, message, *queue_attr):
        super().send_all(self.to_group_batch(message), *queue_attr)

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        return super().subscribe(self.get_converter(callback), *queue_attr)

    def subscribe_all(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        return super().subscribe_all(self.get_converter(callback), *queue_attr)

    @staticmethod
    @abstractmethod
    def to_group_batch(message):
        pass

    @staticmethod
    @abstractmethod
    def from_group_batch(message):
        pass

    def get_converter(self, callback: MessageListener):
        old_handler = callback.handler
        def new_handler(attributes, message):
            message = self.from_group_batch(message)
            old_handler(attributes, message)
        return type('MessageListenerConverter', (callback.__class__, ), {'handler': new_handler})
