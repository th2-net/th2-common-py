#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


from th2common.schema.filter.strategy.impl.default_filter_strategy import DefaultFilterStrategy
from th2common.schema.message.configuration.queue_configuration import QueueConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_rabbit_queue import AbstractRabbitQueue
from th2common.schema.message.impl.rabbitmq.configuration.rabbitmq_configuration import RabbitMQConfiguration
from th2common.schema.message.impl.rabbitmq.configuration.subscribe_target import SubscribeTarget
from th2common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_sender import RabbitRawBatchSender
from th2common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_subscriber import RabbitRawBatchSubscriber
from th2common.schema.message.message_sender import MessageSender
from th2common.schema.message.message_subscriber import MessageSubscriber


class RabbitRawBatchQueue(AbstractRabbitQueue):

    def create_sender(self, configuration: RabbitMQConfiguration,
                      queue_configuration: QueueConfiguration) -> MessageSender:
        return RabbitRawBatchSender(configuration, queue_configuration.exchange, queue_configuration.name)

    def create_subscriber(self, configuration: RabbitMQConfiguration,
                          queue_configuration: QueueConfiguration) -> MessageSubscriber:
        subscribe_target = SubscribeTarget(routing_key=queue_configuration.name, queue=queue_configuration.queue)
        return RabbitRawBatchSubscriber(configuration,
                                        queue_configuration,
                                        DefaultFilterStrategy(),
                                        subscribe_target)
