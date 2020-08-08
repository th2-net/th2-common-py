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
# # Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from th2common.schema.message.configurations import MessageRouterConfiguration
from th2common.schema.message.impl.rabbitmq.abstract_router import AbstractRabbitBatchMessageRouter
from th2common.schema.message.impl.rabbitmq.configuration import RabbitMQConfiguration
from th2common.schema.message.interfaces import MessageListener, SubscriberMonitor


class RabbitRawBatchRouter(AbstractRabbitBatchMessageRouter):

    def __init__(self, rabbit_mq_configuration: RabbitMQConfiguration,
                 configuration: MessageRouterConfiguration) -> None:
        self.rabbit_mq_configuration = rabbit_mq_configuration
        self.configuration = configuration

    def init(self, rabbit_mq_configuration: RabbitMQConfiguration, configuration: MessageRouterConfiguration):
        pass

    def subscribe(self, callback: MessageListener, *queue_attr) -> SubscriberMonitor:
        pass

    def unsubscribe_all(self):
        pass
