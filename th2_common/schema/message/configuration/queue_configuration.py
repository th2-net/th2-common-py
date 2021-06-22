#   Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

from th2_common.schema.configuration.abstract_configuration import AbstractConfiguration
from th2_common.schema.message.configuration.mq_router_filter_configuration import MqRouterFilterConfiguration


class QueueConfiguration(AbstractConfiguration):

    def __init__(self, name: str, queue: str, exchange: str, attributes: list, filters: list, can_read=True,
                 can_write=True, **kwargs) -> None:
        self.routing_key = name
        self.queue = queue
        self.exchange = exchange
        self.attributes = attributes
        self.filters = [MqRouterFilterConfiguration(**filter_schema) for filter_schema in filters]
        self.can_read = can_read
        self.can_write = can_write
        self.check_unexpected_args(kwargs)
