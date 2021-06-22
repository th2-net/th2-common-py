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
from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration


class MessageRouterConfiguration(AbstractConfiguration):
    def __init__(self, queues: dict, **kwargs) -> None:
        self.queues = {queue_alias: QueueConfiguration(**queues[queue_alias]) for queue_alias in queues.keys()}
        self.check_unexpected_args(kwargs)

    def get_queue_by_alias(self, queue_alias):
        return self.queues[queue_alias]

    def find_queues_by_attr(self, attrs) -> {str: QueueConfiguration}:
        result = dict()
        for queue_alias in self.queues.keys():
            if all(attr in self.queues[queue_alias].attributes for attr in attrs):
                result[queue_alias] = self.queues[queue_alias]
        return result
