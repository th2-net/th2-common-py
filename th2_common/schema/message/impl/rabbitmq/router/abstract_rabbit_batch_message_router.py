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
import copy
from abc import ABC, abstractmethod

from google.protobuf.message import Message
from th2_grpc_common.common_pb2 import MessageGroupBatch

from th2_common.schema.message.configuration.message_configuration import QueueConfiguration
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_message_router import AbstractRabbitMessageRouter
from th2_common.schema.metrics.metric_utils import to_group_batch


class AbstractRabbitBatchMessageRouter(AbstractRabbitMessageRouter, ABC):

    def _find_by_filter(self, queues: {str: QueueConfiguration}, batch) -> dict:
        result = dict()
        all_aliases = set(queues.keys)
        for message in self._get_messages(batch):
            sent_to = set()
            for queue_alias in self._filter(queues, message):
                self._add_message(result.setdefault(queue_alias, self._create_batch()), message)
                sent_to.add(queue_alias)
            self.update_dropped_metrics(MessageGroupBatch(groups=[message]), all_aliases - sent_to)
        return result

    def _filter(self, queues: {str: QueueConfiguration}, message: Message) -> {str}:
        aliases = set()
        for queue_alias in queues.keys():
            filters = queues[queue_alias].filters

            if len(filters) == 0 or self._filter_strategy.verify(message, router_filters=filters):
                aliases.add(queue_alias)
        return aliases

    @abstractmethod
    def _get_messages(self, batch) -> list:
        pass

    @abstractmethod
    def _create_batch(self):
        pass

    @abstractmethod
    def _add_message(self, batch, message):
        pass

    @abstractmethod
    def update_dropped_metrics(self, batch, pin):
        pass
