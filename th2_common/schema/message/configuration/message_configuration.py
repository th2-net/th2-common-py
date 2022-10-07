#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from enum import Enum
from typing import Any, Dict, List, Optional, Set

from th2_common.schema.configuration.abstract_configuration import AbstractConfiguration


class FieldFilterOperation(Enum):
    EQUAL = 'EQUAL'
    NOT_EQUAL = 'NOT_EQUAL'
    EMPTY = 'EMPTY'
    NOT_EMPTY = 'NOT_EMPTY'
    WILDCARD = 'WILDCARD'
    UNKNOWN = 'UNKNOWN'

    @classmethod
    def _missing_(cls, value: object) -> Any:
        return FieldFilterOperation.UNKNOWN


class MessageRouterConfiguration(AbstractConfiguration):

    def __init__(self, queues: Dict[str, Any], **kwargs: Any) -> None:
        self.queues = {queue_alias: QueueConfiguration(**queues[queue_alias]) for queue_alias in queues}

        self.check_unexpected_args(kwargs)

    def get_queue_by_alias(self, queue_alias: str) -> 'QueueConfiguration':
        return self.queues[queue_alias]

    def find_queues_by_attr(self, attrs: Set[str]) -> Dict[str, 'QueueConfiguration']:
        result = {}
        for queue_alias in self.queues:
            if all(attr in self.queues[queue_alias].attributes for attr in attrs):
                result[queue_alias] = self.queues[queue_alias]
        return result


class MqConnectionConfiguration(AbstractConfiguration):

    def __init__(self,
                 subscriberName: Optional[str] = None,
                 connectionTimeout: int = -1,
                 connectionCloseTimeout: int = 10000,
                 maxRecoveryAttempts: int = 5,
                 minConnectionRecoveryTimeout: int = 10000,
                 maxConnectionRecoveryTimeout: int = 60000,
                 prefetchCount: int = 10,
                 messageRecursionLimit: int = 100,
                 maxMessages: int = 10,
                 checkInterval: int = 2,
                 **kwargs: Any) -> None:
        self.subscriber_name = subscriberName
        self.connection_timeout = int(connectionTimeout)
        self.connection_close_timeout = int(connectionCloseTimeout)
        self.max_recovery_attempts = int(maxRecoveryAttempts)
        self.min_connection_recovery_timeout = int(minConnectionRecoveryTimeout)
        self.max_connection_recovery_timeout = int(maxConnectionRecoveryTimeout)
        self.prefetch_count = int(prefetchCount)
        self.message_recursion_limit = int(messageRecursionLimit)

        self.check_unexpected_args(kwargs)

        self.max_messages = int(maxMessages)
        self.check_interval = int(checkInterval)


class QueueConfiguration(AbstractConfiguration):

    def __init__(self,
                 name: str,
                 queue: str,
                 exchange: str,
                 attributes: List[str],
                 filters: List[Dict[str, Any]],
                 can_read: bool = True,
                 can_write: bool = True,
                 **kwargs: Any) -> None:
        self.routing_key = name
        self.queue = queue
        self.exchange = exchange
        self.attributes = attributes
        self.filters = [MqRouterFilterConfiguration(**filter_schema) for filter_schema in filters]
        self.can_read = can_read
        self.can_write = can_write

        self.check_unexpected_args(kwargs)


class MqRouterFilterConfiguration(AbstractConfiguration):

    def __init__(self,
                 metadata: Optional[Dict[str, Any]] = None,
                 message: Optional[Dict[str, Any]] = None,
                 **kwargs: Any) -> None:
        self.metadata = []
        self.message = []

        if isinstance(metadata, dict):
            self.metadata = [FieldFilterConfiguration(**metadata[key], fieldName=key) for key in metadata]
        elif isinstance(metadata, list):
            self.metadata = [FieldFilterConfiguration(**key) for key in metadata]

        if isinstance(message, dict):
            self.message = [FieldFilterConfiguration(**message[key], fieldName=key) for key in message]
        elif isinstance(message, list):
            self.message = [FieldFilterConfiguration(**key) for key in message]

        self.check_unexpected_args(kwargs)

    def __str__(self) -> str:
        return str(self.metadata + self.message)


class FieldFilterConfiguration(AbstractConfiguration):

    def __init__(self,
                 fieldName: str,
                 operation: str,
                 value: Optional[str] = None,
                 expectedValue: Optional[str] = None,
                 **kwargs: Any) -> None:
        self.value = value or expectedValue
        self.field_name = fieldName
        self.operation = FieldFilterOperation(operation)

        self.check_unexpected_args(kwargs)

    def __str__(self) -> str:
        return f'Value: {self.value} Field name: {self.field_name} Operation: {self.operation}'
