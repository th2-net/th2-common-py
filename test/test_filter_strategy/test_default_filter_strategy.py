#   Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

from test.resources.filter_strategy.default_filter_strategy_test import filtered_by_queue, message_group_batch
from test.resources.test_classes import rabbit_message_router, test_common_factory
from test.resources.test_util import object_to_dict

filtered_messages = rabbit_message_router.split_and_filter(
    queue_aliases_to_configs=test_common_factory.message_router_configuration.queues,  # type: ignore
    batch=message_group_batch
)


def test_default_filter_strategy_and_filter() -> None:
    queue = 'queue_AND_filter'
    assert object_to_dict(filtered_messages)[queue] == object_to_dict(filtered_by_queue)[queue]


def test_default_filter_strategy_or_filter() -> None:
    queue = 'queue_OR_filter'
    assert object_to_dict(filtered_messages)[queue] == object_to_dict(filtered_by_queue)[queue]


def test_default_filter_strategy_or_and_filter() -> None:
    queue = 'queue_OR_AND_filter'
    assert object_to_dict(filtered_messages)[queue] == object_to_dict(filtered_by_queue)[queue]


def test_default_filter_strategy_empty_filter() -> None:
    queue = 'queue_EMPTY_filter'
    assert object_to_dict(filtered_messages)[queue] == object_to_dict(filtered_by_queue)[queue]
