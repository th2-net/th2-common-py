#   Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

from typing import List, Optional, Tuple

from th2_grpc_common.common_pb2 import AnyMessage, Direction, EventBatch, MessageGroup, MessageGroupBatch, MessageID

from th2_common.schema.message.configuration.message_configuration import MessageRouterConfiguration, \
    MqRouterFilterConfiguration
import th2_common.schema.metrics.common_metrics as common_metrics


def get_filters(configuration: MessageRouterConfiguration,
                aliases: List[str]) -> List[List[MqRouterFilterConfiguration]]:
    return [configuration.queues[alias].filters for alias in aliases]


def get_sequence(group: MessageGroup) -> Optional[int]:
    if group.messages[0].HasField('raw_message'):
        return group.messages[0].raw_message.metadata.id.sequence
    elif group.messages[0].HasField('message'):
        return group.messages[0].message.metadata.id.sequence

    return None


def get_session_alias_and_direction(message_id: MessageID) -> Tuple[str, str]:
    return message_id.connection_id.session_alias, Direction.Name(message_id.direction)


def get_session_alias_and_direction_group(any_message: AnyMessage) -> Tuple[str, str]:
    if any_message.HasField('message'):
        return get_session_alias_and_direction(any_message.message.metadata.id)
    elif any_message.HasField('raw_message'):
        return get_session_alias_and_direction(any_message.raw_message.metadata.id)
    else:
        return common_metrics.UNKNOWN_LABELS


def get_debug_string(class_name: str, ids: List[MessageID]) -> str:
    session_alias, direction = get_session_alias_and_direction(ids[0])
    sequences = ', '.join([str(i.sequence) for i in ids])
    return f'{class_name}: session_alias = {session_alias}, direction = {direction}, sequences = {sequences}'.strip()


def get_debug_string_event(event_batch: EventBatch) -> str:
    return f'EventBatch: {event_batch.events[0].id}'.strip()


def get_debug_string_group(group_batch: MessageGroupBatch) -> str:
    messages = [message for group in group_batch.groups for message in group.messages]
    session_alias, direction = get_session_alias_and_direction_group(messages[0])
    sequences = []

    for message in messages:
        if message.HasField('message'):
            sequences.append(str(message.message.metadata.id.sequence))
        elif message.HasField('raw_message'):
            sequences.append(str(message.raw_message.metadata.id.sequence))
    sequences_string = ''.join(sequences)

    return f'MessageGroupBatch: ' \
           f'session_alias = {session_alias}, ' \
           f'direction = {direction}, ' \
           f'sequences_string = {sequences_string}'.strip()
