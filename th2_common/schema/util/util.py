#   Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

from typing import List, Tuple

from th2_grpc_common.common_pb2 import MessageID, MessageGroupBatch, AnyMessage, EventBatch, Direction, Value, \
    ListValue, Message, MessageMetadata, ConnectionID, MetadataFilter, RootMessageFilter, MessageFilter, \
    ValueFilter, ListValueFilter, SimpleList

import th2_common.schema.metrics.common_metrics as common_metrics


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
    sequences = ''.join([str(i.sequence) for i in ids])
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
    sequences = ''.join(sequences)

    return f'MessageGroupBatch: ' \
           f'session_alias = {session_alias}, ' \
           f'direction = {direction}, ' \
           f'sequences = {sequences}'.strip()


def convert_message_value(value, message_type=None, session_alias=None):
    if isinstance(value, Value):
        return value
    elif isinstance(value, (str, int, float)):
        return Value(simple_value=str(value))
    elif isinstance(value, list):
        return Value(list_value=ListValue(values=[convert_message_value(x) for x in value]))
    elif isinstance(value, dict):
        return Value(message_value=Message(metadata=MessageMetadata(id=MessageID(
                                                                        connection_id=ConnectionID(
                                                                            session_alias=session_alias)),
                                                                    message_type=message_type),
                                           fields={key: convert_message_value(value[key]) for key in value
                                                   if key not in ['message_type', 'session_alias']}))


def create_message(fields: dict, session_alias=None, message_type=None):
    return Message(metadata=MessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias=session_alias)),
                                            message_type=message_type),
                   fields={field: convert_message_value(fields[field]) for field in fields})


def convert_root_message_filter_value(filt, message_type=None, direction=None, fields=False, property_filters=False):
    if isinstance(filt, ValueFilter):
        return filt
    elif isinstance(filt, (str, int, float)) and fields is True:
        return ValueFilter(simple_filter=str(filt))
    elif isinstance(filt, (str, int, float)) and property_filters is True:
        return MetadataFilter.SimpleFilter(value=str(filt))
    elif isinstance(filt, list) and fields is True:
        return ValueFilter(
                    list_filter=ListValueFilter(
                        values=[convert_root_message_filter_value(x,
                                                                  fields=fields,
                                                                  property_filters=property_filters)
                                for x in filt]))
    elif isinstance(filt, list) and property_filters is True:
        return MetadataFilter.SimpleFilter(simple_list=SimpleList(simple_values=filt))
    elif isinstance(filt, dict):
        return ValueFilter(
                    message_filter=MessageFilter(messageType=message_type,
                                                 fields={key: convert_root_message_filter_value(
                                                                                    filt[key],
                                                                                    fields=fields,
                                                                                    property_filters=property_filters)
                                                         for key in filt},
                                                 direction=direction))


def create_root_message_filter(message_filter: dict, metadata_filter: dict, message_type=None):
    return RootMessageFilter(messageType=message_type,
                             message_filter=MessageFilter(messageType=message_type, fields={
                                 field: convert_root_message_filter_value(message_filter[field], fields=True)
                                 for field in message_filter}),
                             metadata_filter=MetadataFilter(property_filters={
                                 filtr: convert_root_message_filter_value(metadata_filter[filtr], property_filters=True)
                                 for filtr in metadata_filter})
                             )
