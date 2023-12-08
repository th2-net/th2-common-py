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

from th2_grpc_common.common_pb2 import AnyMessage, ConnectionID, Direction, Message, MessageGroup, \
    MessageGroupBatch, MessageID, MessageMetadata, RawMessage, RawMessageMetadata, Value


message_group1 = MessageGroup(messages=[  # goes to queue_AND_filter, queue_OR_filter
    AnyMessage(  # to QUEUE1 (AND filter)
        message=Message(metadata=MessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='qwerty'))),
                        fields={'msg11': Value(simple_value='11')})),
    AnyMessage(  # to QUEUE2 (OR filter)
        message=Message(fields={'msg21': Value(simple_value='21')})),
    AnyMessage(  # NOT to QUEUE3 (OR-AND filter)
        message=Message(fields={
            'msg31': Value(simple_value='31'),
            'msg32': Value(simple_value='thirty-one')
        }))
]
)

message_group2 = MessageGroup(messages=[  # goes to queue_OR_filter, queue_OR_AND_filter
    AnyMessage(  # to QUEUE2 (OR filter)
        message=Message(fields={'msg21': Value(simple_value='21')})),
    AnyMessage(  # NOT to QUEUE1 (AND filter)
        message=Message(metadata=MessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='qwerty'))),
                        fields={'msg11': Value(simple_value='eleven')})),
    AnyMessage(  # to QUEUE3 (OR-AND filter)
        message=Message(fields={
            'msg31': Value(simple_value='31'),
            'msg32': Value(simple_value='32')
        }))
])

message_group3 = MessageGroup(messages=[  # goes to queue_AND_filter, queue_OR_filter, queue_OR_AND_filter
    AnyMessage(  # to QUEUE1 (AND filter)
        message=Message(metadata=MessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='qwerty'))),
                        fields={'msg11': Value(simple_value='11')})),
    AnyMessage(  # to QUEUE2 (OR filter)
        message=Message(fields={'msg22': Value(simple_value='22')})),
    AnyMessage(  # to QUEUE3 (OR-AND filter)
        message=Message(metadata=MessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='asdfgh'),
                                                              direction=Direction.SECOND))))
])

message_group4 = MessageGroup(messages=[  # goes to queue_OR_AND_filter
    AnyMessage(  # to QUEUE3 (OR-AND filter)
        raw_message=RawMessage(
            metadata=RawMessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='asdfgh'),
                                                     direction=Direction.SECOND)))),
    AnyMessage(  # NOT to QUEUE3 (OR-AND filter)
        raw_message=RawMessage(
            metadata=RawMessageMetadata(id=MessageID(connection_id=ConnectionID(session_alias='asdfgh'),
                                                     direction=Direction.FIRST))))
])

message_group5 = MessageGroup(messages=[  # goes to queue_EMPTY_filter
    AnyMessage(  # to QUEUE4 (EMPTY filter)
        raw_message=RawMessage()),
    AnyMessage(  # to QUEUE4 (EMPTY filter)
        message=Message())
])

message_group_batch = MessageGroupBatch(groups=[
    message_group1,
    message_group2,
    message_group3,
    message_group4,
    message_group5
])

filtered_by_queue = {
    'queue_AND_filter': MessageGroupBatch(
        groups=[message_group1, message_group3]),
    'queue_OR_filter': MessageGroupBatch(
        groups=[message_group1, message_group2, message_group3]),
    'queue_OR_AND_filter': MessageGroupBatch(
        groups=[message_group2, message_group3, message_group4]),
    'queue_EMPTY_filter': MessageGroupBatch(
        groups=[message_group1, message_group2, message_group3, message_group4, message_group5])
}
