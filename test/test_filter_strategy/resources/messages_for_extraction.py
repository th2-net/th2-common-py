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

from datetime import datetime
import os
import random

from google.protobuf.timestamp_pb2 import Timestamp
from th2_grpc_common.common_pb2 import AnyMessage, ConnectionID, Direction, EventID, ListValue, Message, \
    MessageID, MessageMetadata, RawMessage, RawMessageMetadata, Value

cl_ord_id = random.randint(10 ** 5, (10 ** 6) - 1)
transact_time = datetime.now().isoformat()
parent_event_id = EventID()
ts = Timestamp()

# AnyMessage.message

trading_party_message = Value(message_value=Message(fields={
    'NoPartyIDs': Value(list_value=ListValue(values=[
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='0'),
            'PartyIDSource': Value(simple_value='A'),
            'PartyRole': Value(simple_value='1')
        })),
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='0'),
            'PartyIDSource': Value(simple_value='A'),
            'PartyRole': Value(simple_value='2')
        }))
    ]))
}))

message = Message(parent_event_id=parent_event_id,
                  metadata=MessageMetadata(
                      message_type='NewOrderSingle',
                      id=MessageID(
                          connection_id=ConnectionID(session_alias='jsTUURfy'),
                          direction=Direction.SECOND
                      )
                  ),
                  fields={
                      'OrdType': Value(simple_value='1'),
                      'AccountType': Value(simple_value='2'),
                      'OrderQty': Value(simple_value='3'),
                      'Price': Value(simple_value='4'),
                      'ClOrdID': Value(simple_value=str(cl_ord_id)),
                      'TransactTime': Value(simple_value=transact_time),
                      'TradingParty': trading_party_message
                  })

any_message = AnyMessage(message=message)

trading_party_dict = {
    'NoPartyIDs': [
        {
            'PartyID': '0',
            'PartyIDSource': 'A',
            'PartyRole': '1'
        },
        {
            'PartyID': '0',
            'PartyIDSource': 'A',
            'PartyRole': '2'
        }
    ]
}

message_dict = {
    'OrdType': '1',
    'AccountType': '2',
    'OrderQty': '3',
    'Price': '4',
    'ClOrdID': str(cl_ord_id),
    'TransactTime': transact_time,
    'TradingParty': trading_party_dict,
    'message_type': 'NewOrderSingle',
    'book_name': '',
    'direction': 'SECOND',
    'properties': {},
    'protocol': '',
    'sequence': 0,
    'session_alias': 'jsTUURfy',
    'session_group': '',
    'subsequence': [],
    'timestamp': None
}

#  AnyMessage.raw_message

raw_message = RawMessage(parent_event_id=parent_event_id,
                         metadata=RawMessageMetadata(
                             id=MessageID(
                                 connection_id=ConnectionID(session_alias='jsTUURfy'),
                                 timestamp=ts,
                                 direction=Direction.SECOND
                             ),
                             properties={'field1': 'value1'},
                             protocol='_protocol'
                         ),
                         body=os.urandom(144))

any_message_raw = AnyMessage(raw_message=raw_message)

raw_message_dict = {
    'session_alias': 'jsTUURfy',
    'direction': 'SECOND',
    'protocol': '_protocol'
}
