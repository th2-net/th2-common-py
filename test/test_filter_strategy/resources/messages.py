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


cl_ord_id = random.randint(10 ** 6, (10 ** 7) - 1)
transact_time = datetime.now().isoformat()
parent_event_id = EventID()
ts = Timestamp()

# AnyMessage.message

trading_party_message = Value(message_value=Message(fields={
    'NoPartyIDs': Value(list_value=ListValue(values=[
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='DEMO-CONN1'),
            'PartyIDSource': Value(simple_value='D'),
            'PartyRole': Value(simple_value='76')
        })),
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='0'),
            'PartyIDSource': Value(simple_value='P'),
            'PartyRole': Value(simple_value='3')
        })),
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='0'),
            'PartyIDSource': Value(simple_value='P'),
            'PartyRole': Value(simple_value='122')
        })),
        Value(message_value=Message(fields={
            'PartyID': Value(simple_value='3'),
            'PartyIDSource': Value(simple_value='P'),
            'PartyRole': Value(simple_value='12')
        }))
    ]))
}))

message = Message(parent_event_id=parent_event_id,
                  metadata=MessageMetadata(
                      message_type='NewOrderSingle',
                      id=MessageID(
                          connection_id=ConnectionID(session_alias='arfq02fix10'),
                          direction=Direction.SECOND
                      )
                  ),
                  fields={
                      'SecurityID': Value(simple_value='5221001'),
                      'SecurityIDSource': Value(simple_value='8'),
                      'OrdType': Value(simple_value='2'),
                      'AccountType': Value(simple_value='1'),
                      'OrderCapacity': Value(simple_value='A'),
                      'OrderQty': Value(simple_value='30'),
                      'DisplayQty': Value(simple_value='30'),
                      'Price': Value(simple_value='55'),
                      'ClOrdID': Value(simple_value=str(cl_ord_id)),
                      'SecondaryClOrdID': Value(simple_value='11111'),
                      'Side': Value(simple_value='1'),
                      'TimeInForce': Value(simple_value='0'),
                      'TransactTime': Value(simple_value=transact_time),
                      'TradingParty': trading_party_message
                  })

any_message = AnyMessage(message=message)

trading_party_dict = {
    'NoPartyIDs': [
        {
            'PartyID': 'DEMO-CONN1',
            'PartyIDSource': 'D',
            'PartyRole': '76'
        },
        {
            'PartyID': '0',
            'PartyIDSource': 'P',
            'PartyRole': '3'
        },
        {
            'PartyID': '0',
            'PartyIDSource': 'P',
            'PartyRole': '122'
        },
        {
            'PartyID': '3',
            'PartyIDSource': 'P',
            'PartyRole': '12'
        }
    ]
}

message_dict = {
    'AccountType': '1',
    'ClOrdID': str(cl_ord_id),
    'DisplayQty': '30',
    'OrdType': '2',
    'OrderCapacity': 'A',
    'OrderQty': '30',
    'Price': '55',
    'SecondaryClOrdID': '11111',
    'SecurityID': '5221001',
    'SecurityIDSource': '8',
    'Side': '1',
    'TimeInForce': '0',
    'TradingParty': trading_party_dict,
    'TransactTime': transact_time,
    'session_alias': 'arfq02fix10',
    'message_type': 'NewOrderSingle',
    'direction': 'SECOND'
}


#  AnyMessage.raw_message

raw_message = RawMessage(parent_event_id=parent_event_id,
                         metadata=RawMessageMetadata(
                             id=MessageID(
                                 connection_id=ConnectionID(session_alias='arfq02fix10'),
                                 direction=Direction.SECOND
                             ),
                             timestamp=ts,
                             properties={'field1': 'value1'},
                             protocol='_protocol'
                         ),
                         body=os.urandom(144))

any_message_raw = AnyMessage(raw_message=raw_message)

raw_message_dict = {
    'session_alias': 'arfq02fix10',
    'direction': 'SECOND',
    'protocol': '_protocol'
}
