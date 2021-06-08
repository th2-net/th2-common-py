from datetime import datetime

from google.protobuf.timestamp_pb2 import Timestamp
from th2_grpc_common.common_pb2 import MessageGroupBatch, MessageGroup, AnyMessage, Message, EventID, Value, \
    MessageMetadata, MessageID, RawMessage, RawMessageMetadata

from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_sender import RabbitParsedBatchSender
from th2_common.schema.message.impl.rabbitmq.parsed.rabbit_parsed_batch_subscriber import RabbitParsedBatchSubscriber
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_sender import RabbitRawBatchSender
from th2_common.schema.message.impl.rabbitmq.raw.rabbit_raw_batch_subscriber import RabbitRawBatchSubscriber


class TestCheckParsingMessageGroupBatch:
    def test_parse_message_group_batch_to_message_batch_and_back(self):
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        test_message = Message(parent_event_id=EventID(id='1'),
                               metadata=MessageMetadata(
                                   id=MessageID(),
                                   timestamp=Timestamp(seconds=seconds, nanos=nanos),
                                   message_type='test_type',
                                   properties={'property1': 'property1_value',
                                               'property2': 'property2_value'},
                                   protocol='protocol_name'
                               ),
                               fields={'field1': Value(simple_value='value1'),
                                       'field2': Value(simple_value='value2')})
        messages = [AnyMessage(message=test_message), AnyMessage(message=test_message)]
        groups = [MessageGroup(messages=messages)]
        group_batch = MessageGroupBatch(groups=groups)

        original_bytes = group_batch.SerializeToString()
        value_from_bytes = RabbitParsedBatchSubscriber.value_from_bytes(original_bytes)
        bytes_from_value = RabbitParsedBatchSender.value_to_bytes(value_from_bytes[0])

        assert original_bytes == bytes_from_value

    def test_parse_message_group_batch_to_raw_message_batch_and_back(self):
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        test_message = RawMessage(metadata=RawMessageMetadata(id=MessageID(),
                                                              timestamp=Timestamp(seconds=seconds, nanos=nanos),
                                                              properties={'property1': 'property1_value',
                                                                          'property2': 'property2_value'},
                                                              protocol='protocol_name'
                                                              ),
                                  body=str.encode('12345'))
        messages = [AnyMessage(raw_message=test_message), AnyMessage(raw_message=test_message)]
        groups = [MessageGroup(messages=messages)]
        group_batch = MessageGroupBatch(groups=groups)

        original_bytes = group_batch.SerializeToString()
        value_from_bytes = RabbitRawBatchSubscriber.value_from_bytes(original_bytes)
        bytes_from_value = RabbitRawBatchSender.value_to_bytes(value_from_bytes[0])

        assert original_bytes == bytes_from_value
