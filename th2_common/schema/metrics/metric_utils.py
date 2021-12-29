from th2_grpc_common.common_pb2 import MessageGroupBatch, RawMessageBatch, RawMessage, AnyMessage, MessageGroup, Message

from th2_common.schema.util.util import get_session_alias_and_direction_group, get_sequence
from th2_common.schema.metrics import common_metrics


def update_total_metrics(batch, message_counter, group_counter, sequence_gauge, th2_pin=''):
    b, former_type = to_group_batch(batch)
    if former_type != 'group':
        group_counter, sequence_gauge = None, None
    for group in b.groups:
        gr_labels = (th2_pin, )+get_session_alias_and_direction_group(group.messages[0])
        update_message_metrics(group.messages, message_counter, gr_labels)
        if group_counter:
            group_counter.labels(*gr_labels).inc()
        if sequence_gauge:
            sequence_gauge.labels(*gr_labels).set(get_sequence(group))


def update_message_metrics(messages, counter, labels):
    for msg in messages:
        if msg.HasField('raw_message'):
            counter.labels(*labels, common_metrics.TH2_MESSAGE_TYPES['raw']).inc()
        elif msg.HasField('message'):
            counter.labels(*labels, common_metrics.TH2_MESSAGE_TYPES['parsed']).inc()


def update_dropped_metrics(batch, message_counter, group_counter, th2_pin=''):
    b, former_type = to_group_batch(batch)
    if former_type != 'group':
        group_counter = None
    for group in b.groups:
        labels = (th2_pin, )+get_session_alias_and_direction_group(group.messages[0])
        update_message_metrics(group.messages, message_counter, labels)
        if group_counter:
            group_counter.labels(*labels).inc()


def to_group_batch(batch):
    if batch.HasField('groups'):
        return batch, 'group'
    elif isinstance(batch.messages[0], RawMessage):
        messages = [AnyMessage(raw_message=msg) for msg in batch.messages]
        group = MessageGroup(messages=messages)
        group_batch = MessageGroupBatch(groups=[group])
        return group_batch, 'raw'
    elif isinstance(batch.messages[0], Message):
        messages = [AnyMessage(message=msg) for msg in batch.messages]
        group = MessageGroup(messages=messages)
        group_batch = MessageGroupBatch(groups=[group])
        return group_batch, 'parsed'
