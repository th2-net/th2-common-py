from th2_grpc_common.common_pb2 import MessageGroupBatch, RawMessageBatch, RawMessage, AnyMessage, MessageGroup, Message

from th2_common.schema.util.util import get_session_alias_and_direction_group, get_sequence
from th2_common.schema.metrics import common_metrics


def update_total_metrics(batch, th2_pin, message_counter, group_counter, sequence_gauge):
    for group in batch.groups:
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


def update_dropped_metrics(batch, th2_pin, message_counter, group_counter):
    for group in batch.groups:
        labels = (th2_pin, )+get_session_alias_and_direction_group(group.messages[0])
        update_message_metrics(group.messages, message_counter, labels)
        if group_counter:
            group_counter.labels(*labels).inc()
