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

from google.protobuf.internal.containers import RepeatedCompositeFieldContainer
from prometheus_client import Counter, Gauge
from th2_grpc_common.common_pb2 import MessageGroupBatch

from th2_common.schema.metrics import common_metrics
from th2_common.schema.util.util import get_sequence, get_session_alias_and_direction_group


def update_total_metrics(batch: MessageGroupBatch,
                         th2_pin: str,
                         message_counter: Counter,
                         group_counter: Counter,
                         sequence_gauge: Gauge) -> None:
    for group in batch.groups:
        gr_labels = (th2_pin, ) + get_session_alias_and_direction_group(group.messages[0])
        update_message_metrics(group.messages, message_counter, *gr_labels)
        if group_counter:
            group_counter.labels(*gr_labels).inc()
        if sequence_gauge:
            sequence_gauge.labels(*gr_labels).set(get_sequence(group))


def update_message_metrics(messages: RepeatedCompositeFieldContainer, counter: Counter, *labels: str) -> None:
    for msg in messages:
        if msg.HasField('raw_message'):
            counter.labels(*labels, common_metrics.TH2_MESSAGE_TYPES['raw']).inc()
        elif msg.HasField('message'):
            counter.labels(*labels, common_metrics.TH2_MESSAGE_TYPES['parsed']).inc()


def update_dropped_metrics(batch: MessageGroupBatch,
                           th2_pin: str,
                           message_counter: Counter,
                           group_counter: Counter) -> None:
    for group in batch.groups:
        labels = (th2_pin, ) + get_session_alias_and_direction_group(group.messages[0])
        update_message_metrics(group.messages, message_counter, *labels)
        if group_counter:
            group_counter.labels(*labels).inc()
