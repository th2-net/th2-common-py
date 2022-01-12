#   Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

from google.protobuf.json_format import MessageToJson
from prometheus_client import Counter, Histogram
from th2_grpc_common.common_pb2 import RawMessageBatch, RawMessage, MessageGroupBatch

import th2_common.schema.metrics.common_metrics as common_metrics
from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_batch_subscriber import AbstractRabbitBatchSubscriber, \
    Metadata
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_subscriber import \
    RabbitMessageGroupBatchSubscriber
from th2_common.schema.util.util import get_debug_string, get_session_alias_and_direction


class RabbitRawBatchSubscriber(RabbitMessageGroupBatchSubscriber):
    pass
