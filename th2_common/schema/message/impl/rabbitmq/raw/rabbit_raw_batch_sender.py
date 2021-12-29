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
from prometheus_client import Counter
from th2_grpc_common.common_pb2 import RawMessageBatch, AnyMessage, MessageGroup, MessageGroupBatch

from th2_common.schema.message.impl.rabbitmq.abstract_rabbit_sender import AbstractRabbitSender
from th2_common.schema.message.impl.rabbitmq.group.rabbit_message_group_batch_sender import \
    RabbitMessageGroupBatchSender
from th2_common.schema.util.util import get_debug_string, get_session_alias_and_direction


class RabbitRawBatchSender(RabbitMessageGroupBatchSender):
    pass
