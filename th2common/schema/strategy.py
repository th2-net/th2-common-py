#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
# # Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import ABC, abstractmethod
from threading import Lock

from google.protobuf.message import Message

from th2common.gen import infra_pb2
from th2common.schema.grpc.configurations import GrpcRawRobinStrategy


class FieldExtractionStrategy(ABC):

    @abstractmethod
    def get_fields(self, message: Message) -> {str: str}:
        pass


class AbstractTh2MsgFieldExtraction(FieldExtractionStrategy, ABC):
    SESSION_ALIAS_KEY = "session_alias"
    MESSAGE_TYPE_KEY = "message_type"
    DIRECTION_KEY = "direction"

    def get_fields(self, message: Message) -> {str: str}:
        pass

    @abstractmethod
    def parse_message(self, message: Message) -> infra_pb2.Message:
        pass


class Th2BatchMsgFieldExtraction(AbstractTh2MsgFieldExtraction):

    def parse_message(self, message: Message) -> infra_pb2.Message:
        return message


class RobinStrategy:
    index = 0
    lock = Lock()

    def __init__(self, configuration) -> None:
        self.endpoints = GrpcRawRobinStrategy(**configuration).endpoints

    def get_endpoint(self, request):
        try:
            self.lock.acquire()
            if self.index >= len(self.endpoints):
                self.index = 0
            result = self.endpoints[self.index]
            self.index = self.index + 1
            return result
        finally:
            self.lock.release()
