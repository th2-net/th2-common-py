#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from abc import ABC, abstractmethod

from th2_common.schema.message.message_listener import MessageListener


class MessageSubscriber(ABC):

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def is_close(self) -> bool:
        pass

    @abstractmethod
    def add_listener(self, message_listener: MessageListener) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass
