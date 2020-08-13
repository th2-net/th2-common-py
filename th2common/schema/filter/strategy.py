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

from abc import ABC, abstractmethod
from typing import List

from google.protobuf.message import Message

from th2common.schema.message.configurations import RouterFilter, FieldFilterConfiguration
from th2common.schema.strategy import Th2BatchMsgFieldExtraction


class FilterStrategy(ABC):

    @abstractmethod
    def verify(self, message: Message, router_filter: RouterFilter = None, router_filters: List[RouterFilter] = None):
        pass


class DefaultFilterStrategy(FilterStrategy):

    def __init__(self, extract_strategy=Th2BatchMsgFieldExtraction()) -> None:
        self.extract_strategy = extract_strategy

    def verify(self, message: Message, router_filter: RouterFilter = None, router_filters: List[RouterFilter] = None):
        if router_filters is None:
            msg_field_filters = dict(router_filter.get_message())
            msg_field_filters.update(router_filter.get_metadata())
            return self.check_values(self.extract_strategy.get_fields(message), msg_field_filters)
        else:
            for fields_filter in router_filters:
                if self.verify(message=message, router_filter=fields_filter):
                    return True
            return False

    def check_values(self, message_fields: {str: str}, field_filters: {str: FieldFilterConfiguration}):
        pass


