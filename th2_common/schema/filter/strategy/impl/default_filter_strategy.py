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

from typing import Dict, List, Optional, Union

from th2_common.schema.filter.strategy.abstract_filter_strategy import AbstractFilterStrategy
from th2_common.schema.message.configuration.message_configuration import FieldFilterConfiguration, \
    MqRouterFilterConfiguration
from th2_common.schema.strategy.field_extraction.th2_msg_field_extraction import Th2MsgFieldExtraction
from th2_grpc_common.common_pb2 import MessageGroup


class DefaultFilterStrategy(AbstractFilterStrategy):

    RouterFiltersType = Union[List[MqRouterFilterConfiguration], MqRouterFilterConfiguration]

    def __init__(self) -> None:
        self.extract_strategy = Th2MsgFieldExtraction()

    def verify(self,
               message: MessageGroup,
               router_filters: Optional[RouterFiltersType] = None) -> bool:
        if isinstance(router_filters, MqRouterFilterConfiguration):
            filters = router_filters.message + router_filters.metadata
            return any(self.check_values(self.extract_strategy.get_fields(any_message), filters)
                       for any_message in message.messages)

        elif isinstance(router_filters, list) and len(router_filters) > 0:
            return any(self.verify(message, fields_filter) for fields_filter in router_filters)

        else:
            return True

    def check_values(self, message_fields: Dict[str, str], field_filters: List[FieldFilterConfiguration]) -> bool:
        return all(self.check_value(message_fields[field_filter.field_name], field_filter)
                   if field_filter.field_name in message_fields else False
                   for field_filter in field_filters)
