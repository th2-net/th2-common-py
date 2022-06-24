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
from th2_common.schema.grpc.configuration.grpc_configuration import GrpcFilterConfiguration


class DefaultGrpcFilterStrategy(AbstractFilterStrategy):

    RouterFiltersType = Union[List[GrpcFilterConfiguration], GrpcFilterConfiguration]

    def verify(self,
               message: Dict[str, str],
               router_filters: Optional[RouterFiltersType] = None) -> bool:
        if isinstance(router_filters, GrpcFilterConfiguration):
            return all(self.check_value(message[field_filter.field_name], field_filter)
                       if field_filter.field_name in message else False
                       for field_filter in router_filters.properties)

        elif isinstance(router_filters, list) and len(router_filters) > 0:
            return all(self.verify(message, fields_filter) for fields_filter in router_filters)
        else:
            return True
