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

from typing import List

from th2_common.schema.message.configuration.field_filter_configuration import FieldFilterConfiguration
from th2_common.schema.message.configuration.router_filter import RouterFilterConfiguration


class GrpcRouterFilterConfiguration(RouterFilterConfiguration):

    def __init__(self, endpoint: str, metadata, message, **kwargs) -> None:
        self.metadata = metadata
        self.message = message
        self.endpoint = endpoint
        self.check_unexpected_args(kwargs)

    def get_metadata(self) -> List[FieldFilterConfiguration]:
        return self.metadata

    def get_message(self) -> List[FieldFilterConfiguration]:
        return self.message
