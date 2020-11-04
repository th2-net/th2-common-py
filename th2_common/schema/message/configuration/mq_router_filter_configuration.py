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


from th2_common.schema.message.configuration.field_filter_configuration import FieldFilterConfiguration
from th2_common.schema.message.configuration.router_filter import RouterFilter


class MqRouterFilterConfiguration(RouterFilter):

    def __init__(self, metadata=None, message=None) -> None:
        if metadata is None:
            metadata = dict()
        if message is None:
            message = dict()
        self.metadata = {key: FieldFilterConfiguration(**metadata[key]) for key in metadata.keys()}
        self.message = {key: FieldFilterConfiguration(**message[key]) for key in message.keys()}

    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        return self.metadata

    def get_message(self) -> {str: FieldFilterConfiguration}:
        return self.message
