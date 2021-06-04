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


from abc import ABC, abstractmethod

from th2_common.schema.configuration.configuration import AbstractConfiguration
from th2_common.schema.message.configuration.field_filter_configuration import FieldFilterConfiguration


class RouterFilterConfiguration(AbstractConfiguration, ABC):

    @abstractmethod
    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        pass

    @abstractmethod
    def get_message(self) -> {str: FieldFilterConfiguration}:
        pass
