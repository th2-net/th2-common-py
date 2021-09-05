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

from enum import Enum, auto

from th2_common.schema.configuration.abstract_configuration import AbstractConfiguration


class FieldFilterOperation(Enum):
    EQUAL = auto()
    NOT_EQUAL = auto()
    EMPTY = auto()
    NOT_EMPTY = auto()
    WILDCARD = auto()


class FieldFilterConfiguration(AbstractConfiguration):

    def __init__(self, value: str = None, expectedValue: str = None, fieldName: str = None,
                 operation: FieldFilterOperation = None, **kwargs) -> None:
        self.value = value or expectedValue
        self.field_name = fieldName
        self.operation = operation
        self.check_unexpected_args(kwargs)
