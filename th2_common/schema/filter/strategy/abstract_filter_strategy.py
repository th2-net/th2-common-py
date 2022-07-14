#   Copyright 2022-2022 Exactpro (Exactpro Systems Limited)
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

from abc import ABC
from fnmatch import fnmatch
from typing import Callable, Dict

from th2_common.schema.filter.strategy.filter_strategy import FilterStrategy
from th2_common.schema.message.configuration.message_configuration import FieldFilterConfiguration, FieldFilterOperation


class AbstractFilterStrategy(FilterStrategy, ABC):

    @staticmethod
    def check_value(value: str, filter_configuration: FieldFilterConfiguration) -> bool:
        expected = filter_configuration.value

        options: Dict[FieldFilterOperation, Callable[[str], bool]] = {
            FieldFilterOperation.EQUAL: (lambda v: v == expected),
            FieldFilterOperation.NOT_EQUAL: (lambda v: v != expected),
            FieldFilterOperation.EMPTY: (lambda v: len(v) == 0),
            FieldFilterOperation.NOT_EMPTY: (lambda v: len(v) != 0),
            FieldFilterOperation.WILDCARD: (lambda v: fnmatch(v, expected)),  # type: ignore
            FieldFilterOperation.UNKNOWN: (lambda v: False)
        }

        return options[filter_configuration.operation](value)
