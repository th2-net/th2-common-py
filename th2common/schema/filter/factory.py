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

from th2common.schema.filter.filters import Filter, MqMsgFilter, GrpcMsgFilter
from th2common.schema.filter.strategy import DefaultFilterStrategy
from th2common.schema.message.configurations import FilterableConfiguration, MessageRouterConfiguration


class FilterFactory(ABC):

    @abstractmethod
    def create_filter(self, configuration: FilterableConfiguration) -> Filter:
        pass


class DefaultFilterFactory(FilterFactory):

    def __init__(self, filter_strategy=DefaultFilterStrategy()) -> None:
        self.filter_strategy = filter_strategy

    def create_filter(self, configuration: FilterableConfiguration) -> Filter:
        if isinstance(configuration, MessageRouterConfiguration):
            return MqMsgFilter(configuration, self.filter_strategy)
        else:
            return GrpcMsgFilter(configuration, self.filter_strategy)
