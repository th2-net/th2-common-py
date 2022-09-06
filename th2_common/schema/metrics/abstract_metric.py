#   Copyright 2022 Exactpro (Exactpro Systems Limited)
#   Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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

from th2_common.schema.metrics.metric import Metric


class AbstractMetric(Metric, ABC):

    def __init__(self) -> None:
        self._enabled: bool = False

    def is_enabled(self) -> bool:
        return self._enabled

    def enable(self) -> None:
        if not self._enabled:
            self._enabled = True
            self.on_value_change(self._enabled)

    def disable(self) -> None:
        if self._enabled:
            self._enabled = False
            self.on_value_change(self._enabled)

    @abstractmethod
    def on_value_change(self, value: bool) -> None:
        pass
