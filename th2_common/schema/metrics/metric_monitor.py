#   Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

from th2_common.schema.metrics.metric import Metric


class MetricMonitor:

    def __init__(self, name: str, arbiter: Metric) -> None:
        self.name = name
        self.arbiter = arbiter

    @property
    def is_enabled(self) -> bool:
        return self.arbiter.is_enabled(self)

    @is_enabled.setter
    def is_enabled(self, value):
        if value:
            self.enable()
        else:
            self.disable()

    @property
    def is_metric_enabled(self):
        return self.arbiter.enabled

    def enable(self):
        self.arbiter.enable(self)

    def disable(self):
        self.arbiter.disable(self)