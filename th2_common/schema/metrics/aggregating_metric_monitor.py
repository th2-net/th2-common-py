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

from th2_common.schema.metrics.metric import Metric


class AggregatingMetricMonitor(Metric):

    def __init__(self, name: str, aggregating_metric: Metric) -> None:
        self.name = name
        self.aggregating_metric = aggregating_metric

    def is_enabled(self) -> bool:
        return self.aggregating_metric.is_enabled()

    def enable(self) -> None:
        self.aggregating_metric.enable()

    def disable(self) -> None:
        self.aggregating_metric.disable()
