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

from typing import List

from th2_common.schema.metrics.aggregating_metric_monitor import AggregatingMetricMonitor
from th2_common.schema.metrics.metric import Metric


class AggregatingMetric(Metric):

    def __init__(self, metrics: List[Metric]) -> None:
        self.metrics = metrics

    def create_monitor(self, name: str) -> AggregatingMetricMonitor:
        return AggregatingMetricMonitor(name, self)

    def is_enabled(self) -> bool:
        return all(metric.is_enabled() for metric in self.metrics)

    def enable(self) -> None:
        for metric in self.metrics:
            metric.enable()

    def disable(self) -> None:
        for metric in self.metrics:
            metric.disable()
