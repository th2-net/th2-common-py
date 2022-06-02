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

import logging

from prometheus_client import Counter


class MetricLogHandler(logging.Handler):

    def __init__(self, level: str = 'ERROR', metric_name: str = 'th2_logging_metric') -> None:
        self.metric = Counter(metric_name, 'This metric registers log entries below specified level',
                              ['logger', 'level'])
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        self.metric.labels(record.name, record.levelname).inc()
