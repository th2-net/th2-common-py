import logging

from prometheus_client import Counter


class MetricLogHandler(logging.Handler):

    def __init__(self, level='NOTSET', metric_name='th2_logging_metric'):
        self.metric = Counter(metric_name, 'This metric registers log entries below specified level',
                              ['logger', 'level'])
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        self.metric.labels(record.name, record.levelname).inc()
