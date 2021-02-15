from prometheus_client.exposition import start_wsgi_server

from th2_common.schema.metrics.common_metrics import CommonMetrics


class PrometheusServer:

    def __init__(self, port=8000, host='localhost'):
        self.stopped = None
        self.port = port
        self.host = host

    def run(self):
        CommonMetrics.LIVENESS.inc()
        start_wsgi_server(self.port, self.host)
        self.stopped = False

    def stop(self):
        CommonMetrics.LIVENESS.dec()
        self.stopped = True
