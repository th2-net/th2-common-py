import threading

from prometheus_client.exposition import start_wsgi_server
from th2_common.schema.metrics.common_metrics import CommonMetrics


class PrometheusThread(threading.Thread):
    stopped = False

    def __init__(self, port=8000, host='localhost'):
        threading.Thread.__init__(self)
        self.port = port
        self.host = host

    def run(self):
        CommonMetrics.LIVENESS.inc()
        start_wsgi_server(self.port, self.host)
        while not self.stopped:
            pass

    def stop(self):
        CommonMetrics.LIVENESS.dec()
        self.stopped = True
