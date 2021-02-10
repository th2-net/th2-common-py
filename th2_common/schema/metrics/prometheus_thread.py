import asyncio
import logging
import threading

from prometheus_client.exposition import start_wsgi_server

from th2_common.schema.metrics.common_metrics import CommonMetrics

logger = logging.getLogger()


class PrometheusThread(threading.Thread):

    def __init__(self, port=8000, host='localhost'):
        threading.Thread.__init__(self)
        self.port = port
        self.host = host
        self.__loop = asyncio.get_event_loop()

    def run(self):
        CommonMetrics.LIVENESS.inc()
        start_wsgi_server(self.port, self.host)
        logger.info('Prometheus server is started')
        self.__loop.run_forever()

    def stop(self):
        try:
            CommonMetrics.LIVENESS.dec()
        finally:
            self.__loop.close()
