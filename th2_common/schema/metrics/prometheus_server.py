#   Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
