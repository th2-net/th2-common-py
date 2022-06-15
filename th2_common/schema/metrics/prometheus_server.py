#   Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

from threading import Thread
from typing import Optional
from wsgiref.simple_server import make_server, WSGIServer

from prometheus_client import make_wsgi_app
from prometheus_client.exposition import _SilentHandler


class PrometheusServer:

    def __init__(self, port: int = 8000, host: str = 'localhost') -> None:
        self.stopped: Optional[bool] = None
        self.port = port
        self.host = host
        self.httpd: Optional[WSGIServer] = None
        self.server_thread: Optional[Thread] = None

    def run(self) -> None:
        if self.httpd is None or self.stopped is True:
            self.stopped = False
            app = make_wsgi_app()
            self.httpd = make_server(self.host, self.port, app, handler_class=_SilentHandler)
            self.server_thread = Thread(target=self.httpd.serve_forever, daemon=True)
            self.server_thread.start()

    def stop(self) -> None:
        if self.stopped is False and self.httpd is not None and self.server_thread is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.server_thread.join()
            self.stopped = True
