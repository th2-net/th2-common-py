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

from typing import Any, Optional

from th2_common.schema.configuration.abstract_configuration import AbstractConfiguration


class RabbitMQConfiguration(AbstractConfiguration):

    def __init__(self,
                 host: str,
                 vHost: str,
                 port: int,
                 username: str,
                 password: str,
                 exchangeName: str,
                 prefetch_count: int = 100,
                 subscriberName: Optional[str] = None,
                 **kwargs: Any) -> None:
        self.host = host
        self.vhost = vHost
        self.port = int(port)
        self.username = username
        self.password = password
        self.subscriber_name = subscriberName
        self.exchange_name = exchangeName
        self.prefetch_count = int(prefetch_count)

        self.check_unexpected_args(kwargs)
