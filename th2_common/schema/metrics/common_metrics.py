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

from prometheus_client import Gauge


class CommonMetrics:
    DEFAULT_BUCKETS = [0.000_25, 0.000_5, 0.001, 0.005, 0.010, 0.015, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0]

    LIVENESS = Gauge("th2_liveness", "Service liveness")
    READINESS = Gauge("th2_readiness", "Service readiness")

    RABBITMQ_READINESS = True
    GRPC_READINESS = True
    ALL_READINESS = [RABBITMQ_READINESS, GRPC_READINESS]

    def check_common_readiness(self):
        for readiness_parameter in self.ALL_READINESS:
            if not readiness_parameter:
                return False
        return True
