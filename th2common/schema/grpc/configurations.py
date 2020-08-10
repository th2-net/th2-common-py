#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
#
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
# # Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from th2common.schema.message.configurations import Configuration, FilterableConfiguration, RouterFilter, \
    FieldFilterConfiguration


class GrpcEndpointConfiguration(Configuration):

    def __init__(self, host, port) -> None:
        self.host = host
        self.port = port


class GrpcServerConfiguration(GrpcEndpointConfiguration):

    def __init__(self, host, port, workers) -> None:
        super().__init__(host, port)
        self.workers = workers


class GrpcRouterConfiguration(Configuration):
    def __init__(self, services, server) -> None:
        self.services = services
        self.serverConfiguration = GrpcServerConfiguration(**server)


class GrpcRouterFilterConfiguration(RouterFilter):

    def __init__(self, endpoint: str, metadata, message) -> None:
        self.metadata = metadata
        self.message = message
        self.endpoint = endpoint

    def get_metadata(self) -> {str: FieldFilterConfiguration}:
        return self.metadata

    def get_message(self) -> {str: FieldFilterConfiguration}:
        return self.message


class GrpcRawFilterStrategy(FilterableConfiguration):

    def __init__(self, filters) -> None:
        self.filters = list()
        for key in filters.keys():
            self.filters.append(GrpcRouterFilterConfiguration(**filters[key]))


class GrpcRawRobinStrategy(FilterableConfiguration):

    def __init__(self, endpoints, name) -> None:
        self.endpoints = endpoints
        self.name = name


class GrpcServiceConfiguration(Configuration):
    pass
