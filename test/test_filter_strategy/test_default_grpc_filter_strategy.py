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

from test.resources.test_classes import grpc_router, TestConnection

from th2_common.schema.exception.grpc_router_error import GrpcRouterError


def test_default_grpc_filter_strategy_simple_filter() -> None:
    services = grpc_router._filter_services_by_name(service_class_name='ServiceClass1')
    connection = TestConnection(services=services)  # type: ignore

    filtered_service = connection._filter_services(properties={'session_alias': 'asdfgh', 'msg11': ''})

    assert filtered_service.service_class == 'ServiceClass1'


def test_default_grpc_filter_strategy_multiple_filters() -> None:
    services = grpc_router._filter_services_by_name(service_class_name='ServiceClass2')
    connection = TestConnection(services=services)  # type: ignore

    filtered_service = connection._filter_services(properties={'prop21': '21', 'prop22': '22', 'prop23': '23'})

    assert filtered_service.service_class == 'ServiceClass2'


def test_default_grpc_filter_strategy_multiple_services() -> None:
    services = grpc_router._filter_services_by_name(service_class_name='ServiceClass3')
    connection = TestConnection(services=services)  # type: ignore

    try:
        connection._filter_services(properties={'prop31': '31', 'prop32': '32'})
        raise AssertionError()
    except GrpcRouterError:
        assert True  # two services pass the filter -> GrpcRouterError


def test_default_grpc_filter_strategy_no_services() -> None:
    services = grpc_router._filter_services_by_name(service_class_name='ServiceClass3')
    connection = TestConnection(services=services)  # type: ignore

    try:
        connection._filter_services(properties={'any_filed': '0'})
        raise AssertionError()
    except GrpcRouterError:
        assert True  # no services pass the filter -> GrpcRouterError
