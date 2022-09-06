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

from test.test_configuration.resources import dict_configuration
from test.utils import object_to_dict

import pytest
from th2_common.schema.factory.common_factory import CommonFactory


@pytest.mark.usefixtures('factory')
def test_rabbit_mq_configuration(factory: CommonFactory) -> None:
    factory._create_rabbit_mq_configuration()

    expected = dict_configuration.rabbit_mq_configuration_dict
    actual = factory.rabbit_mq_configuration

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_message_router_configuration(factory: CommonFactory) -> None:
    factory._create_message_router_configuration()

    expected = dict_configuration.mq_configuration_dict
    actual = factory.message_router_configuration

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_conn_manager_configuration(factory: CommonFactory) -> None:
    expected = dict_configuration.mq_router_configuration_dict
    actual = factory._create_conn_manager_configuration()

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_grpc_configuration(factory: CommonFactory) -> None:
    factory._create_grpc_configuration()

    expected = dict_configuration.grpc_configuration_dict
    actual = factory.grpc_configuration

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_grpc_router_configuration(factory: CommonFactory) -> None:
    expected = dict_configuration.grpc_router_configuration_dict
    actual = factory._create_grpc_router_configuration()

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_cradle_configuration(factory: CommonFactory) -> None:
    factory.create_cradle_configuration()

    expected = dict_configuration.cradle_configuration_dict
    actual = factory.create_cradle_configuration()

    assert object_to_dict(actual) == expected


@pytest.mark.usefixtures('factory')
def test_prometheus_configuration(factory: CommonFactory) -> None:
    factory._create_prometheus_configuration()

    expected = dict_configuration.prometheus_configuration_dict
    actual = factory.prometheus_config

    assert object_to_dict(actual) == expected
