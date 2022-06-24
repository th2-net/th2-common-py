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

from test.resources.configuration import dict_configuration
from test.resources.test_classes import test_common_factory
from test.resources.test_util import object_to_dict


def test_rabbit_mq_configuration() -> None:
    expected = dict_configuration.rabbit_mq_configuration_dict
    actual = test_common_factory.rabbit_mq_configuration

    assert object_to_dict(actual) == expected


def test_message_router_configuration() -> None:
    expected = dict_configuration.mq_configuration_dict
    actual = test_common_factory.message_router_configuration

    assert object_to_dict(actual) == expected


def test_conn_manager_configuration() -> None:
    expected = dict_configuration.mq_router_configuration_dict
    actual = test_common_factory.conn_manager_configuration

    assert object_to_dict(actual) == expected


def test_grpc_configuration() -> None:
    expected = dict_configuration.grpc_configuration_dict
    actual = test_common_factory.grpc_configuration

    assert object_to_dict(actual) == expected


def test_grpc_router_configuration() -> None:
    expected = dict_configuration.grpc_router_configuration_dict
    actual = test_common_factory.grpc_router_configuration

    assert object_to_dict(actual) == expected


def test_cradle_configuration() -> None:
    expected = dict_configuration.cradle_configuration_dict
    actual = test_common_factory.cradle_configuration

    assert object_to_dict(actual) == expected


def test_prometheus_configuration() -> None:
    expected = dict_configuration.prometheus_configuration_dict
    actual = test_common_factory.prometheus_configuration

    assert object_to_dict(actual) == expected
