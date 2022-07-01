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

import json
from typing import Any, Dict, List, Optional, Tuple, Union

from th2_common.schema.configuration.abstract_configuration import AbstractConfiguration
from th2_common.schema.message.configuration.message_configuration import FieldFilterConfiguration


class GrpcConfiguration(AbstractConfiguration):

    def __init__(self,
                 services: Dict[str, Dict[str, Any]],
                 server: Optional[Dict[str, Any]] = None,
                 **kwargs: Any) -> None:
        self.services: Dict[str, GrpcServiceConfiguration] = {
            name: GrpcServiceConfiguration(**params) for name, params in services.items()
        }
        if server is not None:
            self.server = GrpcServerConfiguration(**server)

        self.check_unexpected_args(kwargs)


class GrpcConnectionConfiguration(AbstractConfiguration):

    def __init__(self,
                 workers: int = 5,
                 retryPolicy: Optional[Dict[str, Any]] = None,
                 request_size_limit: Union[int, float] = 4,
                 **kwargs: Any) -> None:
        if retryPolicy is None:
            retryPolicy = {}
        self.workers = int(workers)
        self.retry_policy = GrpcRetryPolicyConfiguration(**retryPolicy)
        self.request_size_limit = [
            ('grpc.max_receive_message_length', round(request_size_limit * 1024 * 1024)),
            ('grpc.max_send_message_length', round(request_size_limit * 1024 * 1024))
        ]

        self.check_unexpected_args(kwargs)


class GrpcServiceConfiguration(AbstractConfiguration):

    PropertiesType = List[Dict[str, str]]
    FiltersType = List[Dict[str, PropertiesType]]

    def __init__(self,
                 endpoints: Dict[str, Any],
                 service_class: str,
                 attributes: Optional[List[str]] = None,
                 filters: Optional[FiltersType] = None,
                 strategy: Optional[Dict[str, Any]] = None,  # deprecated
                 **kwargs: Any) -> None:
        self.endpoints: Dict[str, GrpcEndpointConfiguration] = {
            endpoint: GrpcEndpointConfiguration(**configuration) for endpoint, configuration in endpoints.items()
        }
        self.service_class = service_class

        if attributes is not None:
            self.attributes = attributes
        else:
            self.attributes = []

        if filters is not None:
            self.filters = [GrpcFilterConfiguration(**filter_obj) for filter_obj in filters]
        else:
            self.filters = []

        self.check_unexpected_args(kwargs)


class GrpcServerConfiguration(AbstractConfiguration):

    def __init__(self,
                 attributes: List[str],
                 host: str,
                 port: int,
                 workers: int,
                 **kwargs: Any) -> None:
        self.attributes = attributes
        self.host = host
        self.port = port
        self.workers = workers

        self.check_unexpected_args(kwargs)


class GrpcRetryPolicyConfiguration(AbstractConfiguration):

    OptionsType = Tuple[str, Union[str, int]]
    ServicesDictType = Dict[str, str]

    def __init__(self,
                 maxAttemtps: int = 5,
                 initialBackoff: float = 5.,
                 maxBackoff: float = 60.,
                 backoffMultiplier: int = 2,
                 statusCodes: Optional[List[str]] = None,
                 services: Optional[List[ServicesDictType]] = None,
                 **kwargs: Any) -> None:
        """
        Initializes retry policy for later usage of 'options' parameter.

        :param int maxAttemtps: maximum number of retries (defaults to 5)
        :param float initialBackoff: delay before the first retry in seconds (defaults to 5.0)
        :param float maxBackoff: maximum delay before the retry (defaults to 60.0)
        :param int backoffMultiplier: multiplier by which every subsequent delay is changed (defaults to 2)
        :param list statusCodes: list of status code strings which invoke retry attempt (defaults to ['UNAVAILABLE'])
        :param list services: list of dictionaries with keys 'service' and 'method', indicating where policy is
            applicable (defaults to every)
        """
        self.max_attempts = maxAttemtps
        self.initial_backoff = initialBackoff
        self.max_backoff = maxBackoff
        self.backoff_multiplier = backoffMultiplier
        self.status_codes = statusCodes
        self.services = services

        self.check_unexpected_args(kwargs)

    @property
    def options(self) -> List[OptionsType]:
        """Returns the retry options for the channel constructor."""
        if self.services is None:
            self.services = [{}]

        if self.status_codes is None:
            self.status_codes = ['UNAVAILABLE']

        service_config_json = {  # noqa: ECE001
            'methodConfig': [
                {
                    'name': self.services,
                    'retryPolicy': {
                        'maxAttempts': self.max_attempts,
                        'initialBackoff': f'{self.initial_backoff}s',
                        'maxBackoff': f'{self.max_backoff}s',
                        'backoffMultiplier': self.backoff_multiplier,
                        'retryableStatusCodes': self.status_codes,
                    }
                }
            ]
        }

        return [('grpc.enable_retries', 1), ('grpc.service_config', json.dumps(service_config_json))]


class GrpcEndpointConfiguration(AbstractConfiguration):

    def __init__(self,
                 host: str,
                 port: int,
                 attributes: Optional[List[str]] = None,
                 **kwargs: Any) -> None:
        self.host = host
        self.port = port

        if attributes is not None:
            self.attributes = attributes
        else:
            self.attributes = []

        self.check_unexpected_args(kwargs)


class GrpcFilterConfiguration(AbstractConfiguration):

    FilterType = Dict[str, str]

    def __init__(self,
                 properties: Optional[List[FilterType]] = None,
                 metadata: Optional[List[FilterType]] = None,
                 message: Optional[List[FilterType]] = None,
                 **kwargs: Any):
        self.properties: List[FieldFilterConfiguration] = []

        if properties is not None:
            self.properties.extend([FieldFilterConfiguration(**prop) for prop in properties])
        else:
            if metadata is not None:
                self.properties.extend([FieldFilterConfiguration(**md) for md in metadata])
            if message is not None:
                self.properties.extend([FieldFilterConfiguration(**msg) for msg in message])

        self.check_unexpected_args(kwargs)
