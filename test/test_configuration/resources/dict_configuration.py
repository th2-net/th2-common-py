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


rabbit_mq_configuration_dict = {
    'exchange_name': 'exchange',
    'host': '0.0.0.0',
    'password': 'password',
    'port': 10,
    'prefetch_count': 100,
    'subscriber_name': None,
    'username': 'name',
    'vhost': 'v-host'
}

mq_configuration_dict = {  # noqa: ECE001
    'queues': {
        'queue_AND_filter': {
            'attributes': ['attr11', 'attr12'],
            'can_read': True,
            'can_write': True,
            'exchange': 'exchange1',
            'filters': [
                {
                    'message': [
                        {
                            'field_name': 'msg11',
                            'operation': 'EQUAL',
                            'value': '11'
                        }
                    ],
                    'metadata': [
                        {
                            'field_name': 'session_alias',
                            'operation': 'EQUAL',
                            'value': 'qwerty'
                        }
                    ]
                }
            ],
            'queue': '',
            'routing_key': 'name1'
        },
        'queue_EMPTY_filter': {
            'attributes': [],
            'can_read': True,
            'can_write': True,
            'exchange': 'exchange3',
            'filters': [],
            'queue': 'queue_EMPTY_filter',
            'routing_key': 'name3'
        },
        'queue_OR_AND_filter': {
            'attributes': [],
            'can_read': True,
            'can_write': True,
            'exchange': 'exchange3',
            'filters': [
                {
                    'message': [],
                    'metadata': [
                        {
                            'field_name': 'session_alias',
                            'operation': 'EQUAL',
                            'value': 'asdfgh'
                        },
                        {
                            'field_name': 'direction',
                            'operation': 'EQUAL',
                            'value': 'SECOND'
                        }
                    ]
                },
                {
                    'message': [
                        {
                            'field_name': 'msg31',
                            'operation': 'EQUAL',
                            'value': '31'
                        },
                        {
                            'field_name': 'msg32',
                            'operation': 'EQUAL',
                            'value': '32'
                        }
                    ],
                    'metadata': []
                }
            ],
            'queue': '',
            'routing_key': 'name3'
        },
        'queue_OR_filter': {
            'attributes': ['attr21', 'attr22', 'attr23'],
            'can_read': True,
            'can_write': True,
            'exchange': 'exchange2',
            'filters': [
                {
                    'message': [
                        {
                            'field_name': 'msg21',
                            'operation': 'EQUAL',
                            'value': '21'
                        }
                    ],
                    'metadata': []
                },
                {
                    'message': [
                        {
                            'field_name': 'msg22',
                            'operation': 'EQUAL',
                            'value': '22'
                        }
                    ],
                    'metadata': []
                }
            ],
            'queue': 'queue_OR_filter',
            'routing_key': ''
        }
    }
}

mq_router_configuration_dict = {
    'connection_close_timeout': 10,
    'connection_timeout': -1,
    'max_connection_recovery_timeout': 6000,
    'max_recovery_attempts': 5,
    'message_recursion_limit': 100,
    'min_connection_recovery_timeout': 10000,
    'prefetch_count': 50,
    'subscriber_name': None
}

grpc_configuration_dict = {  # noqa: ECE001
    'server': {
        'attributes': None,
        'host': 'localhost',
        'port': 8080,
        'workers': 5
    },
    'services': {
        'service1': {
            'attributes': [],
            'endpoints': {
                'endpoint11': {
                    'attributes': [],
                    'host': '0.0.0.1',
                    'port': '1010'
                },
                'endpoint12': {
                    'attributes': [],
                    'host': '0.0.0.2',
                    'port': 1001
                }
            },
            'filters': [
                {
                    'properties': [
                        {
                            'field_name': 'session_alias',
                            'operation': 'NOT_EQUAL',
                            'value': 'qwerty'
                        },
                        {
                            'field_name': 'msg11',
                            'operation': 'EMPTY',
                            'value': None
                        }
                    ]
                }
            ],
            'service_class': 'ServiceClass1'
        },
        'service2': {
            'attributes': [],
            'endpoints': {
                'endpoint2.1': {
                    'attributes': [],
                    'host': '0.0.0.3',
                    'port': 1011
                }
            },
            'filters': [
                {
                    'properties': [
                        {
                            'field_name': 'prop21',
                            'operation': 'EQUAL',
                            'value': '21'
                        },
                        {
                            'field_name': 'prop22',
                            'operation': 'EQUAL',
                            'value': '22'
                        }
                    ]
                },
                {
                    'properties': [
                        {
                            'field_name': 'prop23',
                            'operation': 'NOT_EMPTY',
                            'value': None
                        }
                    ]
                }
            ],
            'service_class': 'ServiceClass2'
        },
        'service3': {
            'attributes': [],
            'endpoints': {},
            'filters': [
                {
                    'properties': [
                        {
                            'field_name': 'prop31',
                            'operation': 'EQUAL',
                            'value': '31'
                        },
                        {
                            'field_name': 'prop32',
                            'operation': 'EQUAL',
                            'value': '32'
                        }
                    ]
                }
            ],
            'service_class': 'ServiceClass3'
        },
        'service4': {  # the same as service3
            'attributes': [],
            'endpoints': {},
            'filters': [
                {
                    'properties': [
                        {
                            'field_name': 'prop31',
                            'operation': 'EQUAL',
                            'value': '31'
                        },
                        {
                            'field_name': 'prop32',
                            'operation': 'EQUAL',
                            'value': '32'
                        }
                    ]
                }
            ],
            'service_class': 'ServiceClass3'
        }
    }
}

grpc_router_configuration_dict = {
    'retry_policy': {
        'backoff_multiplier': 2,
        'initial_backoff': 5.0,
        'max_attempts': 5,
        'max_backoff': 60.0,
        'services': None,
        'status_codes': None,
    },
    'request_size_limit': [
        ('grpc.max_receive_message_length', 4194304),
        ('grpc.max_send_message_length', 4194304)
    ],
    'workers': 5
}

cradle_configuration_dict = {
    'data_center': 'some_datacenter',
    'host': 'localhost',
    'port': 8080,
    'keyspace': 'some_keyspace',
    'username': 'user1',
    'password': 'password123'
}

prometheus_configuration_dict = {
    'enabled': False,
    'host': '0.0.0.0',
    'port': 9752
}
