from fnmatch import fnmatch
from typing import List, Optional, Dict

from th2_grpc_common.common_pb2 import Message

from th2_common.schema.filter.strategy.filter_strategy import FilterStrategy
from th2_common.schema.message.configuration.message_configuration import RouterFilterConfiguration, \
    FieldFilterConfiguration, FieldFilterOperation


class DefaultGrpcFilterStrategy(FilterStrategy):

    def verify(self, message: Optional[Dict[str, str]], router_filter: Optional[FieldFilterConfiguration] = None,
               router_filters: Optional[List[FieldFilterConfiguration]] = None):
        if not router_filters and not router_filter:
            return True
        elif not router_filters:
            return self.check_value(message, router_filter)
        else:
            for fields_filter in router_filters:
                if self.verify(message=message, router_filter=fields_filter):
                    return True
            return False

    @staticmethod
    def check_value(value, filter_configuration: FieldFilterConfiguration):
        expected = filter_configuration.value
        value = value[filter_configuration.field_name]

        if filter_configuration.operation is FieldFilterOperation.EQUAL:
            return value == expected
        elif filter_configuration.operation is FieldFilterOperation.NOT_EQUAL:
            return value != expected
        elif filter_configuration.operation is FieldFilterOperation.EMPTY:
            return len(value) == 0
        elif filter_configuration.operation is FieldFilterOperation.NOT_EMPTY:
            return len(value) != 0
        elif filter_configuration.operation is FieldFilterOperation.WILDCARD:
            return fnmatch(value, expected)
        else:
            return False
