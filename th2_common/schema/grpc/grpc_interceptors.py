from typing import Any, Callable

import grpc
from grpc_interceptor import ServerInterceptor
from grpc_interceptor.exceptions import GrpcException
from prometheus_client import Counter
from th2_common.schema.metrics.metric_utils import update_grpc_metrics


class MetricInterceptor(ServerInterceptor):
    """ gRPC server interceptor for prometheus metrics"""

    def __init__(self, received_call_total: Counter,
                 received_request: Counter, received_response: Counter):
        """ MetricInterceptor constructor function
        Args:
            received_call_total (Counter): Total number of consuming particular gRPC method.
            received_request (Counter): Number of bytes received from particular gRPC call.
            received_response (Counter): Number of bytes sent to particular gRPC call.
        """
        self.received_call_total = received_call_total
        self.received_request = received_request
        self.received_response = received_response

    def intercept(
            self,
            method: Callable,
            request: Any,
            context: grpc.ServicerContext,
            method_name: str,
    ) -> Any:
        """
        Increases Counter metrics by appropriate values
         Args:
             method: The next interceptor, or method implementation.
             request: The RPC request, as a protobuf message.
             context: The ServicerContext pass by gRPC to the service.
             method_name: A string of the form
                 "/protobuf.package.Service/Method"
         Returns:
             RPC method response
         """
        try:

            response = method(request, context)
            update_grpc_metrics(method_name, response, self.received_call_total,
                                self.received_request, self.received_response)

            return response
        except GrpcException as e:
            context.set_code(e.status_code)
            context.set_details(e.details)
            raise
