# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
# Licensed under the Apache License, Version 2.0 (the "License");
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

# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
import grpc

from th2common.gen import message_comparator_pb2 as th2_dot_message__comparator__pb2


class MessageComparatorServiceStub(object):
    """Missing associated documentation comment in .proto file"""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.compareFilterVsMessages = channel.unary_unary(
                '/th2.utility.MessageComparatorService/compareFilterVsMessages',
                request_serializer=th2_dot_message__comparator__pb2.CompareFilterVsMessagesRequest.SerializeToString,
                response_deserializer=th2_dot_message__comparator__pb2.CompareFilterVsMessagesResponse.FromString,
                )
        self.compareMessageVsMessage = channel.unary_unary(
                '/th2.utility.MessageComparatorService/compareMessageVsMessage',
                request_serializer=th2_dot_message__comparator__pb2.CompareMessageVsMessageRequest.SerializeToString,
                response_deserializer=th2_dot_message__comparator__pb2.CompareMessageVsMessageResponse.FromString,
                )


class MessageComparatorServiceServicer(object):
    """Missing associated documentation comment in .proto file"""

    def compareFilterVsMessages(self, request, context):
        """Compare one filter versus some messages
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def compareMessageVsMessage(self, request, context):
        """Compare one message versus one message
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_MessageComparatorServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'compareFilterVsMessages': grpc.unary_unary_rpc_method_handler(
                    servicer.compareFilterVsMessages,
                    request_deserializer=th2_dot_message__comparator__pb2.CompareFilterVsMessagesRequest.FromString,
                    response_serializer=th2_dot_message__comparator__pb2.CompareFilterVsMessagesResponse.SerializeToString,
            ),
            'compareMessageVsMessage': grpc.unary_unary_rpc_method_handler(
                    servicer.compareMessageVsMessage,
                    request_deserializer=th2_dot_message__comparator__pb2.CompareMessageVsMessageRequest.FromString,
                    response_serializer=th2_dot_message__comparator__pb2.CompareMessageVsMessageResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'th2.utility.MessageComparatorService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class MessageComparatorService(object):
    """Missing associated documentation comment in .proto file"""

    @staticmethod
    def compareFilterVsMessages(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/th2.utility.MessageComparatorService/compareFilterVsMessages',
            th2_dot_message__comparator__pb2.CompareFilterVsMessagesRequest.SerializeToString,
            th2_dot_message__comparator__pb2.CompareFilterVsMessagesResponse.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def compareMessageVsMessage(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/th2.utility.MessageComparatorService/compareMessageVsMessage',
            th2_dot_message__comparator__pb2.CompareMessageVsMessageRequest.SerializeToString,
            th2_dot_message__comparator__pb2.CompareMessageVsMessageResponse.FromString,
            options, channel_credentials,
            call_credentials, compression, wait_for_ready, timeout, metadata)