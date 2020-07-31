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

from th2 import act_fix_pb2_grpc as import_stub


class ActService(object):

    def __init__(self, router):
        self.connector = router.get_connection(ActService, import_stub.ActStub)

    def sendMessage(self, request):
        return self.connector.create_request("sendMessage", request)

    def placeOrderFIX(self, request):
        return self.connector.create_request("placeOrderFIX", request)

    def placeOrderReplaceFIX(self, request):
        return self.connector.create_request("placeOrderReplaceFIX", request)

    def placeOrderCancelFIX(self, request):
        return self.connector.create_request("placeOrderCancelFIX", request)

    def placeOrderMultilegFIX(self, request):
        return self.connector.create_request("placeOrderMultilegFIX", request)

    def placeOrderMultilegReplaceFIX(self, request):
        return self.connector.create_request("placeOrderMultilegReplaceFIX", request)

    def placeQuoteFIX(self, request):
        return self.connector.create_request("placeQuoteFIX", request)

    def placeMarketDataRequestFIX(self, request):
        return self.connector.create_request("placeMarketDataRequestFIX", request)

    def placeOrderMassCancelRequestFIX(self, request):
        return self.connector.create_request("placeOrderMassCancelRequestFIX", request)
