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

from . import act_fix_pb2_grpc as importStub

class ActService(object):

    def __init__(self, router):
        self.connector = router.get_connection(ActService, importStub.ActStub)

    def placeOrderFIX(self, request):
        return self.connector.create_request("placeOrderFIX",request)

    def sendMessage(self, request):
        return self.connector.create_request("sendMessage",request)

    def placeQuoteRequestFIX(self, request):
        return self.connector.create_request("placeQuoteRequestFIX",request)

    def placeQuoteFIX(self, request):
        return self.connector.create_request("placeQuoteFIX",request)

    def placeOrderMassCancelRequestFIX(self, request):
        return self.connector.create_request("placeOrderMassCancelRequestFIX",request)

    def placeQuoteCancelFIX(self, request):
        return self.connector.create_request("placeQuoteCancelFIX",request)

    def placeQuoteResponseFIX(self, request):
        return self.connector.create_request("placeQuoteResponseFIX",request)
