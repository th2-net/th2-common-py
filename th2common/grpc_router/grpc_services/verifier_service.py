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

from th2 import verifier_pb2_grpc as import_stub


class VerifierService(object):

    def __init__(self, router):
        self.connector = router.get_connection(VerifierService, import_stub.VerifierStub)

    def createCheckpoint(self, request):
        return self.connector.create_request("createCheckpoint", request)

    def submitCheckRule(self, request):
        return self.connector.create_request("submitCheckRule", request)

    def submitCheckSequenceRule(self, request):
        return self.connector.create_request("submitCheckSequenceRule", request)
