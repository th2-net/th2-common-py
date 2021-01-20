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


from th2_common.schema.message.configuration.queue_configuration import QueueConfiguration


class MessageRouterConfiguration:
    def __init__(self, queues: dict) -> None:
        self.queues = {queue_alias: QueueConfiguration(**queues[queue_alias]) for queue_alias in queues.keys()}

    def get_queue_by_alias(self, queue_alias):
        return self.queues[queue_alias]

    def find_queues_by_attr(self, attrs) -> {str: QueueConfiguration}:
        result = dict()
        for queue_alias in self.queues.keys():
            ok = True
            for attr in attrs:
                if not self.queues[queue_alias].attributes.__contains__(attr):
                    ok = False
                    break
            if ok:
                result[queue_alias] = self.queues[queue_alias]
        return result
