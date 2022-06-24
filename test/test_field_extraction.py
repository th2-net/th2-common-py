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

from test.resources.field_extraction.messages import any_message, any_message_raw, message_dict, raw_message_dict

from th2_common.schema.strategy.field_extraction.th2_msg_field_extraction import Th2MsgFieldExtraction


def test_message_field_extraction() -> None:
    extract_strategy = Th2MsgFieldExtraction()
    assert extract_strategy.get_fields(any_message) == message_dict


def test_raw_message_field_extraction() -> None:
    extract_strategy = Th2MsgFieldExtraction()
    assert extract_strategy.get_fields(any_message_raw) == raw_message_dict
