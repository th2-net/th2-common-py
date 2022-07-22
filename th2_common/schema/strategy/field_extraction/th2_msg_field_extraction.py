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

import logging
from typing import Any, Dict

from th2_common_utils import message_to_dict
from th2_grpc_common.common_pb2 import AnyMessage, Direction


logger = logging.getLogger(__name__)


class Th2MsgFieldExtraction:

    SESSION_ALIAS_KEY: str = 'session_alias'
    MESSAGE_TYPE_KEY: str = 'message_type'
    DIRECTION_KEY: str = 'direction'
    PROTOCOL_KEY: str = 'protocol'

    def get_fields(self, any_message: AnyMessage) -> Dict[str, Any]:
        if any_message.HasField('message'):
            return self._message_with_metadata_to_dict(any_message)

        elif any_message.HasField('raw_message'):
            return self._raw_message_metadata_to_dict(any_message)

        else:
            logger.warning(f'Cannot get fields from {type(any_message)} object')
            return {}

    def _message_with_metadata_to_dict(self, any_message: AnyMessage) -> Dict[str, Any]:
        message_dict = message_to_dict(any_message.message)

        return {**message_dict['fields'], **message_dict['metadata']}

    def _raw_message_metadata_to_dict(self, any_message: AnyMessage) -> Dict[str, Any]:
        raw_message_metadata = any_message.raw_message.metadata

        return {
            self.SESSION_ALIAS_KEY: raw_message_metadata.id.connection_id.session_alias,
            self.DIRECTION_KEY: Direction.Name(raw_message_metadata.id.direction),
            self.PROTOCOL_KEY: raw_message_metadata.protocol
        }
