#   Copyright 2022-2022 Exactpro (Exactpro Systems Limited)

from typing import Any, Dict

from th2_common_utils import message_to_dict
from th2_grpc_common.common_pb2 import Direction, Message


class Th2MsgFieldExtraction:

    SESSION_ALIAS_KEY = 'session_alias'
    MESSAGE_TYPE_KEY = 'message_type'
    DIRECTION_KEY = 'direction'

    def get_fields(self, message: Message) -> Dict[str, Any]:
        message_fields = message_to_dict(message)
        message_metadata = message.metadata

        metadata_msg_fields = {
            self.SESSION_ALIAS_KEY: message_metadata.id.connection_id.session_alias,
            self.MESSAGE_TYPE_KEY: message_metadata.message_type,
            self.DIRECTION_KEY: Direction.Name(message_metadata.id.direction)
        }
        message_fields.update(metadata_msg_fields)

        return message_fields  # type: ignore
