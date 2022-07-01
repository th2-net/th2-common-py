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

from enum import Enum
from typing import Any, Dict, ItemsView


def object_to_dict(obj: Any) -> Dict[str, Any]:
    if isinstance(obj, Enum):
        return obj.value  # type: ignore
    elif hasattr(obj, '__dict__'):
        return {k: object_to_dict(v) for k, v in obj.__dict__.items()}
    elif isinstance(obj, dict):
        return {k: object_to_dict(v) for k, v in obj.items()}
    elif isinstance(obj, (list, ItemsView)):
        return list(map(object_to_dict, obj))  # type: ignore
    else:
        return obj  # type: ignore
