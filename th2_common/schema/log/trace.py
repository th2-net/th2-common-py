#   Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
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
from typing import Any

_trace_installed = False


def install_trace_logger() -> None:
    global _trace_installed
    if _trace_installed:
        return

    TRACE = 5

    def trace(self: logging.Logger, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.isEnabledFor(TRACE):
            self._log(TRACE, msg, args, **kwargs)

    def log_to_root(msg: str, *args: Any, **kwargs: Any) -> None:
        logging.log(TRACE, msg, *args, **kwargs)

    logging.addLevelName(TRACE, 'TRACE')
    setattr(logging, 'TRACE', TRACE)
    setattr(logging.getLoggerClass(), 'trace', trace)
    setattr(logging, 'trace', log_to_root)

    _trace_installed = True
