#   Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
_trace_installed = False


def install_trace_logger():
    global _trace_installed
    if _trace_installed:
        return

    level = logging.TRACE = 5

    def log_logger(self, message, *args, **kwargs):
        if self.isEnabledFor(level):
            self._log(level, message, args, **kwargs)

    logging.getLoggerClass().trace = log_logger

    def log_root(msg, *args, **kwargs):
        logging.log(level, msg, *args, **kwargs)

    logging.addLevelName(level, "TRACE")
    logging.trace = log_root
    _trace_installed = True
