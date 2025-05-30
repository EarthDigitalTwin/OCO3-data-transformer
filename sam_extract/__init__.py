# Copyright 2025 California Institute of Technology (Caltech)
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

try:
    from importlib.metadata import version as _version
except ImportError:
    from importlib_metadata import version as _version

try:
    __version__ = _version('oco3_sam_zarr')
except Exception:
    __version__ = '999'

import logging
TRACE = 5


def log_trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)


def log_trace_to_root(message, *args, **kwargs):
    logging.log(TRACE, message, *args, **kwargs)


logging.addLevelName(TRACE, 'TRACE')
setattr(logging, 'TRACE', TRACE)
setattr(logging, 'trace', log_trace_to_root)
setattr(logging.getLoggerClass(), 'trace', log_trace)

GROUP_KEYS = ['/', '/Meteorology', '/Preprocessors', '/Retrieval', '/Sounding']
